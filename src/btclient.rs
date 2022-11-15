use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::{hash::Hash, sync::Arc};

use futures::StreamExt;
use google_cloud_rust_raw::bigtable::v2::{
    bigtable::ReadRowsRequest,
    bigtable::{ReadRowsResponse, ReadRowsResponse_CellChunk},
    bigtable_grpc::BigtableClient,
};
use grpcio::{ChannelBuilder, ChannelCredentials, ClientSStreamReceiver, Environment};
use protobuf::well_known_types::StringValue;
use thiserror::Error;

// these are normally Vec<u8>
type RowKey = String;
type Qualifier = String;

#[derive(Debug, Error)]
pub enum BigTableError {
    #[error("Column family {0} is not among the cells stored in this row")]
    MissingColumnFamily(String),

    #[error("Column {0} is not among the cells stored in this row in the column family {1}")]
    MissingColumn(String, String),

    #[error("Index {0} is not valid for the cells stored in this column {1} in the column family {2}. There are {3} such cells.")]
    MissingIndex(String, String, String, u64),

    #[error("Invalid Row Response: {0}")]
    InvalidRowResponse(String),

    #[error("Invalid Chunk: {0}")]
    InvalidChunk(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ReadState {
    RowStart,
    CellStart,
    CellInProgress,
    CellComplete,
    RowComplete,
}

impl Default for ReadState {
    fn default() -> Self {
        ReadState::RowStart
    }
}

pub struct Chunk {
    family: Option<String>,
    value: Vec<u8>,
    timestamp_ms: Option<u64>,
    qualifier: Option<String>,
    state: ReadState,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PartialCell {
    family: String,
    qualifier: Qualifier,
    timestamp_ms: i64,
    labels: Vec<String>,
    value: Vec<u8>,
    value_index: usize,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Cell {
    family: String,
    qualifier: Qualifier,
    value: Vec<u8>,
    value_len: usize,
    value_index: usize,
    timestamp_ms: i64,
    labels: Vec<String>,
}

impl From<PartialCell> for Cell {
    fn from(partial: PartialCell) -> Cell {
        Self {
            family: partial.family,
            qualifier: partial.qualifier,
            value_len: partial.value.len(),
            value: partial.value,
            value_index: partial.value_index,
            timestamp_ms: partial.timestamp_ms,
            labels: partial.labels,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Row {
    row_key: RowKey,
    cells: HashMap<String, Vec<Cell>>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PartialRow {
    row_key: RowKey,
    cells: HashMap<String, Vec<PartialCell>>,
    last_family: String,
    last_family_cells: HashMap<Qualifier, Vec<Cell>>,
    last_qualifier: Qualifier,
    cell_in_progress: RefCell<PartialCell>,
}

#[derive(Debug, Default)]
struct RowMerger {
    state: ReadState,
    last_seen_row_key: Option<RowKey>,
    last_seen_cell_family: Option<String>,
    row_in_progress: RefCell<PartialRow>, // TODO: Make Option?
}

//
impl RowMerger {
    async fn reset_row(
        &mut self,
        chunk: ReadRowsResponse_CellChunk,
    ) -> Result<&mut Self, BigTableError> {
        if self.state == ReadState::RowStart {
            return Err(BigTableError::InvalidChunk("Bare Reset".to_owned()));
        };
        if !chunk.row_key.is_empty() {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has a row key".to_owned(),
            ));
        };
        if chunk.has_family_name() {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has a family_name".to_owned(),
            ));
        }
        if chunk.has_qualifier() {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has a qualifier".to_owned(),
            ));
        }
        if chunk.timestamp_micros > 0 {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has a timestamp".to_owned(),
            ));
        }
        if !chunk.get_labels().is_empty() {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has a labels".to_owned(),
            ));
        }
        if !chunk.value.is_empty() {
            return Err(BigTableError::InvalidChunk(
                "Reset chunk has value".to_owned(),
            ));
        }

        self.state = ReadState::RowStart;
        self.row_in_progress = RefCell::new(PartialRow::default());
        Ok(self)
    }

    /// The initial row contains the first cell data. There may be additional data that we
    /// have to use later, so capture that as well.
    async fn row_start(
        &mut self,
        chunk: &mut ReadRowsResponse_CellChunk,
    ) -> Result<&Self, BigTableError> {
        if chunk.row_key.is_empty() {
            return Err(BigTableError::InvalidChunk(
                "New row is missing a row key".to_owned(),
            ));
        }
        if chunk.has_family_name() {
            info!("👪Family name: {:?}", &chunk.get_family_name());
            self.last_seen_cell_family = Some(chunk.get_family_name().get_value().to_owned());
        }
        if let Some(last_key) = self.last_seen_row_key.clone() {
            if last_key.as_bytes().to_vec() >= chunk.row_key {
                return Err(BigTableError::InvalidChunk(
                    "Out of order row keys".to_owned(),
                ));
            }
        }

        let mut row = self.row_in_progress.borrow_mut();

        row.row_key = String::from_utf8(chunk.row_key.clone()).unwrap_or_default();
        row.cell_in_progress = RefCell::new(PartialCell::default());

        self.state = ReadState::CellStart;
        Ok(self)
    }

    /// cell_start seems to be the main worker. It starts a new cell value (rows contain cells, which
    /// can have multiple versions).
    async fn cell_start(
        &mut self,
        chunk: &mut ReadRowsResponse_CellChunk,
    ) -> Result<&Self, BigTableError> {
        // cells must have qualifiers.
        if !chunk.has_qualifier() {
            self.state = ReadState::CellComplete;
            return Ok(self);
            // return Err(BigTableError::InvalidChunk("Cell missing qualifier for new cell".to_owned()))
        }
        let qualifier = chunk.get_qualifier().clone().take_value();
        // dbg!(chunk.has_qualifier(), String::from_utf8(qualifier.clone()));
        let row = self.row_in_progress.borrow_mut();

        if !row.cells.is_empty()
            && !chunk.row_key.is_empty()
            && chunk.row_key != row.row_key.as_bytes()
        {
            return Err(BigTableError::InvalidChunk(
                "Row key changed mid row".to_owned(),
            ));
        }

        let mut cell = row.cell_in_progress.borrow_mut();
        if chunk.has_family_name() {
            cell.family = chunk.get_family_name().get_value().to_owned();
        } else {
            if self.last_seen_cell_family == None {
                return Err(BigTableError::InvalidChunk(
                    "Cell missing family for new cell".to_owned(),
                ));
            }
            cell.family = self.last_seen_cell_family.clone().unwrap();
        }

        // A qualifier is the name of the cell. (I don't know why it's called that either.)
        cell.qualifier = String::from_utf8(qualifier).unwrap_or_default();

        // record the timestamp for this cell. (Note: this is not the clock time that it was
        // created, but the timestamp that was used for it's creation. It is used by the
        // garbage collector.)
        cell.timestamp_ms = chunk.timestamp_micros;

        // If there are additional labels for this cell, record them.
        // can't call map, so do this the semi-hard way
        let labels = chunk.mut_labels();
        while let Some(label) = labels.pop() {
            cell.labels.push(label)
        }

        // Pre-allocate space for this cell version data. The data will be delivered in
        // multiple chunks. (Not strictly neccessary, but can save us some future allocs)
        if chunk.value_size > 0 {
            cell.value = Vec::with_capacity(chunk.value_size as usize);
            self.state = ReadState::CellInProgress;
        } else {
            // Add the data to what we've got.
            cell.value.append(&mut chunk.value.clone());
            self.state = ReadState::CellComplete;
        }

        Ok(self)
    }

    /// Continue adding data to the cell version. Cell data may exceed a chunk's max size,
    /// so we contine feeding data into it.
    async fn cell_in_progress(
        &mut self,
        chunk: &mut ReadRowsResponse_CellChunk,
    ) -> Result<&Self, BigTableError> {
        let row = self.row_in_progress.borrow();
        let mut cell = row.cell_in_progress.borrow_mut();

        // Quick gauntlet to ensure that we have a cell continuation.
        if cell.value_index > 0 {
            if !chunk.row_key.is_empty() {
                return Err(BigTableError::InvalidChunk(
                    "Found row key mid cell".to_owned(),
                ));
            }
            if chunk.has_family_name() {
                return Err(BigTableError::InvalidChunk(
                    "Found family name mid cell".to_owned(),
                ));
            }
            if chunk.has_qualifier() {
                return Err(BigTableError::InvalidChunk(
                    "Found qualifier mid cell".to_owned(),
                ));
            }
            if chunk.get_timestamp_micros() > 0 {
                return Err(BigTableError::InvalidChunk(
                    "Found timestamp mid cell".to_owned(),
                ));
            }
            if chunk.get_labels().len() > 0 {
                return Err(BigTableError::InvalidChunk(
                    "Found labels mid cell".to_owned(),
                ));
            }
        }

        let mut val = chunk.take_value();
        cell.value_index += val.len();
        cell.value.append(&mut val);

        self.state = if chunk.value_size > 0 {
            ReadState::CellInProgress
        } else {
            ReadState::CellComplete
        };

        Ok(self)
    }

    /// Wrap up a cell that's been in progress.
    async fn cell_complete(
        &mut self,
        chunk: &mut ReadRowsResponse_CellChunk,
    ) -> Result<&Self, BigTableError> {
        let mut row_in_progress = self.row_in_progress.borrow_mut();
        // Read Only version of the row in progress.
        let ro_row_in_progress = row_in_progress.clone();
        // the currently completed cell in progress.
        let mut cell_in_progress = ro_row_in_progress.cell_in_progress.borrow_mut();

        let cell_family = cell_in_progress.family.clone();

        let mut family_changed = false;
        if ro_row_in_progress.last_family != cell_in_progress.family {
            family_changed = true;
            let cip_family = cell_in_progress.family.clone();
            row_in_progress.last_family = cip_family.clone();

            // append the completed cell to the row's cell list.
            let cells = match ro_row_in_progress.cells.get(&cip_family) {
                Some(cells) => {
                    let mut cells = cells.clone();
                    cells.push(cell_in_progress.clone());
                    cells
                }
                None => {
                    let mut cells = Vec::<PartialCell>::new();
                    cells.push(cell_in_progress.clone());
                    cells
                }
            };
            row_in_progress.cells.insert(cip_family, cells);
        }

        // If the family changed, or the cell name changed
        if family_changed || ro_row_in_progress.last_qualifier != cell_in_progress.qualifier {
            let qualifier = cell_in_progress.qualifier.clone();
            row_in_progress.last_qualifier = qualifier.clone();
            let mut qualifier_cells = Vec::new();
            qualifier_cells.push(Cell {
                family: cell_family.clone(),
                timestamp_ms: cell_in_progress.timestamp_ms,
                labels: cell_in_progress.labels.clone(),
                ..Default::default()
            });
            row_in_progress
                .last_family_cells
                .insert(qualifier.clone(), qualifier_cells);
            row_in_progress.last_qualifier = qualifier;
        }

        // reset the cell in progress
        cell_in_progress.timestamp_ms = 0;
        cell_in_progress.value = Vec::new();
        cell_in_progress.value_index = 0;

        // If this isn't the last item in the row, keep going.
        self.state = if !chunk.has_commit_row() {
            ReadState::CellStart
        } else {
            ReadState::RowComplete
        };

        Ok(self)
    }

    /// wrap up a row, reinitialize our state to read the next row.
    async fn row_complete(
        &mut self,
        _chunk: &mut ReadRowsResponse_CellChunk,
    ) -> Result<Row, BigTableError> {
        let mut new_row = Row::default();

        let row = self.row_in_progress.take();
        self.last_seen_row_key = Some(row.row_key.clone());
        new_row.row_key = row.row_key;
        for (key, partial_cells) in row.cells {
            let mut cells: Vec<Cell> = Vec::new();
            for partial_cell in partial_cells {
                cells.push(partial_cell.into())
            }
            new_row.cells.insert(key, cells);
        }

        // now that we're done, write a clean version.
        self.row_in_progress = RefCell::new(PartialRow::default());
        self.state = ReadState::RowStart;
        Ok(new_row)
    }

    /// wrap up anything, we're done reading data.
    async fn finalize(&mut self) -> Result<&Self, BigTableError> {
        if self.state != ReadState::RowStart {
            return Err(BigTableError::InvalidChunk(
                "The row remains partial / is not committed.".to_owned(),
            ));
        }
        Ok(self)
    }

    /// Iterate through all the returned chunks and compile them into finished cells.
    pub async fn process_chunks(
        mut stream: ClientSStreamReceiver<ReadRowsResponse>,
    ) -> Result<HashMap<RowKey, Row>, BigTableError> {
        // Work object
        let mut merger = Self::default();

        // finished collection
        let mut rows = HashMap::<RowKey, Row>::new();

        while let (Some(row_resp_res), s) = stream.into_future().await {
            stream = s;
            let row = match row_resp_res {
                Ok(v) => v,
                Err(e) => return Err(BigTableError::InvalidRowResponse(e.to_string())),
            };
            /*
            ReadRowsResponse:
                pub chunks: ::protobuf::RepeatedField<ReadRowsResponse_CellChunk>,
                pub last_scanned_row_key: ::std::vec::Vec<u8>,
                // special fields
                pub unknown_fields: ::protobuf::UnknownFields,
                pub cached_size: ::protobuf::CachedSize,
            */
            if !row.last_scanned_row_key.is_empty() {
                let row_key = String::from_utf8(row.last_scanned_row_key).unwrap_or_default();
                if merger.last_seen_row_key.clone().unwrap_or_default() >= row_key {
                    return Err(BigTableError::InvalidChunk(
                        "Last scanned row key is out of order".to_owned(),
                    ));
                }
                merger.last_seen_row_key = Some(row_key);
            }

            for mut chunk in row.chunks {
                debug!("🧩 Chunk >> {:?}", &chunk);
                if chunk.get_reset_row() {
                    debug!("‼ resetting row");
                    merger.reset_row(chunk).await?;
                    continue;
                }
                // each of these states feed into the next states.
                if merger.state == ReadState::RowStart {
                    debug!("🟧 new row");
                    merger.row_start(&mut chunk).await?;
                }

                if merger.state == ReadState::CellStart {
                    debug!("🟡   cell start {:?}", chunk.get_qualifier());
                    merger.cell_start(&mut chunk).await?;
                }
                if merger.state == ReadState::CellInProgress {
                    debug!("🟡   cell in progress");
                    merger.cell_in_progress(&mut chunk).await?;
                }
                if merger.state == ReadState::CellComplete {
                    debug!("🟨   cell complete");
                    merger.cell_complete(&mut chunk).await?;
                }
                if merger.state == ReadState::RowComplete {
                    debug! {"🟧 row complete"};
                    let finished_row = merger.row_complete(&mut chunk).await?;
                    rows.insert(finished_row.row_key.clone(), finished_row);
                } else {
                    if chunk.has_commit_row() {
                        return Err(BigTableError::InvalidChunk(format!(
                            "Chunk tried to commit in row in wrong state {:?}",
                            merger.state
                        )));
                    }
                }
                debug!("🧩 Chunk end {:?}", merger.state);
            }
        }
        Ok(rows)
    }
}

pub struct BigTableClient {
    table_name: String,
    pub(crate) client: BigtableClient,
}

impl BigTableClient {
    pub fn new(
        env: Arc<Environment>,
        creds: ChannelCredentials,
        endpoint: &str,
        table_name: &str,
    ) -> Self {
        let chan = ChannelBuilder::new(env)
            .max_send_message_len(1 << 28)
            .max_receive_message_len(1 << 28)
            .set_credentials(creds)
            .connect(endpoint);

        Self {
            table_name: table_name.to_owned(),
            client: BigtableClient::new(chan),
        }
    }

    pub async fn read_rows(self, req: ReadRowsRequest) -> Result<HashMap<RowKey, Row>, String> {
        /*
        let mut rep_field = RepeatedField::default();
        rep_field.push(key.as_bytes().to_vec());
        let mut row_set = RowSet::default();
        row_set.set_row_keys(rep_field);

        // Build the Request, setting the table and including the specified rows.
        let mut req = ReadRowsRequest::default();
        req.set_table_name(self.table_name);
        req.set_rows(row_set);
        */
        // gather ye results.
        // let mut rows = Vec::new();

        let stream = self.client.read_rows(&req).map_err(|e| e.to_string())?;
        /*
        while let (Some(rrow), s) = stream.into_future().await {
            stream = s;
            if let Ok(row) = rrow {
                rows.push(row);
            }
        }
        */

        Ok(RowMerger::process_chunks(stream)
            .await
            .map_err(|e| e.to_string())?)
    }
}
