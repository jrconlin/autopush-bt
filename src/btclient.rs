use std::{sync::Arc, hash::Hash};
use std::cell::RefCell;
use std::collections::HashMap;

use futures::StreamExt;
use google_cloud_rust_raw::bigtable::v2::{
    bigtable::ReadRowsRequest, bigtable::{ReadRowsResponse, ReadRowsResponse_CellChunk}, bigtable_grpc::BigtableClient,
};
use grpcio::{ChannelBuilder, ChannelCredentials, Environment, ClientSStreamReceiver};
use thiserror::Error;

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

pub struct Chunk{
    family: Option<String>,
    value: Vec<u8>,
    timestamp_ms: Option<u64>,
    qualifier: Option<String>,
    state: ReadState
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PartialCell {
    family: String,
    qualifier: Option<Vec<u8>>,
    timestamp_ms: i64,
    labels: Vec<String>,
    value: Vec<u8>,
    value_index: usize,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Cell {
    family: String,
    qualifier: Option<Vec<u8>>,
    value: Vec<u8>,
    value_index: usize,
    timestamp_ms: i64,
    labels: Vec<String>,
}

impl From<PartialCell> for Cell {
    fn from(partial: PartialCell) -> Cell {
        Self {
            family: partial.family,
            qualifier: partial.qualifier,
            value: partial.value,
            value_index: partial.value_index,
            timestamp_ms: partial.timestamp_ms,
            labels: partial.labels
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Row {
    row_key: Vec<u8>,
    cells: HashMap<String, Vec<Cell>>,
    cell_in_progress: RefCell<Cell>
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PartialRow {
    row_key: Vec<u8>,
    cells: HashMap<String, Vec<PartialCell>>,
    last_family: String,
    last_family_cells: HashMap<Vec<u8>, Vec<Cell>>,
    last_qualifier: Option<Vec<u8>>,
    cell_in_progress: RefCell<PartialCell>,
}

#[derive(Debug, Default)]
struct RowMerger{
    state: ReadState,
    last_seen_row_key: Option<Vec<u8>>,
    row_in_progress: RefCell<PartialRow>, // TODO: Make Option?
}

impl RowMerger {
    async fn reset_row(&mut self, chunk:ReadRowsResponse_CellChunk) -> Result<&mut Self, BigTableError> {
        if self.state == ReadState::RowStart {
            return Err(BigTableError::InvalidChunk("Bare Reset".to_owned()))
        };
        if !chunk.row_key.is_empty() {
            return Err(BigTableError::InvalidChunk("Reset chunk has a row key".to_owned()))
        };
        if chunk.has_family_name() {
            return Err(BigTableError::InvalidChunk("Reset chunk has a family_name".to_owned()))
        }
        if chunk.has_qualifier() {
            return Err(BigTableError::InvalidChunk("Reset chunk has a qualifier".to_owned()))
        }
        if chunk.timestamp_micros > 0 {
            return Err(BigTableError::InvalidChunk("Reset chunk has a timestamp".to_owned()))
        }
        if !chunk.get_labels().is_empty() {
            return Err(BigTableError::InvalidChunk("Reset chunk has a labels".to_owned()))
        }
        if !chunk.value.is_empty() {
            return Err(BigTableError::InvalidChunk("Reset chunk has value".to_owned()))
        }

        self.state = ReadState::RowStart;
        self.row_in_progress = RefCell::new(PartialRow::default());
        Ok(self)

    }

    async fn row_start(&mut self, chunk:ReadRowsResponse_CellChunk) -> Result<&Self, BigTableError> {
        if chunk.row_key.is_empty() {
            return Err(BigTableError::InvalidChunk("New row is missing a row key".to_owned()))
        }
        if let Some(last_key) = self.last_seen_row_key.clone() {
            if last_key >= chunk.row_key {
                return Err(BigTableError::InvalidChunk("Out of order row keys".to_owned()))
            }
        }

        let mut row = self.row_in_progress.borrow_mut();

        row.row_key = chunk.row_key;
        row.cell_in_progress = RefCell::new(PartialCell::default());

        self.state = ReadState::CellStart;
        Ok(self)
    }

    async fn cell_start(&mut self, mut chunk:ReadRowsResponse_CellChunk) -> Result<&Self, BigTableError> {
        let row = self.row_in_progress.borrow_mut();

        if !row.cells.is_empty() && !chunk.row_key.is_empty() && chunk.row_key != row.row_key {
            return Err(BigTableError::InvalidChunk("Row key changed mid row".to_owned()))
        }

        let mut cell = row.cell_in_progress.borrow_mut();
        if chunk.has_family_name() {
            cell.family = chunk.get_family_name().get_value().to_owned();
            cell.qualifier = None
        } else {
            return Err(BigTableError::InvalidChunk("Cell missing family for new cell".to_owned()))
        }

        if chunk.has_qualifier() {
            cell.qualifier = Some(chunk.get_qualifier().clone().take_value())
        } else {
            return Err(BigTableError::InvalidChunk("Cell missing qualifier for new cell".to_owned()))
        }
        cell.timestamp_ms = chunk.timestamp_micros;
        // can't call map, so do this the semi-hard way
        let labels = chunk.mut_labels();
        while let Some(label) = labels.pop() {
            cell.labels.push(label)
        }
        if chunk.value_size > 0 {
            cell.value = Vec::with_capacity(chunk.value_size as usize);
            self.state = ReadState::CellInProgress;
        } else {
            cell.value.append(&mut chunk.value);
        }

        Ok(self)
    }

    async fn cell_in_progress(&mut self, mut chunk:ReadRowsResponse_CellChunk) -> Result<&Self, BigTableError> {
        let row = self.row_in_progress.borrow();
        let mut cell = row.cell_in_progress.borrow_mut();

        if cell.value_index > 0{
            if !chunk.row_key.is_empty() {
                return Err(BigTableError::InvalidChunk("Found row key mid cell".to_owned()))
            }
            if chunk.has_family_name() {
                return Err(BigTableError::InvalidChunk("Found family name mid cell".to_owned()))
            }
            if chunk.has_qualifier() {
                return Err(BigTableError::InvalidChunk("Found qualifier mid cell".to_owned()))
            }
            if chunk.get_timestamp_micros() > 0 {
                return Err(BigTableError::InvalidChunk("Found timestamp mid cell".to_owned()))
            }
            if chunk.get_labels().len() > 0 {
                return Err(BigTableError::InvalidChunk("Found labels mid cell".to_owned()))
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

    async fn cell_complete(&mut self, chunk:ReadRowsResponse_CellChunk) -> Result<&Self, BigTableError> {
        let row = self.row_in_progress.borrow();
        let mut cell = row.cell_in_progress.borrow_mut();
        let cell_family = cell.family.clone();

        let mut family_changed = false;
        if row.last_family != cell.family{
            family_changed = true;
            let family = cell.family.clone();
            let cell_family = self.row_in_progress.borrow().cell_in_progress.borrow().family.clone();
            self.row_in_progress.borrow_mut().last_family = cell_family.clone();

            let cells =
                match row.cells.get(&family) {
                    Some(cells) => {
                        let mut cells = cells.clone();
                        cells.push(cell.clone());
                        cells.clone()
                    },
                    None => {
                        let mut cells = Vec::<PartialCell>::new();
                        cells.push(cell.clone());
                        cells
                    }
                };
            self.row_in_progress.borrow_mut().cells.insert(family.clone(), cells);
        }
        if family_changed || row.last_qualifier != cell.qualifier {
            let qualifier = self.row_in_progress.borrow().cell_in_progress.borrow().qualifier.clone();
            let mut row =self.row_in_progress.borrow_mut();
            row.last_qualifier = qualifier.clone();
            let mut qualifier_cells = Vec::new();
            qualifier_cells.push(Cell {
                family: cell_family.clone(),
                timestamp_ms: cell.timestamp_ms,
                labels: cell.labels.clone(),
                ..Default::default()
            });
            if let Some(qualifier) = qualifier {
                row.last_family_cells.insert(qualifier.clone(), qualifier_cells);
                row.last_qualifier = Some(qualifier);
            }
        }

        cell.timestamp_ms = 0;
        cell.value = Vec::new();
        cell.value_index = 0;

        self.state = if ! chunk.has_commit_row() {
            ReadState::CellStart
        } else {
            ReadState::RowComplete
        };

        Ok(self)
    }

    async fn row_complete(&mut self, _chunk:ReadRowsResponse_CellChunk) -> Result<Row, BigTableError> {
        let mut new_row = Row::default();

        {
            let row = self.row_in_progress.borrow();
            self.last_seen_row_key = Some(row.row_key.clone());
            new_row.row_key = row.row_key.clone();
            for (key, partial_cells) in row.cells.clone() {
                let mut cells:Vec<Cell> = Vec::new();
                for partial_cell in partial_cells {
                    cells.push(partial_cell.into())
                }
                new_row.cells.insert(key, cells);
            };
        }

        self.row_in_progress = RefCell::new(PartialRow::default());
        self.state = ReadState::RowStart;
        Ok(new_row)
    }

    async fn finalize(&mut self) -> Result<&Self, BigTableError>{
        if self.state != ReadState::RowStart {
            return Err(BigTableError::InvalidChunk("The row remains partial / is not committed.".to_owned()))
        }
        Ok(self)
    }


    pub async fn process_chunks(mut stream: ClientSStreamReceiver<ReadRowsResponse>) -> Result<HashMap::<Vec<u8>, Row>, BigTableError>{

        let mut merger = Self::default();
        let mut rows = HashMap::<Vec<u8>, Row>::new();

        while let (Some(row_resp_res), s) = stream.into_future().await {
            stream = s;
            let row = match row_resp_res {
                Ok(v) => v,
                Err(e) => return Err(BigTableError::InvalidRowResponse(e.to_string()))
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
                if merger.last_seen_row_key.clone().unwrap_or_default() >= row.last_scanned_row_key {
                    return Err(BigTableError::InvalidChunk("Last scanned row key is out of order".to_owned()))
                }
                merger.last_seen_row_key = Some(row.last_scanned_row_key)
            }

            for chunk in row.chunks {
                if chunk.get_reset_row(){
                    merger.reset_row(chunk).await?;
                    continue;
                }

                if chunk.has_commit_row() && merger.state != ReadState::RowComplete {
                    return Err(BigTableError::InvalidChunk("Chunk tried to commit row in wrong state".to_owned()));
                }
                match merger.state {
                    ReadState::RowStart => merger.row_start(chunk).await?,
                    ReadState::CellStart => merger.cell_start(chunk).await?,
                    ReadState::CellInProgress => merger.cell_in_progress(chunk).await?,
                    ReadState::CellComplete => merger.cell_complete(chunk).await?,
                    ReadState::RowComplete => {
                        let finished_row = merger.row_complete(chunk).await?;
                        rows.insert(finished_row.row_key.clone(), finished_row);
                        &merger
                    },
                };
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

    pub async fn read_rows(self, req: ReadRowsRequest) -> Result<HashMap<Vec<u8>, Row>, String> {
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

        let result = RowMerger::process_chunks(stream).await.map_err(|e| e.to_string())?;
        dbg!(&result);

        Ok(result)
    }
}
