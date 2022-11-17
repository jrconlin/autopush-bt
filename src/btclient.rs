use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use google_cloud_rust_raw::bigtable::v2::bigtable;
use google_cloud_rust_raw::bigtable::v2::data;
use google_cloud_rust_raw::bigtable::v2::{
    bigtable::ReadRowsRequest,
    bigtable::{ReadRowsResponse, ReadRowsResponse_CellChunk},
    bigtable_grpc::BigtableClient,
};
use grpcio::{ChannelBuilder, ChannelCredentials, ClientSStreamReceiver, Environment};
use protobuf::RepeatedField;
use thiserror::Error;

// these are normally Vec<u8>
pub type RowKey = String;
pub type Qualifier = String;
// This must be a String.
pub type FamilyId = String;

#[derive(Debug, Error)]
pub enum BigTableError {
    #[error("Invalid Row Response: {0}")]
    InvalidRowResponse(String),

    #[error("Invalid Chunk: {0}")]
    InvalidChunk(String),

    #[error("BigTable read error {0}")]
    BigTableRead(String),

    #[error("BigTable write error {0}")]
    BigTableWrite(String),
}

/// List of the potential states when we are reading each value from the
/// returned stream and composing a "row"
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

/// An in-progress Cell data struct.
#[derive(Debug, Clone)]
pub(crate) struct PartialCell {
    family: FamilyId,
    /// A "qualifier" is a column name
    qualifier: Qualifier,
    /// Timestamps are returned as microseconds, but need to be
    /// specified as milliseconds (even though the function asks
    /// for microseconds, you * 1000 the mls).
    timestamp: SystemTime,
    /// Not sure if or how these are used
    labels: Vec<String>,
    /// The data buffer.
    value: Vec<u8>,
    /// the returned sort order for the cell.
    value_index: usize,
}

impl Default for PartialCell {
    fn default() -> Self {
        Self {
            family: String::default(),
            qualifier: String::default(),
            timestamp: SystemTime::now(),
            labels: Vec::new(),
            value: Vec::new(),
            value_index: 0,
        }
    }
}

/// A finished Cell. An individual Cell contains the
/// data. There can be multiple cells for a given
/// rowkey::qualifier.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Cell {
    /// The family identifier string.
    pub family: FamilyId,
    /// Column name
    pub qualifier: Qualifier,
    /// Column data
    pub value: Vec<u8>,
    /// the cell's index if returned in a group or array.
    value_index: usize,
    /// "Timestamp" in milliseconds. This value is used by the family
    /// garbage collection rules and may not reflect reality.
    pub timestamp: SystemTime,
    labels: Vec<String>, // not sure if these are used?
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            family: String::default(),
            qualifier: String::default(),
            timestamp: SystemTime::now(),
            labels: Vec::new(),
            value: Vec::new(),
            value_index: 0,
        }
    }
}

impl From<PartialCell> for Cell {
    fn from(partial: PartialCell) -> Cell {
        Self {
            family: partial.family,
            qualifier: partial.qualifier,
            value: partial.value,
            value_index: partial.value_index,
            timestamp: partial.timestamp,
            labels: partial.labels,
        }
    }
}

/// Returns a list of filled cells for the given family.
/// NOTE: Timestamp, here, means whatever the family GC rules dictate.
pub fn fill_cells(
    family: &str,
    timestamp: SystemTime,
    cell_data: HashMap<Qualifier, Vec<u8>>,
) -> Vec<Cell> {
    let mut cells: Vec<Cell> = Vec::new();
    for (value_index, (qualifier, value)) in cell_data.into_iter().enumerate() {
        cells.push(Cell {
            family: family.to_owned(),
            qualifier,
            timestamp,
            value,
            value_index,
            ..Default::default()
        });
    }
    cells
}

/// A finished row. A row consists of a hash of one or more cells per
/// qualifer (cell name).
#[derive(Debug, Default, Clone)]
pub struct Row {
    /// The row's key.
    // This may be any ByteArray value.
    pub row_key: RowKey,
    /// The row's collection of cells, indexed by the family ID.
    pub cells: HashMap<Qualifier, Vec<Cell>>,
}

impl Row {
    pub fn get_cells(&self, family: &str, column: &str) -> Option<Vec<Cell>> {
        let mut result = Vec::<Cell>::new();
        if let Some(family_group) = self.cells.get(family) {
            for cell in family_group {
                if cell.qualifier == column {
                    result.push(cell.clone())
                }
            }
        }
        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    }
}

/// An in-progress Row structure
#[derive(Debug, Default, Clone)]
pub(crate) struct PartialRow {
    /// table uniquie key
    row_key: RowKey,
    /// map of cells per family identifier.
    cells: HashMap<FamilyId, Vec<PartialCell>>,
    /// the last family id string we encountered
    last_family: FamilyId,
    /// the working set of family and cells
    /// we've encountered so far
    last_family_cells: HashMap<FamilyId, Vec<Cell>>,
    /// the last column name we've encountered
    last_qualifier: Qualifier,
    /// Any cell that may be in progress (chunked
    /// across multiple portions)
    cell_in_progress: RefCell<PartialCell>,
}

/// workhorse struct, this is used to gather item data from the stream and build rows.
#[derive(Debug, Default)]
struct RowMerger {
    /// The row's current state. State progresses while processing a single chunk.
    state: ReadState,
    /// The last row key we've encountered. This should be consistent across the chunks.
    last_seen_row_key: Option<RowKey>,
    /// The last cell family. This may change, indicating a new cell group.
    last_seen_cell_family: Option<FamilyId>,
    /// The row that is currently being compiled.
    row_in_progress: RefCell<PartialRow>,
}

impl RowMerger {
    /// discard data so far and return to a neutral state.
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
            info!(
                "👪Family name: {}: {:?}",
                String::from_utf8(chunk.row_key.clone()).unwrap_or_default(),
                &chunk.get_family_name()
            );
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
            if self.last_seen_cell_family.is_none() {
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
        cell.timestamp =
            SystemTime::UNIX_EPOCH + Duration::from_micros(chunk.timestamp_micros as u64);

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
            cell.value.append(&mut chunk.value.clone());
            // Add the data to what we've got.
            /*
            if qualifier != "data".to_owned().as_bytes() {
                cell.value.append(&mut chunk.value.clone());
            } else {
                let mut converted =base64::decode_config(chunk.value.clone(), base64::URL_SAFE_NO_PAD).map_err(|e| BigTableError::BigTableRead(e.to_string()))?;
                cell.value.append(&mut converted);
            }
            */
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
            if chunk.get_labels().is_empty() {
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
                    vec![cell_in_progress.clone()]
                }
            };
            row_in_progress.cells.insert(cip_family, cells);
        }

        // If the family changed, or the cell name changed
        if family_changed || ro_row_in_progress.last_qualifier != cell_in_progress.qualifier {
            let qualifier = cell_in_progress.qualifier.clone();
            row_in_progress.last_qualifier = qualifier.clone();
            let qualifier_cells = vec![Cell {
                family: cell_family,
                timestamp: cell_in_progress.timestamp,
                labels: cell_in_progress.labels.clone(),
                ..Default::default()
            }];
            row_in_progress
                .last_family_cells
                .insert(qualifier.clone(), qualifier_cells);
            row_in_progress.last_qualifier = qualifier;
        }

        // reset the cell in progress
        cell_in_progress.timestamp = SystemTime::now();
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
                } else if chunk.has_commit_row() {
                    return Err(BigTableError::InvalidChunk(format!(
                        "Chunk tried to commit in row in wrong state {:?}",
                        merger.state
                    )));
                }

                debug!("🧩 Chunk end {:?}", merger.state);
            }
        }
        merger.finalize().await?;
        debug!("🚣🏻‍♂️ Rows: {}", &rows.len());
        Ok(rows)
    }
}

#[derive(Clone)]
/// Wrapper for the BigTable connection
pub struct BigTableClient {
    /// The name of the table. This is used when generating a request.
    pub(crate) table_name: String,
    /// The client connection to BigTable.
    client: BigtableClient,
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

    /// Read a given row from the row key.
    pub async fn read_row(&self, row_key: &str) -> Result<Option<Row>, BigTableError> {
        debug!("Row key: {}", row_key);

        let mut row_keys = RepeatedField::default();
        row_keys.push(row_key.to_owned().as_bytes().to_vec());

        let mut row_set = data::RowSet::default();
        row_set.set_row_keys(row_keys);

        let mut req = bigtable::ReadRowsRequest::default();
        req.set_table_name(self.table_name.clone());
        req.set_rows(row_set);

        let rows = self.read_rows(req).await?;
        Ok(rows.get(row_key).cloned())
    }

    /// Take a big table ReadRowsRequest (containing the keys and filters) and return a set of row data.
    ///
    ///
    pub async fn read_rows(
        &self,
        req: ReadRowsRequest,
    ) -> Result<HashMap<RowKey, Row>, BigTableError> {
        let resp = self
            .client
            .clone()
            .read_rows(&req)
            .map_err(|e| BigTableError::BigTableRead(e.to_string()))?;
        RowMerger::process_chunks(resp).await
    }

    /// write a given row.
    ///
    /// there's also `.mutate_rows` which I presume allows multiple.
    pub async fn write_row(&self, row: Row) -> Result<(), BigTableError> {
        let mut req = bigtable::MutateRowRequest::default();

        // compile the mutations.
        // It's possible to do a lot here, including altering in process
        // mutations, clearing them, etc. It's all up for grabs until we commit
        // below. For now, let's just presume a write and be done.
        let mut mutations = protobuf::RepeatedField::default();
        req.set_table_name(self.table_name.clone());
        req.set_row_key(row.row_key.into_bytes());
        for (_family, cells) in row.cells {
            for cell in cells {
                let mut mutation = data::Mutation::default();
                let mut set_cell = data::Mutation_SetCell::default();
                let timestamp = cell
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?;
                set_cell.family_name = cell.family;
                set_cell.set_column_qualifier(cell.qualifier.into_bytes());
                set_cell.set_value(cell.value);
                // Yes, this is passing milli bounded time as a micro. Otherwise I get
                // a `Timestamp granularity mismatch` error
                set_cell.set_timestamp_micros((timestamp.as_millis() * 1000) as i64);
                mutation.set_set_cell(set_cell);
                mutations.push(mutation);
            }
        }
        req.set_mutations(mutations);

        // Do the actual commit.
        // fails with `cannot execute `LocalPool` executor from within another executor: EnterError`
        let _resp = self
            .client
            .mutate_row_async(&req)
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?
            .await
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?;
        Ok(())
    }

    /// Delete all cell data from the specified columns with the optional time range.
    pub async fn delete_cells(
        &self,
        row_key: &str,
        family: &str,
        column_names: &Vec<Vec<u8>>,
        time_range: Option<&data::TimestampRange>,
    ) -> Result<(), BigTableError> {
        let mut req = bigtable::MutateRowRequest::default();
        req.set_table_name(self.table_name.clone());
        let mut mutations = protobuf::RepeatedField::default();
        req.set_row_key(row_key.to_owned().into_bytes());
        for column in column_names {
            let mut mutation = data::Mutation::default();
            // Mutation_DeleteFromRow -- Delete all cells for a given row.
            // Mutation_DeleteFromFamily -- Delete all cells from a family for a given row.
            // Mutation_DeleteFromColumn -- Delete all cells from a column name for a given row, restricted by timestamp range.
            let mut del_cell = data::Mutation_DeleteFromColumn::default();
            del_cell.set_family_name(family.to_owned());
            del_cell.set_column_qualifier(column.to_owned());
            if let Some(range) = time_range {
                del_cell.set_time_range(range.clone());
            }
            mutation.set_delete_from_column(del_cell);
            mutations.push(mutation);
        }

        req.set_mutations(mutations);

        let _resp = self
            .client
            .mutate_row_async(&req)
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?
            .await
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?;
        Ok(())
    }

    /// Delete all the cells for the given row. NOTE: This will drop the row.
    pub async fn delete_row(&self, row_key: &str) -> Result<(), BigTableError> {
        let mut req = bigtable::MutateRowRequest::default();
        req.set_table_name(self.table_name.clone());
        let mut mutations = protobuf::RepeatedField::default();
        req.set_row_key(row_key.to_owned().into_bytes());
        let mut mutation = data::Mutation::default();
        mutation.set_delete_from_row(data::Mutation_DeleteFromRow::default());
        mutations.push(mutation);
        req.set_mutations(mutations);

        let _resp = self
            .client
            .mutate_row_async(&req)
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?
            .await
            .map_err(|e| BigTableError::BigTableWrite(e.to_string()))?;
        Ok(())
    }
}
