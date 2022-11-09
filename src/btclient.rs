use std::sync::Arc;

use futures::StreamExt;
use google_cloud_rust_raw::bigtable::v2::{
    bigtable::ReadRowsRequest, bigtable::ReadRowsResponse, bigtable_grpc::BigtableClient,
    data::RowSet,
};
use grpcio::{ChannelBuilder, ChannelCredentials, Environment};
use protobuf::RepeatedField;

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

    pub async fn read_rows(self, key: &str) -> Result<Vec<ReadRowsResponse>, String> {
        // set the list of keys to search. This can be a range, or a vector of keys.
        let mut rep_field = RepeatedField::default();
        rep_field.push(key.as_bytes().to_vec());
        let mut row_set = RowSet::default();
        row_set.set_row_keys(rep_field);

        // Build the Request, setting the table and including the specified rows.
        let mut req = ReadRowsRequest::default();
        req.set_table_name(self.table_name);
        req.set_rows(row_set);

        // gather ye results.
        let mut rows = Vec::new();

        let mut stream = self.client.read_rows(&req).map_err(|e| e.to_string())?;
        while let (Some(rrow), s) = stream.into_future().await {
            stream = s;
            if let Ok(row) = rrow {
                rows.push(row);
            }
        }
        Ok(rows)
    }
}
