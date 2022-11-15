use std::{env, sync::Arc};

use futures::executor::block_on;
use futures::stream::StreamExt;
use grpcio::{ChannelCredentials, EnvBuilder};

use google_cloud_rust_raw::bigtable::v2::{bigtable, data};
use protobuf::RepeatedField;

#[macro_use]
extern crate slog_scope;

mod btclient;
mod logging;

async fn async_main() {
    logging::init_logging(false);
    info!("starting");
    let table_name = env::var("DSN").unwrap_or_else(|_| {
        "projects/autopush-dev/instances/development-1/tables/autopush".to_owned()
    });
    let endpoint =
        env::var("ENDPOINT").unwrap_or_else(|_| "bigtable.googleapis.com:443".to_owned());

    debug!("Getting env vars...");
    let env = Arc::new(EnvBuilder::new().build());
    let creds = ChannelCredentials::google_default_credentials().unwrap();

    // TODO: throw these in a pool?
    let client = btclient::BigTableClient::new(env, creds, &endpoint, &table_name);
    // build a Request (we'll go with a regex one first.)
    let req = {
        // TODO: method
        let filter = {
            // build a value stripping filter:
            let mut strip_filter = data::RowFilter::default();
            strip_filter.set_strip_value_transformer(true);

            // filter only rows that are UAIDs.
            let mut regx_filter = data::RowFilter::default();
            regx_filter.set_row_key_regex_filter("^[^#]+".as_bytes().to_vec());
            regx_filter
            /*
            // Build a chain for these filters.
            let mut chain = data::RowFilter_Chain::default();
            let mut repeat_field = RepeatedField::default();
            repeat_field.push(strip_filter);
            repeat_field.push(regx_filter);
            chain.set_filters(repeat_field);

            // and store them into a single filter.
            let mut filter = data::RowFilter::default();
            filter.set_chain(chain);
            filter
            */
        };

        // Build the Request, setting the table and including the specified rows.
        let mut req = bigtable::ReadRowsRequest::default();
        req.set_table_name(table_name);
        req.set_filter(filter);
        // req.set_rows(row_set);
        req
    };

    // TODO: method
    let result = client.read_rows(req).await.unwrap();
    for key in result.keys() {
        println!("🚣🏻‍♂️    {:?} => {:?}", key, result.get(key).unwrap());
    }
    println!("");

    /*
    let uaids = {
        // get uaids:
        // TODO: stuff this into a class
        let mut stream = match client.client.read_rows(&req).map_err(|e| e.to_string()) {
            Ok(v) => v,
            Err(e) => {
                error!("{}", e);
                return;
            }
        };

        let mut uaids = Vec::new();

        while let (Some(rrow), s) = stream.into_future().await {
            stream = s;
            if let Ok(row) = rrow {
                print!("====");
                for chunk in row.get_chunks() {

                    // TODO: merge chunks into logical row.
                    // (See row_merger._RowMerger.process_chunks)
                    if let Ok(key) = std::str::from_utf8(&chunk.row_key) {
                        if !key.is_empty() {
                            uaids.push(key.to_owned());
                        }
                        // we get back a lot more random chunks. Need to figure out how to stich them together?
                        // TODO: each additional cell is included in it's own chunk?
                        // Yes, reading the chunks that form a row is a state machine.


                        dbg!(&chunk);
                    }
                }
                // rows.push(row.chunks.);
            }
        }
        uaids
    };

    dbg!(uaids);
    */
}

fn main() {
    block_on(async_main())
}
