use std::collections::HashMap;
use std::{env, sync::Arc, time::SystemTime};

use bigtable_client::{error::BigTableError, BigTableClient};
use futures::executor::block_on;
use grpcio::{ChannelCredentials, EnvBuilder};

use google_cloud_rust_raw::bigtable::v2::{bigtable, data};
use protobuf::RepeatedField;
use rand::{seq::SliceRandom, Rng};

use crate::bigtable_client::{cell::fill_cells, cell::Cell, row::Row, Qualifier};

#[macro_use]
extern crate slog_scope;

mod bigtable_client;
mod logging;

#[allow(dead_code)]
/// This is a demo showing how to construct a complex request.
async fn get_uaids(client: &BigTableClient) -> Result<Vec<String>, BigTableError> {
    // build a Request (we'll go with a regex one first.)
    let req = {
        // TODO: method
        let filter = {
            // if we only want key values, we don't realy care about the cells.
            // this will strip those values from the returned rows, making the
            // response a bit faster.
            let mut strip_filter = data::RowFilter::default();
            strip_filter.set_strip_value_transformer(true);

            /*
            // you can also specify a set, which limits fetched data to just
            // items between two keys.
            let mut range_set = data::RowRange::default();

            // Range keys are either open (meaning that they do not include
            // the provided key) or closed (meaning that they do include the
            // provided key). It would be most common to have an open start
            // key and a closed end key.
            range_set.set_start_key_open(start_key_bytes);
            range_set.set_end_key_closed(end_key_bytes);
            */

            // regex filters are pretty much what's on the tin. They use
            // regular expression syntax to search data. Ideally, these are
            // scoped to a limited set of keys (using the key range). We're doing
            // a semi expensive table scan here.
            let mut regex_filter = data::RowFilter::default();
            regex_filter.set_row_key_regex_filter("^[^#]+".as_bytes().to_vec());

            // Build a chain for these filters.
            // BigTable first gathers all the row data then applies the
            // specified filters. With a `RowFilter_Chain` the product of
            // one filter feeds into the next.
            // `RowFilter_Condition`

            let mut chain = data::RowFilter_Chain::default();
            let mut repeat_field = RepeatedField::default();
            repeat_field.push(strip_filter);
            repeat_field.push(regex_filter);
            chain.set_filters(repeat_field);

            // and store them into a single filter.
            let mut filter = data::RowFilter::default();
            filter.set_chain(chain);
            filter
        };

        // Build the Request, setting the table and including the specified rows.
        // in this case, we're going to get all the UAIDs (all keys that do not
        // include the `#` separator.
        let mut req = bigtable::ReadRowsRequest::default();
        req.set_table_name(client.table_name.clone());
        req.set_filter(filter);
        // req.set_rows(row_set);
        req
    };

    // Get the filtered data and return just the row_keys
    // yes, don't do this in production.
    Ok(client
        .clone()
        .read_rows(req)
        .await?
        .keys()
        .map(|v| v.to_owned())
        .collect::<Vec<String>>())
}

#[allow(dead_code)]
/// allow for either a env specified ID or randomly pick an existing one.
async fn target_uaid(client: &BigTableClient) -> Result<String, BigTableError> {
    match env::var("UAID") {
        Ok(v) => Ok(v),
        Err(_e) => {
            let uaid = get_uaids(client)
                .await?
                .choose(&mut rand::thread_rng())
                .map(|v| v.to_owned());
            Ok(uaid.unwrap_or_else(|| "".to_owned()))
        }
    }
}

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
    let client = bigtable_client::BigTableClient::new(env, creds, &endpoint, &table_name);
    // build a Request (we'll go with a regex one first.)

    // Randomly pick a UAID
    // let uaid = target_uaid(&client).await.unwrap();
    let uaid = uuid::Uuid::new_v4().as_simple().to_string();

    info!("⛏ Picked UAID {:?}", &uaid);
    // Add some data for the UAID:

    // Timestamps are a bit weird in BigTable.
    // the "timestamp" field is basically a numeric that's compared to the system clock
    // to determine what action to take. (e.g. if BigTable's garbage collection rules
    // for a given family say that this cell should live for 1ms, it's not uncommon to
    // have the timestamp be set into the future so that at timestamp+1ms, the record is
    // removed.)
    // In addition, when writing timestamp values, Bigtable seems to want timestamps
    // rounded to the second. (otherwise you get "Timestamp granularity mismatch" errors)
    let now = SystemTime::now();
    let u_now = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let mut cell_data: HashMap<Qualifier, Vec<u8>> = HashMap::new();

    // this value can have ms granularity, since we're storing it as pure data.
    cell_data.insert(
        "connected_at".into(),
        u_now.as_micros().to_be_bytes().to_vec(),
    );
    cell_data.insert(
        "node_id".into(),
        format!("https://some.node/r/{}", rand::random::<u64>()).into(),
    );

    // use the same family in this function as the row you're adding.
    let mut cells: HashMap<Qualifier, Vec<Cell>> = HashMap::new();
    // We only have one family here, so we just do this once.
    cells.insert(
        "default".into(),
        // note that this convenience function takes a second bound timestamp.
        fill_cells("default", SystemTime::now(), cell_data),
    );
    let row = Row {
        row_key: uaid.clone(),
        cells,
    };
    client.write_row(row).await.unwrap();
    info!("Wrote UAID connection.");

    // Let's make up some channel data to show how that works.
    let chid = uuid::Uuid::new_v4().as_simple().to_string();
    info!("Adding CHID: {}", &chid);

    let ttl = SystemTime::now() + time::Duration::seconds(rand::thread_rng().gen_range(60..10000));

    let data = "Amidst the mists and coldest frosts, I thrust my fists against the posts and still demand to see the ghosts".to_owned().into_bytes();
    let data_len = data.len();

    // And write the cells.
    let mut cells: HashMap<Qualifier, Vec<Cell>> = HashMap::new();
    let mut cell_data: HashMap<Qualifier, Vec<u8>> = HashMap::new();
    cell_data.insert("data".into(), data.clone());
    cell_data.insert(
        "sortkey_timestamp".into(),
        u_now.as_secs().to_be_bytes().to_vec(),
    );
    cell_data.insert("headers".into(), "header, header, header".to_owned().into());
    cells.insert("message".into(), fill_cells("message", ttl, cell_data));

    let chid_row_key = format!("{}#{}", &uaid, &chid);

    let row = Row {
        row_key: chid_row_key.clone(),
        cells,
    };
    info!(
        "📝 writing row {:?}, Data len: {}, timestamp: {:?}",
        &chid_row_key, data_len, ttl
    );
    client.write_row(row).await.unwrap();

    // std::thread::sleep(std::time::Duration::from_secs(5));
    info!("  Reading row {:?}", &chid_row_key);
    match client.read_row(&chid_row_key).await.unwrap() {
        Some(read_row) => {
            let row_data = read_row
                .get_cells("message", "data")
                .unwrap()
                .pop()
                .unwrap()
                .value;
            assert_eq!(row_data, data);
            print!(
                "\tRow: {}\n\tData: {:?}\n",
                &chid_row_key,
                String::from_utf8(row_data).unwrap()
            )
        }
        None => {
            error!("🚨Could not find row!");
        }
    };

    // deleting the data (an ack)
    info!(" Acking data");
    client
        .delete_cells(
            &chid_row_key,
            "message",
            &["data".to_owned().as_bytes().to_vec()].to_vec(),
            None,
        )
        .await
        .unwrap();

    match client.read_row(&chid_row_key).await.unwrap() {
        Some(read_row) => {
            if read_row.get_cells("message", "data").is_some() {
                error!("🚨 Data not deleted")
            }
        }
        None => {
            info!("Row not found!");
        }
    };

    info!(" Unregistering");
    client.delete_row(&chid_row_key).await.unwrap();

    info!("🛑 Done");

    // TODO: Get the newly created data by polling the UAID.
}

fn main() {
    block_on(async_main())
}
