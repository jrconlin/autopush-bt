use std::collections::HashMap;
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use btclient::{BigTableClient, BigTableError};
use futures::executor::block_on;
use grpcio::{ChannelCredentials, EnvBuilder};

use google_cloud_rust_raw::bigtable::v2::{bigtable, data};
use protobuf::RepeatedField;
use rand::{seq::SliceRandom, Rng};

use crate::btclient::{fill_cells, Cell, Qualifier, Row};

#[macro_use]
extern crate slog_scope;

mod btclient;
mod logging;

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

async fn target_uaid(client: &BigTableClient) -> Result<String, BigTableError> {
    match env::var("UAID") {
        Ok(v) => Ok(v),
        Err(e) => {
            let uaid = get_uaids(&client)
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
    let client = btclient::BigTableClient::new(env, creds, &endpoint, &table_name);
    // build a Request (we'll go with a regex one first.)

    // Randomly pick a UAID
    let uaid = target_uaid(&client).await.unwrap();
    info!("⛏ Picked UAID {:?}", &uaid);

    // Add some data:
    // for the UAID:
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now_ms = now.as_micros();
    let mut cell_data: HashMap<Qualifier, Vec<u8>> = HashMap::new();
    cell_data.insert("connected_at".into(), now_ms.to_be_bytes().to_vec());
    cell_data.insert(
        "node_id".into(),
        format!("https://some.node/r/{}", rand::random::<u64>()).into(),
    );

    // use the same family in this function as the row you're adding.
    let mut cells: HashMap<Qualifier, Vec<Cell>> = HashMap::new();
    // We only have one family here, so we just do this once.
    cells.insert("default".into(), fill_cells("default", now_ms, cell_data));
    let row = Row {
        row_key: uaid.clone().into(),
        cells,
    };
    client.write_row(row).await.unwrap();
    info!("Wrote UAID connection.");

    // Let's make up some channel data to show how that works.
    let chid = uuid::Uuid::new_v4().as_simple().to_string();
    info!("Adding CHID: {}", &chid);

    let ttl = (SystemTime::now()
        + time::Duration::seconds(rand::thread_rng().gen_range(60..10000)))
    .duration_since(UNIX_EPOCH)
    .unwrap();

    // Written data is automagically converted to base64 when stored.
    // You will need to decode it on the way out.
    /*
    let data_len = rand::thread_rng().gen_range(2048..4096);
    let data =
        (0..data_len)
            .map(|_| rand::random::<u8>())
            .collect::<Vec<u8>>();
    */

    let data = "Amidst the mists and coldest frosts, I thrust my fists against the posts and still demand to see the ghosts".to_owned().into_bytes();
    let data_len = data.len();
    // And write the cells.
    let mut cells: HashMap<Qualifier, Vec<Cell>> = HashMap::new();
    let mut cell_data: HashMap<Qualifier, Vec<u8>> = HashMap::new();
    cell_data.insert("data".into(), data);
    cell_data.insert(
        "sortkey_timestamp".into(),
        now.as_secs().to_be_bytes().to_vec(),
    );
    cell_data.insert("headers".into(), "header, header, header".to_owned().into());
    cells.insert(
        "message".into(),
        fill_cells("message", ttl.as_secs() as u128, cell_data),
    );

    let chid_row = format!("{}#{}", &uaid, &chid);

    let row = Row {
        row_key: chid_row.clone().into(),
        cells,
    };
    info!(
        "📝 writing row {:?}, Data len: {}, timestamp: {}",
        &chid_row,
        data_len,
        ttl.as_secs()
    );
    client.write_row(row).await.unwrap();

    // std::thread::sleep(std::time::Duration::from_secs(5));
    info!("  Reading row {:?}", &chid_row);
    if let Some(row) = client.read_row(&chid_row).await.unwrap() {
        println!(
            "
Row: {}
Data: {:?}
",
            row.row_key,
            String::from_utf8(row.get("message", "data").unwrap().pop().unwrap().value).unwrap()
        )
    };

    info!("🛑 Done");

    // TODO: Get the newly created data by polling the UAID.
}

fn main() {
    block_on(async_main())
}
