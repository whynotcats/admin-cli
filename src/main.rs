use std::{error::Error, fs::File, io::Write};

use clap::Parser;
use elasticsearch::{
    http::{transport::Transport, StatusCode},
    indices::{IndicesCreateParts, IndicesExistsParts, IndicesPutMappingParts},
    BulkOperation, BulkParts, Elasticsearch,
};
use serde_json::{self, Value};

pub mod geonames;
pub use geonames::{load_admin_files, Location};

#[derive(Parser, Debug)]
#[clap(name = "server", about = "A server for our wasm project!")]
struct Opt {
    #[clap(short, long)]
    path: String,

    #[clap(short = '1', long)]
    admin1: String,

    #[clap(short = '2', long)]
    admin2: String,

    #[clap(short, long, default_value = "http://localhost:9200")]
    elasticsearch: String,

    #[clap(short, long, default_value = "geolocations")]
    index: String,

    #[clap(short, long, default_value_t = 100000)]
    buffer: usize,
}

async fn run_elasticsearch() -> Result<(), Box<dyn Error>> {
    let opt = Opt::parse();
    println!("Loading admin files");
    let (admin1, admin2) = load_admin_files(&opt.admin1, &opt.admin2)?;

    println!("Creating connection to {}", opt.elasticsearch);
    let client = Elasticsearch::new(Transport::single_node(&opt.elasticsearch)?);

    println!("Checking to see if index {} exists", &opt.index);
    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&[&opt.index]))
        .send()
        .await?;

    if exists_response.status_code() == StatusCode::NOT_FOUND {
        println!("Creating index with mapping");
        let create_index_response = client
            .indices()
            .create(IndicesCreateParts::Index(&opt.index))
            .send()
            .await?;

        if StatusCode::is_success(&create_index_response.status_code()) {
            println!("Applying Mapping");
            let apply_mapping_response = client
                .indices()
                .put_mapping(IndicesPutMappingParts::Index(&[&opt.index]))
                .body(Location::generate_mapping())
                .send()
                .await?;

            if apply_mapping_response.status_code() == StatusCode::OK {
                println!("Created mapping for index {}", &opt.index);
            } else {
                panic!("Could not update mapping for index {}", &opt.index);
            }
        } else {
            panic!("Could not create index {}", &opt.index);
        }
    } else {
        println!("Index {} exists", opt.index);
    }

    println!("Opening file {}", opt.path);
    let f = std::fs::File::open(opt.path)?;
    let mut file = zip::read::ZipArchive::new(f)?;
    let zf = file.by_index(0)?;

    println!("Building file reader");
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_reader(Box::new(zf));

    let mut records = 0;
    let mut commands: Vec<BulkOperation<_>> = Vec::with_capacity(opt.buffer);

    for result in rdr.deserialize() {
        let record: Location = result?;

        commands.push(
            BulkOperation::index(record.generate_elasticsearch_document(&admin1, &admin2))
                .id(record.id.to_string())
                .into(),
        );
        records += 1;

        if records % opt.buffer == 0 {
            println!("Loaded {} commands", records);

            let response = client
                .bulk(BulkParts::Index(&opt.index))
                .body(commands)
                .send()
                .await?;

            let response_body = response.json::<Value>().await?;
            let success = !response_body["errors"].as_bool().unwrap();
            if success {
                commands = Vec::with_capacity(opt.buffer);
                println!("Inserted {} records", records);
            } else {
                let mut file = File::create("error.log")?;
                file.write_all(response_body.to_string().as_bytes())?;

                panic!("Error inserting records into elaticsearch");
            }
        }
    }

    if !commands.is_empty() {
        let response = client
            .bulk(BulkParts::Index(&opt.index))
            .body(commands)
            .send()
            .await?;

        let success = !response.json::<Value>().await?["errors"].as_bool().unwrap();
        if success {
            println!("Inserted {} records", records);
        } else {
            panic!("Error inserting records into elaticsearch")
        }
    }

    println!("Done sending to elasticsearch");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run_elasticsearch().await?;
    Ok(())
}
