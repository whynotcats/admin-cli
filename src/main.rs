use std::{
    error::Error,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

use clap::{Parser, Subcommand};
use elasticsearch::{
    http::{transport::Transport, StatusCode},
    indices::{IndicesCreateParts, IndicesExistsParts, IndicesPutMappingParts},
    BulkOperation, BulkParts, Elasticsearch,
};
use image::GenericImageView;
use image::{imageops::FilterType::Lanczos3, io::Reader as ImageReader};
use serde_json::{self, Value};

pub mod geonames;
pub use geonames::{load_admin_files, Location};

#[derive(Parser)]
#[command(author= "Why Not Cats", version, about = "Administrative Utlity for Why Not Cats projects", long_about = None)]
struct Opt {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Seed {
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
    },
    Images {
        path: String,

        #[clap(short, long)]
        output: Option<PathBuf>,
    },
}

struct Size {
    width: u32,
    height: Option<u32>,
    suffix: String,
}

async fn run() -> Result<(), Box<dyn Error>> {
    let opt = Opt::parse();

    match &opt.command {
        Commands::Seed {
            path,
            admin1,
            admin2,
            elasticsearch,
            index,
            buffer,
        } => {
            println!("Loading admin files");
            let (admin1, admin2) = load_admin_files(admin1, admin2)?;

            println!("Creating connection to {}", elasticsearch);
            let client = Elasticsearch::new(Transport::single_node(elasticsearch)?);

            println!("Checking to see if index {} exists", index);
            let exists_response = client
                .indices()
                .exists(IndicesExistsParts::Index(&[index]))
                .send()
                .await?;

            if exists_response.status_code() == StatusCode::NOT_FOUND {
                println!("Creating index with mapping");
                let create_index_response = client
                    .indices()
                    .create(IndicesCreateParts::Index(index))
                    .send()
                    .await?;

                if StatusCode::is_success(&create_index_response.status_code()) {
                    println!("Applying Mapping");
                    let apply_mapping_response = client
                        .indices()
                        .put_mapping(IndicesPutMappingParts::Index(&[index]))
                        .body(Location::generate_mapping())
                        .send()
                        .await?;

                    if apply_mapping_response.status_code() == StatusCode::OK {
                        println!("Created mapping for index {}", index);
                    } else {
                        panic!("Could not update mapping for index {}", index);
                    }
                } else {
                    panic!("Could not create index {}", index);
                }
            } else {
                println!("Index {} exists", index);
            }

            println!("Opening file {}", path);
            let f = std::fs::File::open(path)?;
            let mut file = zip::read::ZipArchive::new(f)?;
            let zf = file.by_index(0)?;

            println!("Building file reader");
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .has_headers(false)
                .from_reader(Box::new(zf));

            let mut records = 0;
            let mut commands: Vec<BulkOperation<_>> = Vec::with_capacity(*buffer);

            for result in rdr.deserialize() {
                let record: Location = result?;

                commands.push(
                    BulkOperation::index(record.generate_elasticsearch_document(&admin1, &admin2))
                        .id(record.id.to_string())
                        .into(),
                );
                records += 1;

                if records % buffer == 0 {
                    println!("Loaded {} commands", records);

                    let response = client
                        .bulk(BulkParts::Index(index))
                        .body(commands)
                        .send()
                        .await?;

                    let response_body = response.json::<Value>().await?;
                    let success = !response_body["errors"].as_bool().unwrap();
                    if success {
                        commands = Vec::with_capacity(*buffer);
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
                    .bulk(BulkParts::Index(index))
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
        Commands::Images { path, output } => {
            println!("Opening image at {}", path);
            let sizes = [
                Size {
                    width: 1200,
                    height: None,
                    suffix: "1200px".to_string(),
                },
                Size {
                    width: 600,
                    height: None,
                    suffix: "600px".to_string(),
                },
                Size {
                    width: 2400,
                    height: None,
                    suffix: "2400px".to_string(),
                },
            ];

            let p = Path::new(path);
            let file_name = p.file_stem().unwrap();
            for size in sizes {
                let output_path = if output.is_none() {
                    p.with_file_name(format!(
                        "{}-{}",
                        file_name
                            .to_str()
                            .expect("Could not get file_name of image"),
                        size.suffix
                    ))
                    .with_extension("jpg")
                } else {
                    output.as_deref().unwrap().to_path_buf()
                };

                let now = Instant::now();
                let img = ImageReader::open(path)
                    .expect("Could not open path to image")
                    .decode()
                    .expect("Could not decode image");

                let (_x, y) = img.dimensions();
                let new_img = img.resize(size.width, size.height.unwrap_or(y), Lanczos3);

                match new_img.save_with_format(&output_path, image::ImageFormat::Jpeg) {
                    Ok(_) => {
                        println!("Done processing image in {}ms", now.elapsed().as_millis());
                    }
                    Err(err) => {
                        println!("Error saving image to {}: {}", &output_path.display(), err);
                    }
                }
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run().await?;
    Ok(())
}
