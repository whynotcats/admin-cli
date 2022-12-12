use chrono::NaiveDate;
use csv;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, error::Error};

//  code, name, name ascii, geonameid
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Admin1Data {
    pub code: String, // <CountryCode>.<Admin1Code>
    pub name: String,
    pub ascii_name: String,
    pub geonameid: i64,
}

// concatenated codes <tab>name <tab> asciiname <tab> geonameId
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Admin2Data {
    pub code: String, // <CountryCode>.<Admin1Code>.<Admin2Code>
    pub name: String,
    pub ascii_name: String,
    pub geonameid: i64,
}

// geonameid         : integer id of record in geonames database
// name              : name of geographical point (utf8) varchar(200)
// asciiname         : name of geographical point in plain ascii characters, varchar(200)
// alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
// latitude          : latitude in decimal degrees (wgs84)
// longitude         : longitude in decimal degrees (wgs84)
// feature class     : see http://www.geonames.org/export/codes.html, char(1)
// feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
// country code      : ISO-3166 2-letter country code, 2 characters
// cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
// admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
// admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
// admin3 code       : code for third level administrative division, varchar(20)
// admin4 code       : code for fourth level administrative division, varchar(20)
// population        : bigint (8 byte int)
// elevation         : in meters, integer
// dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
// timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
// modification date : date of last modification in yyyy-MM-dd format
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Location {
    pub id: i64,
    pub name: String,
    pub ascii_name: String,
    pub alternate_names: String,
    pub latitude: f64,
    pub longitude: f64,
    pub feature_class: Option<char>,
    pub feature_code: String,
    pub country_code: String,
    pub cc2: String,
    pub admin1_code: String,
    pub admin2_code: String,
    pub admin3_code: String,
    pub admin4_code: Option<String>,
    pub population: Option<i64>,
    pub elevation: Option<i64>,
    pub dem: Option<i64>,
    pub timezone: String,
    pub modification_date: NaiveDate,
}

impl Location {
    pub fn key(self: &Location) -> String {
        format!("{}, {}", self.name, self.country_code)
    }

    pub fn value(self: &Location) -> String {
        format!("{},{}", self.latitude, self.longitude)
    }

    pub fn generate_elasticsearch_document(
        self: &Location,
        admin1: &HashMap<String, String>,
        admin2: &HashMap<String, String>,
    ) -> Value {
        let pop = if let Some(population) = self.population {
            if population < 0 {
                None
            } else {
                Some(population)
            }
        } else {
            None
        };

        let admin_1_key = format!("{}.{}", self.country_code.to_uppercase(), self.admin1_code);
        let admin_2_key = format!(
            "{}.{}.{}",
            self.country_code.to_uppercase(),
            self.admin1_code,
            self.admin2_code
        );

        json!({
            "name": self.name,
            "ascii_name": self.ascii_name,
            "location": [self.longitude, self.latitude],
            "elevation": self.elevation,
            "country_code": self.country_code,
            "feature_code": self.feature_code,
            "feature_class": self.feature_class,
            "admin1": admin1.get(&admin_1_key),
            "admin2": admin2.get(&admin_2_key),
            "population": pop,
            "timezone": self.timezone,
            "modification_date": self.modification_date
        })
    }

    pub fn generate_mapping() -> Value {
        json!({"properties": {
            "name": {"type": "text"},
            "ascii_name": {"type": "text"},
            "alternate_names": {"type": "text"},
            "location": {"type": "geo_point"},
            "country_code": {"type": "keyword"},
            "feature_code": {"type": "keyword"},
            "admin1": {"type": "text"},
            "admin2": {"type": "text"},
            "feature_class": {"type": "keyword"},
            "population": {"type": "unsigned_long"},
            "elevation": {"type": "integer"},
            "timezone": {"type": "keyword"},
            "modification_date": {"type": "date"},
        }})
    }
}

pub fn read_file(file_name: &str) -> Result<Vec<Location>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(file_name)?;
    let mut locations = Vec::new();

    for result in rdr.deserialize() {
        let record: Location = result?;
        locations.push(record);
    }

    Ok(locations)
}

pub fn load_admin_files(
    admin_1_file: &str,
    admin_2_file: &str,
) -> Result<(HashMap<String, String>, HashMap<String, String>), Box<dyn Error>> {
    let mut admin_1_data: HashMap<String, String> = HashMap::new();
    let mut admin_2_data: HashMap<String, String> = HashMap::new();

    // TODO: make this parallizable
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_path(admin_1_file)?;

    for result in rdr.deserialize() {
        let record: Admin1Data = result?;
        admin_1_data.insert(record.code, record.name);
    }

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_path(admin_2_file)?;

    for result in rdr.deserialize() {
        let record: Admin2Data = result?;
        admin_2_data.insert(record.code, record.name);
    }

    Ok((admin_1_data, admin_2_data))
}
