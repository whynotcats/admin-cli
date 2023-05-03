use serde::Deserialize;
use std::error::Error;
use std::fs::{self, canonicalize, read_dir};
use std::io;
use std::path::Path;
use std::{fs::ReadDir, path::PathBuf};
// use toml;

#[derive(Deserialize)]
pub enum SiteType {
    #[serde(alias = "static", alias = "STATIC")]
    Static,
    #[serde(alias = "api", alias = "API")]
    Api,
}

#[derive(Deserialize)]
pub struct Metadata {
    pub source_dir: Option<PathBuf>,
    pub sites: Vec<ProjectSite>,
}

#[derive(Deserialize)]
pub struct ProjectSite {
    pub name: String,
    pub source: PathBuf,
    pub site_type: SiteType,
}

pub fn load_metadata(root: &Path) -> Result<Metadata, Box<dyn Error>> {
    let root = discover_single(root)?;
    let file = fs::read_to_string(&root)?;

    let parsed_toml = toml::from_str::<Metadata>(&file)?;

    Ok(parsed_toml)
}

// Find and load .cat.toml project metadata
pub fn discover_single(path: &Path) -> Result<PathBuf, io::Error> {
    let mut candidates = discover_project_toml(path)?;
    let res = match candidates.pop() {
        None => panic!("No project toml found"),
        Some(it) => it,
    };

    if !candidates.is_empty() {
        panic!("more than one project found");
    }

    Ok(res)
}

fn discover_project_toml(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    return find_project_toml(path)?
        .into_iter()
        .map(|path| path.canonicalize())
        .collect();
}

fn find_project_toml(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    match find_in_parent_dirs(path, ".cat.toml") {
        Some(it) => Ok(vec![it]),
        None => Ok(find_toml_in_child_dir(read_dir(path)?)),
    }
}

fn find_in_parent_dirs(path: &Path, file_name: &str) -> Option<PathBuf> {
    if path.file_name().unwrap_or_default() == file_name && path.is_file() {
        return Some(path.to_path_buf());
    }

    let mut curr = Some(path);

    while let Some(path) = curr {
        let candidate = path.join(file_name);
        if fs::metadata(&candidate).is_ok() {
            return Some(candidate);
        }

        curr = path.parent();
    }

    None
}

fn find_toml_in_child_dir(entities: ReadDir) -> Vec<PathBuf> {
    entities
        .filter_map(Result::ok)
        .map(|it| it.path().join(".cat.toml"))
        .filter(|it| it.exists())
        .map(canonicalize)
        .filter_map(|it| it.ok())
        .collect()
}
