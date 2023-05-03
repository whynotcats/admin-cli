use serde::Deserialize;
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use toml;

#[derive(Deserialize)]
struct TrunkToml {
    build: BuildToml,
}

#[derive(Deserialize)]
struct BuildToml {
    #[allow(dead_code)]
    target: Option<String>,
    dist: Option<String>,
}

// Build the trunk app
pub fn run_trunk(app_dir: &PathBuf) -> Result<(), Box<dyn Error>> {
    println!("Building trunk app: {}", app_dir.display());
    let mut cmd = Command::new("trunk");

    // Move into the project directory
    cmd.current_dir(app_dir);

    // Build the site
    let result = cmd
        .arg("build")
        .arg("--release")
        .arg("--public-url")
        .arg("/assets/")
        .status()
        .expect("Failed to build trunk app");

    if !result.success() {
        return Err("Failed to build trunk app".into());
    }

    Ok(())
}

// Move the generated output files into the correct directories for deployment
pub fn move_files(project_dir: &PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    // Get the output of the build
    let dist_dir = project_dir
        .join(
            toml::from_str::<TrunkToml>(&std::fs::read_to_string(project_dir.join("Trunk.toml"))?)?
                .build
                .dist
                .unwrap_or_else(|| "dist".into()),
        )
        .canonicalize()?;

    let assets_dir = dist_dir.clone().join("assets");

    // Arrange files in the correct directories
    // index.html
    // -> assets/
    //   *.wasm
    //   *.js
    //   *.css
    Command::new("nu")
        .arg("-c")
        .arg(format!(
            "mkdir {}",
            &assets_dir.to_str().expect("Failed to create assets path")
        ))
        .status()
        .expect("Failed to create assets directory");

    println!("Created assets directory: {}", &assets_dir.display());

    let moveable_file_types: Vec<&OsStr> = vec!["wasm", "js", "css"]
        .into_iter()
        .map(OsStr::new)
        .collect();

    // Move all the css, wasm, and js files into the created assets directory
    for entry in fs::read_dir(&dist_dir)?
        .into_iter()
        .filter(|f| f.is_ok())
        .map(|f| f.unwrap().path())
        .filter(|f| moveable_file_types.contains(&f.extension().unwrap_or_default()))
        .collect::<Vec<PathBuf>>()
    {
        fs::rename(&entry, assets_dir.join(&entry.file_name().unwrap()))?;
    }

    println!(
        "Moved js, css, and wasm addets to {}",
        &assets_dir.display()
    );

    Ok(dist_dir.clone())
}

pub fn scp_files(
    dist_dir: &PathBuf,
    server: &str,
    static_site_name: &str,
) -> Result<(), Box<dyn Error>> {
    let static_site_dir = PathBuf::from(format!("/var/www/{}", static_site_name));
    let output_dir = match dist_dir.to_str().unwrap().starts_with("\\\\") {
        true => dist_dir
            .to_str()
            .expect("dist_dir to be a str")
            .get(
                (dist_dir
                    .to_str()
                    .unwrap()
                    .find(':')
                    .expect("path should have a : character in it")
                    - 1)..,
            )
            .expect("cannot strip \\\\?\\ prefix from dist_dir"),
        false => dist_dir.to_str().expect("dist_dir to be a str"),
    };

    dbg!(output_dir);

    Command::new("scp")
        .arg("-r")
        .arg(output_dir)
        .arg(format!("{}:{}", &server, &static_site_dir.display()))
        .status()?;

    Ok(())
}
