use std::{fs, path::Path};

fn main() {
    let migrations_dir = Path::new("../../infra/sql/migrations");
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    let Ok(entries) = fs::read_dir(migrations_dir) else {
        return;
    };

    let mut migration_files = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    migration_files.sort();

    for migration_file in migration_files {
        println!("cargo:rerun-if-changed={}", migration_file.display());
    }
}
