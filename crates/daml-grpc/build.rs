use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let vendored_include = protoc_bin_vendored::include_path()?;
    let proto_root = PathBuf::from("../../proto");

    let protos = [
        proto_root.join("com/daml/ledger/api/v2/update_service.proto"),
        proto_root.join("com/daml/ledger/api/v2/state_service.proto"),
        proto_root.join("com/daml/ledger/api/v2/event_query_service.proto"),
        proto_root.join("com/daml/ledger/api/v2/command_service.proto"),
        proto_root.join("google/rpc/status.proto"),
    ];

    let proto_root_str = proto_root.to_string_lossy().to_string();
    let vendored_include_str = vendored_include.to_string_lossy().to_string();

    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &protos
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            &[proto_root_str, vendored_include_str],
        )?;

    Ok(())
}
