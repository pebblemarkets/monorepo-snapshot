#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;

use pebble_fluid::wal::{WalCommand, WalOffset, WalSegmentReader};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliArgs {
    wal_dir: PathBuf,
    from_offset: WalOffset,
    verify_only: bool,
}

fn usage(binary: &str) -> String {
    format!(
        "Usage: {binary} <wal_dir> [--from-offset <u64>] [--verify-only]\n\
         Example: {binary} ./wal --from-offset 100"
    )
}

fn parse_args<I>(args: I) -> Result<CliArgs, String>
where
    I: IntoIterator<Item = String>,
{
    let mut iter = args.into_iter();
    let binary = iter.next().unwrap_or_else(|| "wal_inspector".to_string());
    let wal_dir = iter
        .next()
        .ok_or_else(|| usage(&binary))?
        .trim()
        .to_string();
    if wal_dir.is_empty() {
        return Err("wal_dir must not be empty".to_string());
    }

    let mut from_offset = WalOffset(0);
    let mut verify_only = false;

    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--verify-only" => verify_only = true,
            "--from-offset" => {
                let raw = iter
                    .next()
                    .ok_or_else(|| "--from-offset requires a value".to_string())?;
                let value = raw
                    .parse::<u64>()
                    .map_err(|e| format!("invalid --from-offset value: {e}"))?;
                from_offset = WalOffset(value);
            }
            _ => return Err(format!("unknown flag: {flag}\n{}", usage(&binary))),
        }
    }

    Ok(CliArgs {
        wal_dir: PathBuf::from(wal_dir),
        from_offset,
        verify_only,
    })
}

fn format_command(command: &WalCommand) -> String {
    match command {
        WalCommand::PlaceOrder(incoming) => format!(
            "PlaceOrder order_id={} account_id={} market_id={} side={:?} type={:?} qty={} nonce={}",
            incoming.order_id.0,
            incoming.account_id,
            incoming.market_id.0,
            incoming.side,
            incoming.order_type,
            incoming.quantity_minor,
            incoming.nonce.0,
        ),
        WalCommand::CancelOrder {
            order_id,
            market_id,
            account_id,
        } => format!(
            "CancelOrder order_id={} market_id={} account_id={}",
            order_id.0, market_id.0, account_id
        ),
    }
}

fn run(args: CliArgs) -> Result<(), String> {
    let mut reader = WalSegmentReader::open(&args.wal_dir, args.from_offset, true)
        .map_err(|e| format!("failed to open WAL reader: {e}"))?;

    let mut count = 0u64;
    loop {
        let batch = reader
            .read_batch(512)
            .map_err(|e| format!("failed reading WAL records: {e}"))?;
        if batch.is_empty() {
            break;
        }
        for record in batch {
            count = count
                .checked_add(1)
                .ok_or_else(|| "record count overflow".to_string())?;
            if !args.verify_only {
                println!(
                    "offset={} engine_version={} {}",
                    record.offset.0,
                    record.engine_version,
                    format_command(&record.command)
                );
            }
        }
    }

    if args.verify_only {
        println!(
            "WAL verification succeeded: {} record(s) from offset {}",
            count, args.from_offset.0
        );
    } else {
        println!("WAL dump completed: {} record(s)", count);
    }
    Ok(())
}

fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    if let Err(err) = run(args) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pebble_trading::{
        IncomingOrder, MarketId, OrderId, OrderNonce, OrderSide, OrderType, TimeInForce,
    };

    #[test]
    fn parse_args_parses_verify_and_offset() {
        let args = parse_args(vec![
            "wal_inspector".to_string(),
            "./wal".to_string(),
            "--from-offset".to_string(),
            "42".to_string(),
            "--verify-only".to_string(),
        ])
        .unwrap();
        assert_eq!(args.wal_dir, PathBuf::from("./wal"));
        assert_eq!(args.from_offset, WalOffset(42));
        assert!(args.verify_only);
    }

    #[test]
    fn parse_args_rejects_unknown_flag() {
        let err = parse_args(vec![
            "wal_inspector".to_string(),
            "./wal".to_string(),
            "--nope".to_string(),
        ])
        .unwrap_err();
        assert!(err.contains("unknown flag"));
    }

    #[test]
    fn format_command_place_and_cancel() {
        let incoming = IncomingOrder {
            order_id: OrderId("o1".to_string()),
            account_id: "alice".to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            nonce: OrderNonce(0),
            price_ticks: 10,
            quantity_minor: 1,
            submitted_at_micros: 1,
        };
        let place = format_command(&WalCommand::PlaceOrder(incoming));
        assert!(place.contains("PlaceOrder"));
        assert!(place.contains("order_id=o1"));

        let cancel = format_command(&WalCommand::CancelOrder {
            order_id: OrderId("o1".to_string()),
            market_id: MarketId("m1".to_string()),
            account_id: "alice".to_string(),
        });
        assert!(cancel.contains("CancelOrder"));
        assert!(cancel.contains("market_id=m1"));
    }
}
