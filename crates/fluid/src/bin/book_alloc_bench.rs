#![allow(clippy::print_stdout)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use pebble_fluid::book::{MarketBook, RestingEntry};
use pebble_fluid::ids::StringInterner;
use pebble_trading::{
    IncomingOrder, InternalAccountId, InternalOrderId, MarketId, OrderId, OrderNonce, OrderSide,
    OrderType, TimeInForce,
};

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

// SAFETY: We forward directly to the system allocator and only update atomic counters.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: Delegating allocation to the system allocator with the same `layout`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: Delegating deallocation to the system allocator with the original pointer/layout.
        unsafe { System.dealloc(ptr, layout) };
    }
}

fn parse_env_usize(name: &str, default: usize) -> Result<usize, io::Error> {
    match env::var(name) {
        Ok(raw) => raw.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse {name}={raw:?} as usize: {error}"),
            )
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is not valid UTF-8"),
        )),
    }
}

fn reset_counters() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    DEALLOC_CALLS.store(0, Ordering::Relaxed);
    DEALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn usize_to_i64(value: usize, context: &str) -> Result<i64, io::Error> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to convert {context}={value} to i64"),
        )
    })
}

fn build_resting_book(
    resting_orders: usize,
) -> Result<
    (
        MarketBook,
        StringInterner<InternalOrderId>,
        StringInterner<InternalAccountId>,
    ),
    io::Error,
> {
    let mut book = MarketBook::new_multi_outcome();
    let mut order_interner = StringInterner::default();
    let mut account_interner = StringInterner::default();
    for idx in 0..resting_orders {
        let price_offset = usize_to_i64(idx % 128, "price_offset")?;
        let submitted = usize_to_i64(idx, "submitted_at_micros")?;
        let order_id = format!("ask-{idx}");
        let account_id = format!("maker-{}", idx % 256);
        book.insert(RestingEntry {
            order_id: order_interner.intern(&order_id),
            account_id: account_interner.intern(&account_id),
            outcome: "YES".to_string(),
            side: OrderSide::Sell,
            price_ticks: 100 + price_offset,
            remaining_minor: 1,
            locked_minor: 1,
            submitted_at_micros: submitted,
        });
    }
    Ok((book, order_interner, account_interner))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let resting_orders = parse_env_usize("FLUID_BENCH_RESTING", 10_000)?;
    let iterations = parse_env_usize("FLUID_BENCH_ITERS", 200_000)?;
    if iterations == 0 {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "FLUID_BENCH_ITERS must be > 0").into(),
        );
    }

    let (book, order_interner, account_interner) = build_resting_book(resting_orders)?;
    let incoming = IncomingOrder {
        order_id: OrderId("bench-taker".to_string()),
        account_id: "bench-taker".to_string(),
        market_id: MarketId("bench-market".to_string()),
        outcome: "YES".to_string(),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        tif: TimeInForce::Gtc,
        nonce: OrderNonce(0),
        price_ticks: 10_000,
        quantity_minor: 1,
        submitted_at_micros: 0,
    };

    let warmup = book.match_order(&incoming, &order_interner, &account_interner, "bench-v1", 0)?;
    if warmup.fills.is_empty() {
        return Err(
            io::Error::other("warmup produced no fills; benchmark setup is invalid").into(),
        );
    }

    reset_counters();
    let start = Instant::now();
    let mut first_sequence = 0i64;
    let mut total_fills = 0u64;
    for _ in 0..iterations {
        let outcome = book.match_order(
            &incoming,
            &order_interner,
            &account_interner,
            "bench-v1",
            first_sequence,
        )?;
        first_sequence = first_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fill sequence overflow"))?;
        total_fills =
            total_fills.saturating_add(u64::try_from(outcome.fills.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "fill count conversion failed")
            })?);
    }
    let elapsed = start.elapsed();

    let alloc_calls = ALLOC_CALLS.load(Ordering::Relaxed);
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed);
    let dealloc_calls = DEALLOC_CALLS.load(Ordering::Relaxed);
    let dealloc_bytes = DEALLOC_BYTES.load(Ordering::Relaxed);

    let per_cycle_calls = alloc_calls as f64 / iterations as f64;
    let per_cycle_bytes = alloc_bytes as f64 / iterations as f64;
    let cycles_per_second = iterations as f64 / elapsed.as_secs_f64();

    println!("book_alloc_bench");
    println!("resting_orders={resting_orders}");
    println!("iterations={iterations}");
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
    println!("cycles_per_second={cycles_per_second:.2}");
    println!("alloc_calls_total={alloc_calls}");
    println!("alloc_bytes_total={alloc_bytes}");
    println!("dealloc_calls_total={dealloc_calls}");
    println!("dealloc_bytes_total={dealloc_bytes}");
    println!("alloc_calls_per_cycle={per_cycle_calls:.4}");
    println!("alloc_bytes_per_cycle={per_cycle_bytes:.2}");
    println!("total_fills={total_fills}");

    Ok(())
}
