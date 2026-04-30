/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Benchmark: lexsort_to_indices vs RowConverter for sort_large_file.
//!
//! Takes a Parquet file as input, converts it to IPC, then benchmarks
//! both sort variants with detailed metrics (time, file sizes, memory, CPU).
//!
//! Usage:
//!   cargo run --release --features test-utils --bin sort_bench -- [OPTIONS]
//!
//! Options:
//!   --file <PATH>           Parquet file to benchmark (required)
//!   --sort-cols <NAMES>     Comma-separated sort column names (required)
//!   --ipc-batch-size <N>    Batch size for Parquet->IPC conversion (default: 8192)
//!   --sort-batch-size <N>   Batch size for sort chunks in NativeWriter (default: 8192)
//!   --iters <N>             Iterations per variant (default: 3)

use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::time::Instant;

use arrow_ipc::writer::FileWriter as IpcFileWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use opensearch_parquet_format::writer::NativeParquetWriter;
use opensearch_parquet_format::merge::stats::SortStats;

fn parse_arg(args: &[String], flag: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

// =============================================================================
// Resource measurement helpers
// =============================================================================

/// Snapshot of process resource usage.
struct ResourceSnapshot {
    rss_bytes: u64,
    user_cpu_us: u64,
    sys_cpu_us: u64,
}

/// Read current process RSS and CPU times.
/// Uses /proc/self on Linux, getrusage on macOS.
fn take_snapshot() -> ResourceSnapshot {
    #[cfg(target_os = "linux")]
    {
        linux_snapshot()
    }
    #[cfg(not(target_os = "linux"))]
    {
        rusage_snapshot()
    }
}

#[cfg(target_os = "linux")]
fn linux_snapshot() -> ResourceSnapshot {
    // RSS from /proc/self/statm (field 1, in pages)
    let rss_bytes = fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0);

    // CPU times from /proc/self/stat (fields 14=utime, 15=stime, in clock ticks)
    let (user_cpu_us, sys_cpu_us) = fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            // Fields after the comm (which is in parens)
            let after_comm = s.rfind(')')? ;
            let fields: Vec<&str> = s[after_comm + 2..].split_whitespace().collect();
            // field index 11 = utime, 12 = stime (0-indexed after comm closing paren)
            let ticks_per_sec = 100u64; // sysconf(_SC_CLK_TCK) is typically 100
            let utime = fields.get(11)?.parse::<u64>().ok()?;
            let stime = fields.get(12)?.parse::<u64>().ok()?;
            Some((utime * 1_000_000 / ticks_per_sec, stime * 1_000_000 / ticks_per_sec))
        })
        .unwrap_or((0, 0));

    ResourceSnapshot { rss_bytes, user_cpu_us, sys_cpu_us }
}

#[cfg(not(target_os = "linux"))]
fn rusage_snapshot() -> ResourceSnapshot {
    use std::mem::MaybeUninit;
    unsafe {
        let mut usage = MaybeUninit::<libc::rusage>::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        ResourceSnapshot {
            rss_bytes: (usage.ru_maxrss as u64) * 1024, // macOS reports in KB
            user_cpu_us: usage.ru_utime.tv_sec as u64 * 1_000_000 + usage.ru_utime.tv_usec as u64,
            sys_cpu_us: usage.ru_stime.tv_sec as u64 * 1_000_000 + usage.ru_stime.tv_usec as u64,
        }
    }
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

// =============================================================================
// Parquet to IPC conversion
// =============================================================================

fn parquet_to_ipc(parquet_path: &str, ipc_path: &str, batch_size: usize) {
    let file = File::open(parquet_path).expect("Failed to open Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("Failed to read Parquet metadata");
    let schema = builder.schema().clone();
    let reader = builder.with_batch_size(batch_size).build().expect("Failed to build Parquet reader");

    let out_file = File::create(ipc_path).expect("Failed to create IPC file");
    let mut writer = IpcFileWriter::try_new(out_file, &schema).expect("Failed to create IPC writer");

    let mut total_rows: usize = 0;
    let mut batch_count: usize = 0;

    for batch_result in reader {
        let batch = batch_result.expect("Failed to read batch");
        total_rows += batch.num_rows();
        batch_count += 1;
        writer.write(&batch).expect("Failed to write IPC batch");
    }

    writer.finish().expect("Failed to finish IPC writer");

    let parquet_size = fs::metadata(parquet_path).expect("Failed to stat Parquet").len();
    let ipc_size = fs::metadata(ipc_path).expect("Failed to stat IPC").len();

    eprintln!(
        "  Converted Parquet -> IPC: {} batches, {} rows",
        batch_count, total_rows
    );
    eprintln!(
        "  Parquet: {} -> IPC: {} (ipc_batch_size={})",
        fmt_bytes(parquet_size),
        fmt_bytes(ipc_size),
        batch_size
    );
}

// =============================================================================
// Benchmark results
// =============================================================================

struct BenchResult {
    elapsed: std::time::Duration,
    output_size: u64,
    rss_delta: i64,
    peak_rss: u64,
    user_cpu_ms: f64,
    sys_cpu_ms: f64,
    sort_stats: Option<SortStats>,
}

fn run_bench(
    label: &str,
    ipc_path: &str,
    output_dir: &Path,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
    batch_size: usize,
    use_row_conversion: bool,
    iters: usize,
) -> Vec<BenchResult> {
    let mut results = Vec::with_capacity(iters);

    for i in 0..iters {
        let output = output_dir
            .join(format!("{}_{}.parquet", label, i))
            .to_string_lossy()
            .to_string();

        let before = take_snapshot();
        let start = Instant::now();

        // Use stats variant for row_conversion, plain variant for lexsort
        // (lexsort comparisons happen inside opaque Arrow function, can't instrument)
        let sort_stats = if use_row_conversion {
            let stats = NativeParquetWriter::bench_sort_large_file_with_stats(
                ipc_path, &output, "bench-index",
                sort_columns, reverse_sorts, nulls_first,
                batch_size, true,
            ).expect("sort_large_file_with_stats failed");
            Some(stats)
        } else {
            NativeParquetWriter::bench_sort_large_file_with_stats(
                ipc_path, &output, "bench-index",
                sort_columns, reverse_sorts, nulls_first,
                batch_size, false,
            ).ok() // lexsort won't have chunk_sort_comparisons but merge stats are valid
        };

        let elapsed = start.elapsed();
        let after = take_snapshot();

        let output_size = fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
        let rss_delta = after.rss_bytes as i64 - before.rss_bytes as i64;
        let user_cpu_ms = (after.user_cpu_us.saturating_sub(before.user_cpu_us)) as f64 / 1000.0;
        let sys_cpu_ms = (after.sys_cpu_us.saturating_sub(before.sys_cpu_us)) as f64 / 1000.0;

        results.push(BenchResult {
            elapsed,
            output_size,
            rss_delta,
            peak_rss: after.rss_bytes,
            user_cpu_ms,
            sys_cpu_ms,
            sort_stats,
        });

        if i < iters - 1 {
            let _ = fs::remove_file(&output);
        } else {
            eprintln!("  Output kept: {}", output);
        }
    }

    results
}

fn print_stats(label: &str, results: &[BenchResult]) {
    let n = results.len();
    let millis: Vec<f64> = results.iter().map(|r| r.elapsed.as_secs_f64() * 1000.0).collect();
    let min = millis.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = millis.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let mean = millis.iter().sum::<f64>() / n as f64;

    let avg_output_size = results.iter().map(|r| r.output_size).sum::<u64>() / n as u64;
    let avg_user_cpu = results.iter().map(|r| r.user_cpu_ms).sum::<f64>() / n as f64;
    let avg_sys_cpu = results.iter().map(|r| r.sys_cpu_ms).sum::<f64>() / n as f64;
    let peak_rss = results.iter().map(|r| r.peak_rss).max().unwrap_or(0);
    let avg_rss_delta = results.iter().map(|r| r.rss_delta).sum::<i64>() / n as i64;

    eprintln!("  --- {} ---", label);
    eprintln!(
        "    time:       iters={:>2}  min={:>8.1}ms  mean={:>8.1}ms  max={:>8.1}ms",
        n, min, mean, max
    );
    eprintln!("    output:     {}", fmt_bytes(avg_output_size));
    eprintln!("    cpu_user:   {:.1}ms avg", avg_user_cpu);
    eprintln!("    cpu_sys:    {:.1}ms avg", avg_sys_cpu);
    eprintln!("    cpu_total:  {:.1}ms avg", avg_user_cpu + avg_sys_cpu);
    eprintln!("    peak_rss:   {}", fmt_bytes(peak_rss));
    eprintln!("    rss_delta:  {:+.2} MB avg", avg_rss_delta as f64 / (1024.0 * 1024.0));

    // Print sort stats from the last iteration (representative)
    if let Some(ref stats) = results.last().and_then(|r| r.sort_stats.as_ref()) {
        eprintln!("    --- comparison stats (last iter) ---");
        eprintln!("    chunks_sorted:          {}", stats.chunk_count);
        eprintln!("    chunk_total_size:       {}", fmt_bytes(stats.chunk_total_bytes));
        eprintln!("    chunk_sort_comparisons: {}", stats.chunk_sort_comparisons);
        eprintln!("    merge_heap_pops:        {}", stats.merge.heap_pops);
        eprintln!("    merge_heap_pushes:      {}", stats.merge.heap_pushes);
        eprintln!("    merge_tier1_rows:       {}", stats.merge.tier1_rows);
        eprintln!("    merge_tier2_skips:      {}", stats.merge.tier2_skips);
        eprintln!("    merge_tier2_rows:       {}", stats.merge.tier2_rows);
        eprintln!("    merge_tier3_searches:   {}", stats.merge.tier3_searches);
        eprintln!("    merge_tier3_comparisons:{}", stats.merge.tier3_comparisons);
        eprintln!("    merge_tier3_rows:       {}", stats.merge.tier3_rows);
        eprintln!("    total_comparisons:      {}", stats.total_comparisons());
    }
    eprintln!();
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        eprintln!("Usage: sort_bench --file <PARQUET_PATH> --sort-cols <COL1,COL2,...> [OPTIONS]");
        eprintln!("  --file <PATH>           Parquet file to benchmark (required)");
        eprintln!("  --sort-cols <NAMES>     Comma-separated sort column names (required)");
        eprintln!("  --ipc-batch-size <N>    Batch size for Parquet->IPC conversion (default: 8192)");
        eprintln!("  --sort-batch-size <N>   Batch size for sort chunks in NativeWriter (default: 8192)");
        eprintln!("  --iters <N>             Iterations per variant (default: 3)");
        return;
    }

    let parquet_file = parse_arg(&args, "--file", "");
    if parquet_file.is_empty() {
        eprintln!("Error: --file is required");
        std::process::exit(1);
    }

    let sort_cols_str = parse_arg(&args, "--sort-cols", "");
    if sort_cols_str.is_empty() {
        eprintln!("Error: --sort-cols is required (comma-separated column names)");
        std::process::exit(1);
    }

    let ipc_batch_size: usize = parse_arg(&args, "--ipc-batch-size", "8192").parse().expect("Invalid --ipc-batch-size");
    let sort_batch_size: usize = parse_arg(&args, "--sort-batch-size", "8192").parse().expect("Invalid --sort-batch-size");
    let iters: usize = parse_arg(&args, "--iters", "3").parse().expect("Invalid --iters");

    let sort_columns: Vec<String> = sort_cols_str.split(',').map(|s| s.trim().to_string()).collect();
    let num_sort_cols = sort_columns.len();
    let reverse_sorts: Vec<bool> = vec![false; num_sort_cols];
    let nulls_first: Vec<bool> = vec![false; num_sort_cols];

    let parquet_path = Path::new(&parquet_file);
    let parent_dir = parquet_path.parent().unwrap_or_else(|| Path::new("."));
    let output_dir = parent_dir.join("sorted_files");
    fs::create_dir_all(&output_dir).expect("Failed to create sorted_files directory");

    let parquet_filename = parquet_path.file_name().unwrap().to_string_lossy();
    let ipc_path = output_dir
        .join(format!("{}.ipc", parquet_filename))
        .to_string_lossy()
        .to_string();

    let input_size = fs::metadata(&parquet_file).expect("Failed to stat input").len();

    eprintln!("\n=== Sort Benchmark ===");
    eprintln!("  input:           {} ({})", parquet_file, fmt_bytes(input_size));
    eprintln!("  sort_cols:       {:?}", sort_columns);
    eprintln!("  ipc_batch_size:  {}", ipc_batch_size);
    eprintln!("  sort_batch_size: {}", sort_batch_size);
    eprintln!("  iters:           {}", iters);
    eprintln!("  output_dir:      {}\n", output_dir.display());

    // Step 1: Convert Parquet to IPC
    eprintln!("Step 1: Converting Parquet to IPC...");
    let convert_start = Instant::now();
    parquet_to_ipc(&parquet_file, &ipc_path, ipc_batch_size);
    eprintln!("  Conversion took: {:.1}ms\n", convert_start.elapsed().as_secs_f64() * 1000.0);

    // Step 2: Warmup
    eprintln!("Step 2: Warmup...");
    let warmup_out = output_dir.join("warmup.parquet").to_string_lossy().to_string();
    for use_rc in [false, true] {
        let _ = NativeParquetWriter::bench_sort_large_file(
            &ipc_path, &warmup_out, "bench-index",
            &sort_columns, &reverse_sorts, &nulls_first,
            sort_batch_size, use_rc,
        );
        let _ = fs::remove_file(&warmup_out);
    }

    // Step 3: Benchmark
    eprintln!("\nStep 3: Running benchmarks...\n");

    let lexsort_results = run_bench(
        "lexsort", &ipc_path, &output_dir,
        &sort_columns, &reverse_sorts, &nulls_first,
        sort_batch_size, false, iters,
    );
    print_stats("lexsort_to_indices", &lexsort_results);

    let rowconv_results = run_bench(
        "row_conversion", &ipc_path, &output_dir,
        &sort_columns, &reverse_sorts, &nulls_first,
        sort_batch_size, true, iters,
    );
    print_stats("row_conversion", &rowconv_results);

    // Summary
    let lexsort_mean = lexsort_results.iter().map(|r| r.elapsed.as_secs_f64()).sum::<f64>() / iters as f64;
    let rowconv_mean = rowconv_results.iter().map(|r| r.elapsed.as_secs_f64()).sum::<f64>() / iters as f64;
    let speedup = lexsort_mean / rowconv_mean;

    let lexsort_cpu = lexsort_results.iter().map(|r| r.user_cpu_ms + r.sys_cpu_ms).sum::<f64>() / iters as f64;
    let rowconv_cpu = rowconv_results.iter().map(|r| r.user_cpu_ms + r.sys_cpu_ms).sum::<f64>() / iters as f64;

    eprintln!("=== Summary ===");
    eprintln!("  time speedup (row_conversion): {:.2}x", speedup);
    eprintln!("  cpu  speedup (row_conversion): {:.2}x", lexsort_cpu / rowconv_cpu);

    // Cleanup IPC file
    let _ = fs::remove_file(&ipc_path);

    eprintln!("\nDone.");
}
