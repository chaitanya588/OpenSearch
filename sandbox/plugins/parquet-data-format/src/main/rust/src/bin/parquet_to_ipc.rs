/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Convert a Parquet file to Arrow IPC format.
//!
//! Usage:
//!   cargo run --release --bin parquet_to_ipc -- <input.parquet>
//!
//! Output is written to <input.parquet>.ipc

use std::env;
use std::fs::{self, File};

use arrow_ipc::writer::FileWriter as IpcFileWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: parquet_to_ipc <input.parquet>");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = format!("{}.ipc", input_path);

    let file = File::open(input_path).expect("Failed to open input Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("Failed to read Parquet metadata");
    let schema = builder.schema().clone();
    let reader = builder.build().expect("Failed to build Parquet reader");

    let out_file = File::create(&output_path).expect("Failed to create output IPC file");
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

    let input_size = fs::metadata(input_path).expect("Failed to stat input").len();
    let output_size = fs::metadata(&output_path).expect("Failed to stat output").len();

    eprintln!(
        "Converted {} -> {}\n  {} batches, {} rows\n  Parquet: {:.2} MB -> IPC: {:.2} MB",
        input_path,
        output_path,
        batch_count,
        total_rows,
        input_size as f64 / (1024.0 * 1024.0),
        output_size as f64 / (1024.0 * 1024.0),
    );
}
