//! # Streaming K-Way Merge for Sorted Parquet Files
//!
//! This module implements a high-performance streaming k-way merge for sorted
//! Parquet files. It is designed for the OpenSearch Composite Engine merge path
//! where multiple individually-sorted segment files must be merged into a single
//! globally-sorted output file.
//!
//! ## Merge Algorithm
//!
//! The k-way merge uses a three-tier cascade for efficiency:
//!
//! 1. **Tier 1 (single cursor)**: When only one input cursor remains, its
//!    batches are dumped directly to the output — no heap operations needed.
//! 2. **Tier 2 (whole batch)**: If the last sort value in a cursor's current
//!    batch is <= the heap's next-smallest value, the entire remaining batch
//!    is emitted as a zero-copy slice.
//! 3. **Tier 3 (binary search)**: Otherwise, a binary search within the batch
//!    finds the exact boundary where the cursor's values exceed the heap top,
//!    and only the safe prefix is emitted.
//!
//! ## Row ID Rewriting
//!
//! The output file always contains a `___row_id` column with sequential values
//! `0..N`, ensuring globally unique row identifiers in the merged output.
//! A RowIdMapping is returned to Java so that Lucene segments can be reordered
//! to match the sorted Parquet output.

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString, JValue};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType as ArrowDataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::rate_limited_writer::RateLimitedWriter;
use crate::{log_error, log_info};

// =============================================================================
// Constants
// =============================================================================

/// Reserved column name for the synthetic row identifier added during merge.
const ROW_ID_COLUMN_NAME: &str = "___row_id";

/// Number of rows to request per Parquet read batch.
const BATCH_SIZE: usize = 100_000;

/// Approximate number of rows to buffer before flushing to the output file.
const OUTPUT_FLUSH_ROWS: usize = 1_000_000;

/// Disk write rate limit in MB/s.
const RATE_LIMIT_MB_PER_SEC: f64 = 20.0;

// =============================================================================
// Error types
// =============================================================================

/// Result type alias for merge operations.
pub type MergeResult<T> = Result<T, MergeError>;

/// Unified error type for all merge failures.
#[derive(Debug)]
pub enum MergeError {
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
    Io(std::io::Error),
    Logic(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Arrow(e) => write!(f, "Arrow error: {e}"),
            MergeError::Parquet(e) => write!(f, "Parquet error: {e}"),
            MergeError::Io(e) => write!(f, "IO error: {e}"),
            MergeError::Logic(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for MergeError {}

impl From<arrow::error::ArrowError> for MergeError {
    fn from(e: arrow::error::ArrowError) -> Self { MergeError::Arrow(e) }
}
impl From<parquet::errors::ParquetError> for MergeError {
    fn from(e: parquet::errors::ParquetError) -> Self { MergeError::Parquet(e) }
}
impl From<std::io::Error> for MergeError {
    fn from(e: std::io::Error) -> Self { MergeError::Io(e) }
}

// =============================================================================
// Sort value extraction — supports all numeric/temporal Arrow types
// =============================================================================

/// A comparable sort value extracted from an Arrow array cell.
#[derive(Debug, Clone, PartialEq)]
enum SortValue {
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    UInt64(u64),
    Float32(ordered_float::OrderedFloat<f32>),
    Float64(ordered_float::OrderedFloat<f64>),
    Utf8(String),
    Null,
}

impl Eq for SortValue {}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortValue::Null, SortValue::Null) => Ordering::Equal,
            (SortValue::Null, _) => Ordering::Less,
            (_, SortValue::Null) => Ordering::Greater,
            (SortValue::Int32(a), SortValue::Int32(b)) => a.cmp(b),
            (SortValue::Int64(a), SortValue::Int64(b)) => a.cmp(b),
            (SortValue::UInt32(a), SortValue::UInt32(b)) => a.cmp(b),
            (SortValue::UInt64(a), SortValue::UInt64(b)) => a.cmp(b),
            (SortValue::Float32(a), SortValue::Float32(b)) => a.cmp(b),
            (SortValue::Float64(a), SortValue::Float64(b)) => a.cmp(b),
            (SortValue::Utf8(a), SortValue::Utf8(b)) => a.cmp(b),
            _ => Ordering::Equal, // mismatched types — shouldn't happen
        }
    }
}

/// Extract a SortValue from an Arrow array at the given row index.
fn get_sort_value(col: &dyn arrow::array::Array, row: usize) -> SortValue {
    use arrow::array::*;
    if col.is_null(row) {
        return SortValue::Null;
    }
    match col.data_type() {
        ArrowDataType::Int8 => SortValue::Int32(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row) as i32),
        ArrowDataType::Int16 => SortValue::Int32(col.as_any().downcast_ref::<Int16Array>().unwrap().value(row) as i32),
        ArrowDataType::Int32 => SortValue::Int32(col.as_any().downcast_ref::<Int32Array>().unwrap().value(row)),
        ArrowDataType::Int64 => SortValue::Int64(col.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
        ArrowDataType::UInt8 => SortValue::UInt32(col.as_any().downcast_ref::<UInt8Array>().unwrap().value(row) as u32),
        ArrowDataType::UInt16 => SortValue::UInt32(col.as_any().downcast_ref::<UInt16Array>().unwrap().value(row) as u32),
        ArrowDataType::UInt32 => SortValue::UInt32(col.as_any().downcast_ref::<UInt32Array>().unwrap().value(row)),
        ArrowDataType::UInt64 => SortValue::UInt64(col.as_any().downcast_ref::<UInt64Array>().unwrap().value(row)),
        ArrowDataType::Float32 => SortValue::Float32(ordered_float::OrderedFloat(
            col.as_any().downcast_ref::<Float32Array>().unwrap().value(row),
        )),
        ArrowDataType::Float64 => SortValue::Float64(ordered_float::OrderedFloat(
            col.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        )),
        ArrowDataType::Utf8 => SortValue::Utf8(
            col.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string(),
        ),
        ArrowDataType::Date32 => SortValue::Int32(col.as_any().downcast_ref::<Date32Array>().unwrap().value(row)),
        ArrowDataType::Date64 => SortValue::Int64(col.as_any().downcast_ref::<Date64Array>().unwrap().value(row)),
        ArrowDataType::Timestamp(_, _) => {
            // All timestamp variants store as i64 internally
            SortValue::Int64(col.as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(row))
                .unwrap_or_else(|| {
                    // Try via the generic PrimitiveArray<TimestampXType> approach
                    use arrow::array::AsArray;
                    col.as_primitive::<arrow::datatypes::TimestampMillisecondType>().value(row)
                }))
        }
        _ => SortValue::Null, // unsupported type treated as null
    }
}

// =============================================================================
// FileCursor — streaming reader for one sorted Parquet input file
// =============================================================================

/// A cursor over a single sorted Parquet input file.
struct FileCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    current_batch: Option<RecordBatch>,
    row_idx: usize,
    _file_id: usize,
    sort_col_idx: usize,
    /// Original file path, used for RowIdMapping file-id extraction.
    file_path: String,
}

impl FileCursor {
    fn new(
        path: &str,
        file_id: usize,
        sort_column: &str,
        batch_size: usize,
    ) -> MergeResult<Option<Self>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        // Validate sort column exists
        if schema.fields().iter().find(|f| f.name() == sort_column).is_none() {
            let available: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
            return Err(MergeError::Logic(format!(
                "Sort column '{}' not found in file '{}'. Available: {:?}",
                sort_column, path, available
            )));
        }

        let mut reader = builder.with_batch_size(batch_size).build()?;

        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => return Ok(None), // empty file
        };

        let sort_col_idx = first_batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == sort_column)
            .ok_or_else(|| {
                MergeError::Logic(format!(
                    "Sort column '{}' not found after read in file '{}'",
                    sort_column, path
                ))
            })?;

        Ok(Some(FileCursor {
            reader,
            current_batch: Some(first_batch),
            row_idx: 0,
            _file_id: file_id,
            sort_col_idx,
            file_path: path.to_string(),
        }))
    }

    /// Sort value at the current row position.
    fn current_sort_value(&self) -> SortValue {
        let batch = self.current_batch.as_ref().unwrap();
        get_sort_value(batch.column(self.sort_col_idx).as_ref(), self.row_idx)
    }

    /// Sort value at the last row of the current batch.
    fn last_sort_value(&self) -> SortValue {
        let batch = self.current_batch.as_ref().unwrap();
        let last = batch.num_rows() - 1;
        get_sort_value(batch.column(self.sort_col_idx).as_ref(), last)
    }

    /// Number of rows remaining in the current batch (from row_idx to end).
    fn remaining_rows(&self) -> usize {
        let batch = self.current_batch.as_ref().unwrap();
        batch.num_rows() - self.row_idx
    }

    /// Take a slice of the current batch from row_idx..row_idx+len.
    fn take_slice(&self, len: usize) -> RecordBatch {
        let batch = self.current_batch.as_ref().unwrap();
        batch.slice(self.row_idx, len)
    }

    /// Advance the cursor by `n` rows. If we exhaust the current batch,
    /// load the next one. Returns false if the cursor is exhausted.
    fn advance(&mut self, n: usize) -> MergeResult<bool> {
        self.row_idx += n;
        let batch_len = self.current_batch.as_ref().unwrap().num_rows();
        if self.row_idx >= batch_len {
            // Try to load next batch
            match self.reader.next() {
                Some(Ok(b)) if b.num_rows() > 0 => {
                    self.current_batch = Some(b);
                    self.row_idx = 0;
                    Ok(true)
                }
                Some(Err(e)) => Err(e.into()),
                _ => {
                    self.current_batch = None;
                    Ok(false)
                }
            }
        } else {
            Ok(true)
        }
    }

    /// Is this cursor still alive (has data)?
    fn is_alive(&self) -> bool {
        self.current_batch.is_some()
    }

    /// Extract writer generation from filename for RowIdMapping.
    fn writer_generation_id(&self) -> String {
        extract_writer_generation(&self.file_path)
            .unwrap_or_else(|| {
                std::path::Path::new(&self.file_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&self.file_path)
                    .to_string()
            })
    }
}

// =============================================================================
// HeapItem — min-heap entry for k-way merge
// =============================================================================

/// An entry in the merge heap. We want a min-heap, but BinaryHeap is a max-heap,
/// so we reverse the ordering.
struct HeapItem {
    sort_value: SortValue,
    cursor_idx: usize,
    /// When `reverse_sort` is true, we flip the comparison so the heap
    /// yields the largest value first (descending order).
    reverse: bool,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value && self.cursor_idx == other.cursor_idx
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. For ascending sort we reverse so smallest
        // comes out first. For descending sort we keep natural order so largest
        // comes out first.
        let base = self.sort_value.cmp(&other.sort_value);
        let ordered = if self.reverse { base } else { base.reverse() };
        // Tie-break on cursor_idx for stability
        ordered.then_with(|| other.cursor_idx.cmp(&self.cursor_idx))
    }
}

// =============================================================================
// Row ID mapping data (mirrors parquet_merge.rs)
// =============================================================================

pub struct RowIdMappingEntry {
    pub old_file_id: String,
    pub old_row_id: i64,
    pub new_row_id: i64,
}

/// Public entry type for sort permutation caching in SORT_PERMUTATION map.
/// Used by lib.rs to store the permutation produced during sort-on-close.
#[derive(Debug, Clone)]
pub struct SortPermutationEntry {
    pub old_file_id: String,
    pub old_row_id: i64,
    pub new_row_id: i64,
}

impl From<RowIdMappingEntry> for SortPermutationEntry {
    fn from(e: RowIdMappingEntry) -> Self {
        SortPermutationEntry {
            old_file_id: e.old_file_id,
            old_row_id: e.old_row_id,
            new_row_id: e.new_row_id,
        }
    }
}

// =============================================================================
// Helper: extract writer generation from filename
// =============================================================================

fn extract_writer_generation(path: &str) -> Option<String> {
    let filename = std::path::Path::new(path)
        .file_stem()
        .and_then(|n| n.to_str())?;
    filename.rsplit('_').next().map(|s| s.to_string())
}

/// Public wrapper for extract_writer_generation, used by lib.rs IPC staging path.
pub fn extract_writer_generation_pub(path: &str) -> Option<String> {
    extract_writer_generation(path)
}

// =============================================================================
// Helper: rewrite ___row_id column in a batch with sequential IDs
// =============================================================================

fn rewrite_row_ids(batch: &RecordBatch, start_row_id: i64) -> MergeResult<RecordBatch> {
    let num_rows = batch.num_rows();
    let row_ids: Int64Array = (start_row_id..start_row_id + num_rows as i64).collect();

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, col) in batch.columns().iter().enumerate() {
        if schema.field(i).name() == ROW_ID_COLUMN_NAME {
            columns.push(Arc::new(row_ids.clone()));
        } else {
            columns.push(col.clone());
        }
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Public wrapper for rewrite_row_ids, used by lib.rs IPC staging path.
pub fn rewrite_row_ids_pub(batch: &RecordBatch, start_row_id: i64) -> MergeResult<RecordBatch> {
    rewrite_row_ids(batch, start_row_id)
}

// =============================================================================
// Core merge function
// =============================================================================

/// Perform a streaming k-way sorted merge of the input Parquet files.
///
/// Returns the row ID mapping entries and the output file identifier.
pub fn merge_streaming_sorted(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
    reverse_sort: bool,
) -> MergeResult<(Vec<RowIdMappingEntry>, String)> {
    if input_files.is_empty() {
        return Err(MergeError::Logic("No input files provided".into()));
    }

    log_info!(
        "[MERGE_STREAM] Starting sorted merge of {} files to {}, sort_column={}, reverse={}",
        input_files.len(), output_path, sort_column, reverse_sort
    );

    // Open cursors for all input files
    let mut cursors: Vec<FileCursor> = Vec::new();
    // Track per-file original row counts for RowIdMapping
    let mut file_row_counts: Vec<(String, usize)> = Vec::new(); // (writer_gen_id, total_rows_in_file)

    for (i, path) in input_files.iter().enumerate() {
        if !std::path::Path::new(path).exists() {
            return Err(MergeError::Logic(format!("Input file not found: {}", path)));
        }
        // Count total rows in file for RowIdMapping
        let file = File::open(path)?;
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let file_metadata = reader_builder.metadata().file_metadata().clone();
        let total_rows = file_metadata.num_rows() as usize;

        match FileCursor::new(path, i, sort_column, BATCH_SIZE)? {
            Some(cursor) => {
                let gen_id = cursor.writer_generation_id();
                file_row_counts.push((gen_id, total_rows));
                cursors.push(cursor);
            }
            None => {
                // Empty file, still record it with 0 rows
                let gen_id = extract_writer_generation(path)
                    .unwrap_or_else(|| path.to_string());
                file_row_counts.push((gen_id, 0));
            }
        }
    }

    if cursors.is_empty() {
        // All files empty — write empty output
        let first_file = File::open(&input_files[0])?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
        let schema = builder.schema().clone();
        let out_file = File::create(output_path)?;
        let throttled = RateLimitedWriter::new(out_file, RATE_LIMIT_MB_PER_SEC * 1024.0 * 1024.0)
            .map_err(|e| MergeError::Io(e))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let writer = ArrowWriter::try_new(throttled, schema, Some(props))?;
        writer.close()?;

        let output_file_id = std::path::Path::new(output_path)
            .file_name().and_then(|n| n.to_str()).unwrap_or(output_path).to_string();
        return Ok((Vec::new(), output_file_id));
    }

    // Read schema from first cursor's batch for the output writer
    let output_schema = cursors[0].current_batch.as_ref().unwrap().schema();

    // Create output writer
    let out_file = File::create(output_path)?;
    let throttled = RateLimitedWriter::new(out_file, RATE_LIMIT_MB_PER_SEC * 1024.0 * 1024.0)
        .map_err(|e| MergeError::Io(e))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(throttled, output_schema.clone(), Some(props))?;

    // Build initial heap
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (idx, cursor) in cursors.iter().enumerate() {
        heap.push(HeapItem {
            sort_value: cursor.current_sort_value(),
            cursor_idx: idx,
            reverse: reverse_sort,
        });
    }

    // Output row tracking
    let mut global_row_id: i64 = 0;
    let mut output_buffer: Vec<RecordBatch> = Vec::new();
    let mut buffered_rows: usize = 0;

    // Track the mapping: for each output row, which (file_id, original_row_within_file)
    // We build this by tracking how many rows we've consumed from each cursor.
    let mut cursor_rows_consumed: Vec<i64> = vec![0; cursors.len()];

    // Mapping entries
    let mut mappings: Vec<RowIdMappingEntry> = Vec::new();

    // Macro to flush buffered batches to writer
    let flush_buffer = |writer: &mut ArrowWriter<RateLimitedWriter<File>>,
                        buffer: &mut Vec<RecordBatch>,
                        buffered: &mut usize,
                        row_id: &mut i64| -> MergeResult<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        let schema = buffer[0].schema();
        let combined = compute::concat_batches(&schema, buffer.iter())?;
        let rewritten = rewrite_row_ids(&combined, *row_id)?;
        writer.write(&rewritten)?;
        *row_id += combined.num_rows() as i64;
        buffer.clear();
        *buffered = 0;
        Ok(())
    };

    // K-way merge loop
    while !heap.is_empty() {
        let top = heap.pop().unwrap();
        let cidx = top.cursor_idx;

        if !cursors[cidx].is_alive() {
            continue;
        }

        // Tier 1: single cursor remaining — dump everything
        if heap.is_empty() {
            loop {
                let remaining = cursors[cidx].remaining_rows();
                let slice = cursors[cidx].take_slice(remaining);

                // Record mappings for this slice
                let file_id_str = cursors[cidx].writer_generation_id();
                for r in 0..remaining {
                    mappings.push(RowIdMappingEntry {
                        old_file_id: file_id_str.clone(),
                        old_row_id: cursor_rows_consumed[cidx] + r as i64,
                        new_row_id: global_row_id + buffered_rows as i64 + r as i64,
                    });
                }
                cursor_rows_consumed[cidx] += remaining as i64;

                output_buffer.push(slice);
                buffered_rows += remaining;

                if buffered_rows >= OUTPUT_FLUSH_ROWS {
                    flush_buffer(&mut writer, &mut output_buffer, &mut buffered_rows, &mut global_row_id)?;
                }

                if !cursors[cidx].advance(remaining)? {
                    break;
                }
            }
            break;
        }

        // Peek at the next-smallest value in the heap
        let heap_next_value = heap.peek().unwrap().sort_value.clone();

        // Tier 2: whole-batch optimization
        let cursor_last = cursors[cidx].last_sort_value();
        let batch_fits = if reverse_sort {
            cursor_last >= heap_next_value
        } else {
            cursor_last <= heap_next_value
        };

        if batch_fits {
            let remaining = cursors[cidx].remaining_rows();
            let slice = cursors[cidx].take_slice(remaining);

            let file_id_str = cursors[cidx].writer_generation_id();
            for r in 0..remaining {
                mappings.push(RowIdMappingEntry {
                    old_file_id: file_id_str.clone(),
                    old_row_id: cursor_rows_consumed[cidx] + r as i64,
                    new_row_id: global_row_id + buffered_rows as i64 + r as i64,
                });
            }
            cursor_rows_consumed[cidx] += remaining as i64;

            output_buffer.push(slice);
            buffered_rows += remaining;

            if buffered_rows >= OUTPUT_FLUSH_ROWS {
                flush_buffer(&mut writer, &mut output_buffer, &mut buffered_rows, &mut global_row_id)?;
            }

            if cursors[cidx].advance(remaining)? {
                heap.push(HeapItem {
                    sort_value: cursors[cidx].current_sort_value(),
                    cursor_idx: cidx,
                    reverse: reverse_sort,
                });
            }
            continue;
        }

        // Tier 3: binary search within the batch to find the safe prefix
        let batch = cursors[cidx].current_batch.as_ref().unwrap();
        let sort_col = batch.column(cursors[cidx].sort_col_idx);
        let start = cursors[cidx].row_idx;
        let end = batch.num_rows();

        // Binary search for the boundary where values exceed heap_next_value
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_val = get_sort_value(sort_col.as_ref(), mid);
            let exceeds = if reverse_sort {
                mid_val < heap_next_value
            } else {
                mid_val > heap_next_value
            };
            if exceeds {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        let safe_count = lo - start;
        if safe_count == 0 {
            // Edge case: even the first row exceeds — emit just one row
            let slice = cursors[cidx].take_slice(1);
            let file_id_str = cursors[cidx].writer_generation_id();
            mappings.push(RowIdMappingEntry {
                old_file_id: file_id_str,
                old_row_id: cursor_rows_consumed[cidx],
                new_row_id: global_row_id + buffered_rows as i64,
            });
            cursor_rows_consumed[cidx] += 1;

            output_buffer.push(slice);
            buffered_rows += 1;

            if cursors[cidx].advance(1)? {
                heap.push(HeapItem {
                    sort_value: cursors[cidx].current_sort_value(),
                    cursor_idx: cidx,
                    reverse: reverse_sort,
                });
            }
        } else {
            let slice = cursors[cidx].take_slice(safe_count);
            let file_id_str = cursors[cidx].writer_generation_id();
            for r in 0..safe_count {
                mappings.push(RowIdMappingEntry {
                    old_file_id: file_id_str.clone(),
                    old_row_id: cursor_rows_consumed[cidx] + r as i64,
                    new_row_id: global_row_id + buffered_rows as i64 + r as i64,
                });
            }
            cursor_rows_consumed[cidx] += safe_count as i64;

            output_buffer.push(slice);
            buffered_rows += safe_count;

            if cursors[cidx].advance(safe_count)? {
                heap.push(HeapItem {
                    sort_value: cursors[cidx].current_sort_value(),
                    cursor_idx: cidx,
                    reverse: reverse_sort,
                });
            }
        }

        if buffered_rows >= OUTPUT_FLUSH_ROWS {
            flush_buffer(&mut writer, &mut output_buffer, &mut buffered_rows, &mut global_row_id)?;
        }
    }

    // Flush remaining buffer
    flush_buffer(&mut writer, &mut output_buffer, &mut buffered_rows, &mut global_row_id)?;

    // Close writer
    let file_metadata = writer.close()?;
    log_info!(
        "[MERGE_STREAM] Sorted merge complete: {} total rows written",
        file_metadata.num_rows
    );

    let output_file_id = std::path::Path::new(output_path)
        .file_name().and_then(|n| n.to_str()).unwrap_or(output_path).to_string();

    Ok((mappings, output_file_id))
}

// =============================================================================
// Sort-on-write: sort a single Parquet file by a column
// =============================================================================

/// Sort a single Parquet file in-place by the given sort column.
/// Reads the file, sorts all batches, writes to a temp file, then renames.
///
/// Returns the sort permutation as a Vec of RowIdMappingEntry, where each entry
/// maps (old_row_id → new_row_id) within the same file. This permutation can be
/// used directly to reorder Lucene segments without a redundant re-merge step.
pub fn sort_parquet_file(
    file_path: &str,
    sort_column: &str,
    reverse_sort: bool,
) -> MergeResult<Vec<RowIdMappingEntry>> {
    use arrow::compute::SortOptions;

    log_info!(
        "[SORT] Sorting file {} by column '{}' (reverse={})",
        file_path, sort_column, reverse_sort
    );

    // Read all batches from the file
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.with_batch_size(BATCH_SIZE).build()?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        if batch.num_rows() > 0 {
            all_batches.push(batch);
        }
    }

    if all_batches.is_empty() {
        return Ok(Vec::new()); // nothing to sort
    }

    // Concatenate all batches into one
    let combined = compute::concat_batches(&schema, all_batches.iter())?;

    // Find sort column index
    let sort_col_idx = schema.fields().iter().position(|f| f.name() == sort_column)
        .ok_or_else(|| MergeError::Logic(format!(
            "Sort column '{}' not found in file '{}'", sort_column, file_path
        )))?;

    // Sort using arrow::compute::sort_to_indices + take
    let sort_col = combined.column(sort_col_idx);
    let options = SortOptions {
        descending: reverse_sort,
        nulls_first: true,
    };
    let indices = arrow::compute::sort_to_indices(sort_col, Some(options), None)?;

    // Build the sort permutation: indices[new_pos] = old_pos
    // So: old_row_id = indices[i], new_row_id = i
    let file_id = extract_writer_generation(file_path)
        .unwrap_or_else(|| {
            std::path::Path::new(file_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(file_path)
                .to_string()
        });

    let num_rows = indices.len();
    let mut mappings: Vec<RowIdMappingEntry> = Vec::with_capacity(num_rows);
    for new_row_id in 0..num_rows {
        let old_row_id = indices.value(new_row_id) as i64;
        mappings.push(RowIdMappingEntry {
            old_file_id: file_id.clone(),
            old_row_id,
            new_row_id: new_row_id as i64,
        });
    }

    // Apply sort indices to all columns
    let sorted_columns: Vec<ArrayRef> = combined.columns().iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;

    // Rewrite ___row_id column with sequential values
    let final_batch = rewrite_row_ids(&sorted_batch, 0)?;

    // Write to temp file, then rename
    let temp_path = format!("{}.sorting.tmp", file_path);
    let out_file = File::create(&temp_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))?;
    writer.write(&final_batch)?;
    writer.close()?;

    // Atomic rename
    std::fs::rename(&temp_path, file_path)?;

    log_info!("[SORT] File sorted successfully: {} ({} rows, {} mappings)",
        file_path, num_rows, mappings.len());
    Ok(mappings)
}

// =============================================================================
// JNI helpers
// =============================================================================

fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let iterator = env.call_method(&list, "iterator", "()Ljava/util/Iterator;", &[])?
        .l()?;

    let mut result = Vec::new();
    while env.call_method(&iterator, "hasNext", "()Z", &[])?.z()? {
        let element = env.call_method(&iterator, "next", "()Ljava/lang/Object;", &[])?
            .l()?;
        let path_string = env.call_method(&element, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let jstring = JString::from(path_string);
        let string = env.get_string(&jstring)?;
        result.push(string.to_str()?.to_string());
    }

    Ok(result)
}

fn create_row_id_mapping_object<'local>(
    env: &mut JNIEnv<'local>,
    mappings: Vec<RowIdMappingEntry>,
    output_file_id: &str,
) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
    let size = mappings.len();

    let old_row_ids = env.new_long_array(size as i32)?;
    let new_row_ids = env.new_long_array(size as i32)?;
    let file_ids = env.new_object_array(
        size as i32,
        "java/lang/String",
        JObject::null(),
    )?;

    let mut old_ids_vec = Vec::with_capacity(size);
    let mut new_ids_vec = Vec::with_capacity(size);

    for (i, mapping) in mappings.into_iter().enumerate() {
        old_ids_vec.push(mapping.old_row_id);
        new_ids_vec.push(mapping.new_row_id);
        let file_id_str = env.new_string(&mapping.old_file_id)?;
        env.set_object_array_element(&file_ids, i as i32, file_id_str)?;
    }

    env.set_long_array_region(&old_row_ids, 0, &old_ids_vec)?;
    env.set_long_array_region(&new_row_ids, 0, &new_ids_vec)?;

    let row_id_mapping = env.new_object(
        "org/opensearch/index/engine/exec/merge/RowIdMapping",
        "([J[J[Ljava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&old_row_ids.into()),
            JValue::Object(&new_row_ids.into()),
            JValue::Object(&file_ids.into()),
            JValue::Object(&env.new_string(output_file_id)?.into()),
        ],
    )?;

    Ok(row_id_mapping)
}

// =============================================================================
// JNI entry point: sorted merge (returns RowIdMapping like parquet_merge.rs)
// =============================================================================

/// JNI entry point for streaming sorted merge. Called from
/// `RustBridge.mergeParquetFilesSorted`. Returns a RowIdMapping object.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesSorted<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input_files: JObject<'local>,
    output_file: JString<'local>,
    sort_key: JString<'local>,
    is_reverse: jni::sys::jboolean,
) -> JObject<'local> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        let sort_key_str: String = if sort_key.is_null() {
            return Err("Sort key must not be null for sorted merge".to_string());
        } else {
            env.get_string(&sort_key)
                .map_err(|e| format!("Failed to get sort key string: {}", e))?
                .into()
        };
        let is_reverse_bool = is_reverse != 0;

        log_info!(
            "[JNI] mergeParquetFilesSorted: {} files, output={}, sort={}, reverse={}",
            input_files_vec.len(), output_path, sort_key_str, is_reverse_bool
        );

        merge_streaming_sorted(&input_files_vec, &output_path, &sort_key_str, is_reverse_bool)
            .map_err(|e| format!("{}", e))
    }));

    match result {
        Ok(Ok((mappings, output_file_id))) => {
            match create_row_id_mapping_object(&mut env, mappings, &output_file_id) {
                Ok(obj) => obj,
                Err(e) => {
                    log_error!("[JNI] Failed to create RowIdMapping: {}", e);
                    let _ = env.throw_new("java/lang/RuntimeException",
                        &format!("Failed to create RowIdMapping: {}", e));
                    JObject::null()
                }
            }
        }
        Ok(Err(e)) => {
            log_error!("[JNI] Sorted merge failed: {}", e);
            let _ = env.throw_new("java/lang/RuntimeException",
                &format!("Sorted merge failed: {}", e));
            JObject::null()
        }
        Err(e) => {
            log_error!("[JNI] Rust panic in sorted merge: {:?}", e);
            let _ = env.throw_new("java/lang/RuntimeException",
                &format!("Rust panic: {:?}", e));
            JObject::null()
        }
    }
}

// =============================================================================
// JNI entry point: sort a single file on close
// =============================================================================

/// JNI entry point for sorting a single Parquet file in-place.
/// Called from `RustBridge.sortParquetFile`.
/// Returns a RowIdMapping containing the sort permutation (old_row_id → new_row_id).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_sortParquetFile<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    file_path: JString<'local>,
    sort_column: JString<'local>,
    is_reverse: jni::sys::jboolean,
) -> JObject<'local> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let path: String = env.get_string(&file_path)
            .map_err(|e| format!("Failed to get file path: {}", e))?
            .into();
        let col: String = env.get_string(&sort_column)
            .map_err(|e| format!("Failed to get sort column: {}", e))?
            .into();
        let reverse = is_reverse != 0;

        sort_parquet_file(&path, &col, reverse)
            .map_err(|e| format!("{}", e))
    }));

    match result {
        Ok(Ok(mappings)) => {
            // Extract file_id from the mappings (all entries share the same file_id)
            let file_id = mappings.first()
                .map(|m| m.old_file_id.clone())
                .unwrap_or_default();
            match create_row_id_mapping_object(&mut env, mappings, &file_id) {
                Ok(obj) => obj,
                Err(e) => {
                    log_error!("[JNI] Failed to create RowIdMapping from sort: {}", e);
                    let _ = env.throw_new("java/io/IOException",
                        &format!("Failed to create RowIdMapping: {}", e));
                    JObject::null()
                }
            }
        }
        Ok(Err(e)) => {
            log_error!("[JNI] sortParquetFile failed: {}", e);
            let _ = env.throw_new("java/io/IOException",
                &format!("Sort failed: {}", e));
            JObject::null()
        }
        Err(e) => {
            log_error!("[JNI] Rust panic in sortParquetFile: {:?}", e);
            let _ = env.throw_new("java/io/IOException",
                &format!("Rust panic: {:?}", e));
            JObject::null()
        }
    }
}
