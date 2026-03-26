use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::{Arc, Mutex};
use parquet::format::FileMetaData as FormatFileMetaData;
use parquet::file::metadata::FileMetaData as FileFileMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_ipc::reader::FileReader as IpcFileReader;

pub mod logger;
pub mod parquet_merge;
pub mod parquet_merge_stream;
pub mod rate_limited_writer;
pub mod field_config;
pub mod native_settings;
pub mod writer_properties_builder;

pub use parquet_merge::*;
pub use parquet_merge_stream::*;

// Re-export macros from the shared crate for logging
pub use vectorized_exec_spi::{log_info, log_error, log_debug};

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
    /// Manages Arrow IPC file writers for the sort-on-close path.
    /// When a sort column is configured, batches are written as raw Arrow IPC
    /// (no Parquet encoding/compression) to avoid the redundant encode→decode
    /// cycle before sorting.
    static ref IPC_WRITER_MANAGER: DashMap<String, Arc<Mutex<IpcFileWriter<File>>>> = DashMap::new();
    /// Tracks sort configuration per file. When a writer is created with a sort column,
    /// the config is stored here and applied when the writer is closed.
    static ref SORT_CONFIG: DashMap<String, SortConfig> = DashMap::new();
    /// Caches the sort permutation (RowIdMapping entries) produced during sort-on-close.
    /// Key is the file path. Retrieved by Java via getSortPermutation() JNI call,
    /// which removes the entry from the map.
    static ref SORT_PERMUTATION: DashMap<String, Vec<parquet_merge_stream::SortPermutationEntry>> = DashMap::new();
}

/// Sort configuration for a writer file.
#[derive(Debug, Clone)]
struct SortConfig {
    sort_column: String,
    reverse_sort: bool,
}

/// Path suffix for the intermediate Arrow IPC file used during sort-on-close.
const IPC_STAGING_SUFFIX: &str = ".arrow_ipc_staging";

struct NativeParquetWriter;

impl NativeParquetWriter {

    fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        // log_info!("[RUST] create_writer called for file: {}, schema_address: {}", filename, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid schema address (null pointer) for file: {}, schema_address: {}", filename, schema_address);
            return Err("Invalid schema address".into());
        }

        if WRITER_MANAGER.contains_key(&filename) || IPC_WRITER_MANAGER.contains_key(&filename) {
            log_error!("[RUST] ERROR: Writer already exists for file: {}", filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);

        // log_info!("[RUST] Schema created with {} fields", schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            log_debug!("[RUST] Field {}: {} ({})", i, field.name(), field.data_type());
        }

        // If sort config exists for this file, use Arrow IPC staging path
        // to avoid redundant Parquet encode/decode before sorting.
        if SORT_CONFIG.contains_key(&filename) {
            let ipc_path = format!("{}{}", filename, IPC_STAGING_SUFFIX);
            let file = File::create(&ipc_path)?;
            let file_clone = file.try_clone()?;
            FILE_MANAGER.insert(filename.clone(), file_clone);
            let ipc_writer = IpcFileWriter::try_new(file, &schema)?;
            IPC_WRITER_MANAGER.insert(filename, Arc::new(Mutex::new(ipc_writer)));
        } else {
            let file = File::create(&filename)?;
            let file_clone = file.try_clone()?;
            FILE_MANAGER.insert(filename.clone(), file_clone);
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build();
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            WRITER_MANAGER.insert(filename, Arc::new(Mutex::new(writer)));
        }
        Ok(())
    }

    fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        // log_info!("[RUST] write_data called for file: {}, array_address: {}, schema_address: {}", filename, array_address, schema_address);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid FFI addresses for file: {}, array_address: {}, schema_address: {}", filename, array_address, schema_address);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);

            match arrow::ffi::from_ffi(arrow_array, &arrow_schema) {
                Ok(array_data) => {
                    log_debug!("[RUST] Successfully imported array_data, length: {}", array_data.len());

                    let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);
                    log_debug!("[RUST] Array type: {:?}, length: {}", array.data_type(), array.len());

                    if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                        log_debug!("[RUST] Successfully cast to StructArray with {} columns", struct_array.num_columns());

                        let schema = Arc::new(arrow::datatypes::Schema::new(
                            struct_array.fields().clone()
                        ));

                        let record_batch = RecordBatch::try_new(
                            schema.clone(),
                            struct_array.columns().to_vec(),
                        )?;

                        // Check IPC writer first (sort-on-close path), then Parquet writer
                        if let Some(writer_arc) = IPC_WRITER_MANAGER.get(&filename) {
                            log_debug!("[RUST] Writing RecordBatch to IPC staging file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            Ok(())
                        } else if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                            log_debug!("[RUST] Writing RecordBatch to Parquet file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            Ok(())
                        } else {
                            log_error!("[RUST] ERROR: No writer found for file: {}", filename);
                            Err("Writer not found".into())
                        }
                    } else {
                        log_error!("[RUST] ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                        Err("Expected struct array from VectorSchemaRoot".into())
                    }
                }
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to import from FFI: {:?}", e);
                    Err(e.into())
                }
            }
        }
    }

    fn close_writer(filename: String) -> Result<Option<FormatFileMetaData>, Box<dyn std::error::Error>> {
        // log_info!("[RUST] close_writer called for file: {}", filename);

        // Check if this is an IPC writer (sort-on-close path)
        if let Some((_, ipc_writer_arc)) = IPC_WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(ipc_writer_arc) {
                Ok(mutex) => {
                    let mut writer = mutex.into_inner().unwrap();
                    writer.finish()?;
                    // IPC writer is now closed; the staging file is complete.
                    // Return None — the actual Parquet metadata will be produced
                    // after sort-on-close in the JNI closeWriter entry point.
                    Ok(None)
                }
                Err(_) => {
                    log_error!("[RUST] ERROR: IPC Writer still in use for file: {}", filename);
                    Err("IPC Writer still in use".into())
                }
            }
        } else if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(file_metadata) => {
                            Ok(Some(file_metadata))
                        }
                        Err(e) => {
                            log_error!("[RUST] ERROR: Failed to close writer for file: {}", filename);
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    log_error!("[RUST] ERROR: Writer still in use for file: {}", filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: Writer not found for file: {}\n", filename);
            Err("Writer not found".into())
        }
    }

    /// Sort-on-close for the IPC staging path: reads Arrow IPC batches from the
    /// staging file, sorts them, writes the final Parquet file, and cleans up
    /// the staging file. Returns the Parquet file metadata.
    fn sort_ipc_staging_and_write_parquet(
        filename: &str,
        sort_column: &str,
        reverse_sort: bool,
    ) -> Result<(FormatFileMetaData, Vec<parquet_merge_stream::SortPermutationEntry>), Box<dyn std::error::Error>> {
        use arrow::compute::SortOptions;

        let ipc_path = format!("{}{}", filename, IPC_STAGING_SUFFIX);

        log_info!("[RUST] sort_ipc_staging: reading IPC staging file: {}", ipc_path);

        // Read all batches from the Arrow IPC staging file
        let ipc_file = File::open(&ipc_path)?;
        let ipc_reader = IpcFileReader::try_new(ipc_file, None)?;
        let schema = ipc_reader.schema();

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for batch_result in ipc_reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            // Write an empty Parquet file
            let out_file = File::create(filename)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build();
            let writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))?;
            let metadata = writer.close()?;
            // Clean up staging file
            let _ = std::fs::remove_file(&ipc_path);
            return Ok((metadata, Vec::new()));
        }

        // Concatenate all batches into one
        let combined = arrow::compute::concat_batches(&schema, all_batches.iter())?;

        // Find sort column index
        let sort_col_idx = schema.fields().iter().position(|f| f.name() == sort_column)
            .ok_or_else(|| format!(
                "Sort column '{}' not found in IPC staging file '{}'", sort_column, ipc_path
            ))?;

        // Sort using arrow::compute::sort_to_indices + take
        let sort_col = combined.column(sort_col_idx);
        let options = SortOptions {
            descending: reverse_sort,
            nulls_first: true,
        };
        let indices = arrow::compute::sort_to_indices(sort_col, Some(options), None)?;

        // Build the sort permutation
        let file_id = parquet_merge_stream::extract_writer_generation_pub(filename)
            .unwrap_or_else(|| {
                std::path::Path::new(filename)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(filename)
                    .to_string()
            });

        let num_rows = indices.len();
        let mut permutation: Vec<parquet_merge_stream::SortPermutationEntry> = Vec::with_capacity(num_rows);
        for new_row_id in 0..num_rows {
            let old_row_id = indices.value(new_row_id) as i64;
            permutation.push(parquet_merge_stream::SortPermutationEntry {
                old_file_id: file_id.clone(),
                old_row_id,
                new_row_id: new_row_id as i64,
            });
        }

        // Apply sort indices to all columns
        let sorted_columns: Vec<Arc<dyn arrow::array::Array>> = combined.columns().iter()
            .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;

        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;

        // Rewrite ___row_id column with sequential values
        let final_batch = parquet_merge_stream::rewrite_row_ids_pub(&sorted_batch, 0)?;

        // Write the final sorted Parquet file
        let out_file = File::create(filename)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();
        let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))?;
        writer.write(&final_batch)?;
        let metadata = writer.close()?;

        // Clean up the IPC staging file
        let _ = std::fs::remove_file(&ipc_path);

        log_info!("[RUST] sort_ipc_staging: sorted {} rows, wrote Parquet to {}", num_rows, filename);

        Ok((metadata, permutation))
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        // log_info!("[RUST] fsync_file called for file: {}", filename);

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            match file.sync_all() {
                Ok(_) => {
                    // log_info!("[RUST] Successfully fsynced file: {}", filename);
                    drop(file);
                    FILE_MANAGER.remove(&filename);
                    Ok(())
                }
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to fsync file: {}", filename);
                    Err(e.into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: File not found for fsync: {}", filename);
            Err("File not found".into())
        }
    }

    fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        log_debug!("[RUST] get_filtered_writer_memory_usage called with prefix: {}", path_prefix);

        let mut total_memory = 0;
        let mut writer_count = 0;

        for entry in WRITER_MANAGER.iter() {
            let filename = entry.key();
            let writer_arc = entry.value();

            // Filter writers by path prefix
            if filename.starts_with(&path_prefix) {
                if let Ok(writer) = writer_arc.lock() {
                    let memory_usage = writer.memory_size();
                    total_memory += memory_usage;
                    writer_count += 1;

                    log_debug!("[RUST] Filtered Writer {}: {} bytes", filename, memory_usage);
                }
            }
        }

        log_debug!("[RUST] Total memory usage across {} filtered ArrowWriters (prefix: {}): {} bytes", writer_count, path_prefix, total_memory);

        Ok(total_memory)
    }

    fn get_file_metadata(filename: String) -> Result<FileFileMetaData, Box<dyn std::error::Error>> {
        log_debug!("[RUST] get_file_metadata called for file: {}\n", filename);

        // Open the Parquet file
        let file = match File::open(&filename) {
            Ok(f) => f,
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to open file {}: {:?}", filename, e);
                return Err(format!("File not found: {}", filename).into());
            }
        };

        // Create SerializedFileReader
        let reader = match SerializedFileReader::new(file) {
            Ok(r) => r,
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to create Parquet reader for {}: {:?}", filename, e);
                return Err(format!("Invalid Parquet file format: {}", e).into());
            }
        };

        // Get metadata from the reader
        let parquet_metadata = reader.metadata();
        let file_metadata = parquet_metadata.file_metadata().clone();

        log_debug!("[RUST] Successfully read metadata from file: {}, version={}, num_rows={}\n",
                                  filename, file_metadata.version(), file_metadata.num_rows());

        Ok(file_metadata)
    }

    fn create_java_metadata<'local>(env: &mut JNIEnv<'local>, metadata: &FormatFileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
        // Find the ParquetFileMetadata class
        let class = env.find_class("com/parquet/parquetdataformat/bridge/ParquetFileMetadata")?;

        // Create Java String for created_by (handle None case)
        let created_by_jstring = match &metadata.created_by {
            Some(created_by) => env.new_string(created_by)?,
            None => JObject::null().into(),
        };

        // Create the Java object using new_object with signature
        let java_metadata = env.new_object(&class, "(IJLjava/lang/String;)V", &[
            (metadata.version).into(),
            (metadata.num_rows).into(),
            (&created_by_jstring).into(),
        ])?;

        Ok(java_metadata)
    }

    fn create_java_metadata_from_file<'local>(env: &mut JNIEnv<'local>, metadata: &FileFileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
        // Find the ParquetFileMetadata class
        let class = env.find_class("com/parquet/parquetdataformat/bridge/ParquetFileMetadata")?;

        // Create Java String for created_by (handle None case)
        let created_by_jstring = match metadata.created_by() {
            Some(created_by) => env.new_string(created_by)?,
            None => JObject::null().into(),
        };

        // Create the Java object using new_object with signature
        let java_metadata = env.new_object(&class, "(IJLjava/lang/String;)V", &[
            (metadata.version()).into(),
            (metadata.num_rows()).into(),
            (&created_by_jstring).into(),
        ])?;

        Ok(java_metadata)
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger using the shared crate
    vectorized_exec_spi::logger::init_logger_from_env(&env);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    schema_address: jlong,
    sort_column: JString,
    reverse_sort: jni::sys::jboolean,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();

    // Store sort config if a sort column is provided.
    // create_writer checks SORT_CONFIG to decide IPC vs Parquet path.
    if !sort_column.is_null() {
        let sort_col_str: String = env.get_string(&sort_column)
            .map(|s| s.into())
            .unwrap_or_default();
        if !sort_col_str.is_empty() {
            SORT_CONFIG.insert(filename.clone(), SortConfig {
                sort_column: sort_col_str,
                reverse_sort: reverse_sort != 0,
            });
        }
    }

    match NativeParquetWriter::create_writer(filename, schema_address as i64) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    array_address: jlong,
    schema_address: jlong
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::write_data(filename, array_address as i64, schema_address as i64) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_closeWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();

    // Check if this file has sort config — determines whether we used IPC staging
    let sort_config = SORT_CONFIG.remove(&filename).map(|(_, cfg)| cfg);

    match NativeParquetWriter::close_writer(filename.clone()) {
        Ok(maybe_metadata) => {
            if let Some(cfg) = sort_config {
                // IPC staging path: the IPC writer was closed (returning None metadata).
                // Now read the IPC staging file, sort, and write final Parquet.
                log_info!("[RUST] Sort-on-close (IPC path): {} by '{}' (reverse={})",
                    filename, cfg.sort_column, cfg.reverse_sort);
                match NativeParquetWriter::sort_ipc_staging_and_write_parquet(
                    &filename, &cfg.sort_column, cfg.reverse_sort
                ) {
                    Ok((metadata, permutation)) => {
                        log_info!("[RUST] Sort-on-close produced {} permutation entries for {}",
                            permutation.len(), filename);
                        SORT_PERMUTATION.insert(filename.clone(), permutation);

                        match NativeParquetWriter::create_java_metadata(&mut env, &metadata) {
                            Ok(java_obj) => java_obj.into_raw(),
                            Err(e) => {
                                log_error!("[RUST] ERROR: Failed to create Java metadata: {:?}", e);
                                let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                                JObject::null().into_raw()
                            }
                        }
                    }
                    Err(e) => {
                        log_error!("[RUST] Sort-on-close (IPC path) failed for {}: {}", filename, e);
                        let _ = env.throw_new("java/io/IOException",
                            &format!("Sort-on-close failed: {}", e));
                        JObject::null().into_raw()
                    }
                }
            } else {
                // Standard Parquet path — no sorting needed
                match maybe_metadata {
                    Some(metadata) => {
                        match NativeParquetWriter::create_java_metadata(&mut env, &metadata) {
                            Ok(java_obj) => java_obj.into_raw(),
                            Err(e) => {
                                let error_msg = format!("[RUST] ERROR: Failed to create Java metadata object: {:?}\n", e);
                                log_error!("{}", error_msg.trim());
                                let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                                JObject::null().into_raw()
                            }
                        }
                    }
                    None => {
                        JObject::null().into_raw()
                    }
                }
            }
        }
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to close writer: {:?}\n", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to close writer: {}", e));
            JObject::null().into_raw()
        }
    }
}

/// JNI entry point to retrieve the sort permutation cached during sort-on-close.
/// Returns a RowIdMapping object if a permutation was cached for this file, or null.
/// The permutation is removed from the cache after retrieval (single-use).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getSortPermutation<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    file: JString<'local>,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();

    match SORT_PERMUTATION.remove(&filename) {
        Some((_, permutation)) => {
            if permutation.is_empty() {
                return JObject::null().into_raw();
            }
            // Extract file_id from first entry (all entries share the same file_id for single-file sort)
            let file_id = permutation[0].old_file_id.clone();
            let size = permutation.len();

            // Build parallel arrays for RowIdMapping constructor
            let old_row_ids = match env.new_long_array(size as i32) {
                Ok(arr) => arr,
                Err(e) => {
                    log_error!("[RUST] Failed to create old_row_ids array: {}", e);
                    return JObject::null().into_raw();
                }
            };
            let new_row_ids = match env.new_long_array(size as i32) {
                Ok(arr) => arr,
                Err(e) => {
                    log_error!("[RUST] Failed to create new_row_ids array: {}", e);
                    return JObject::null().into_raw();
                }
            };
            let file_ids = match env.new_object_array(size as i32, "java/lang/String", JObject::null()) {
                Ok(arr) => arr,
                Err(e) => {
                    log_error!("[RUST] Failed to create file_ids array: {}", e);
                    return JObject::null().into_raw();
                }
            };

            let mut old_ids_vec = Vec::with_capacity(size);
            let mut new_ids_vec = Vec::with_capacity(size);

            for (i, entry) in permutation.into_iter().enumerate() {
                old_ids_vec.push(entry.old_row_id);
                new_ids_vec.push(entry.new_row_id);
                match env.new_string(&entry.old_file_id) {
                    Ok(s) => { let _ = env.set_object_array_element(&file_ids, i as i32, s); }
                    Err(e) => {
                        log_error!("[RUST] Failed to create file_id string: {}", e);
                        return JObject::null().into_raw();
                    }
                }
            }

            let _ = env.set_long_array_region(&old_row_ids, 0, &old_ids_vec);
            let _ = env.set_long_array_region(&new_row_ids, 0, &new_ids_vec);

            let file_id_jstr = match env.new_string(&file_id) {
                Ok(s) => s,
                Err(e) => {
                    log_error!("[RUST] Failed to create file_id string: {}", e);
                    return JObject::null().into_raw();
                }
            };

            match env.new_object(
                "org/opensearch/index/engine/exec/merge/RowIdMapping",
                "([J[J[Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    jni::objects::JValue::Object(&old_row_ids.into()),
                    jni::objects::JValue::Object(&new_row_ids.into()),
                    jni::objects::JValue::Object(&file_ids.into()),
                    jni::objects::JValue::Object(&file_id_jstr.into()),
                ],
            ) {
                Ok(obj) => obj.into_raw(),
                Err(e) => {
                    log_error!("[RUST] Failed to create RowIdMapping: {}", e);
                    JObject::null().into_raw()
                }
            }
        }
        None => JObject::null().into_raw(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_flushToDisk(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::flush_to_disk(filename) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_file_metadata(filename) {
        Ok(metadata) => {
            match NativeParquetWriter::create_java_metadata_from_file(&mut env, &metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    let error_msg = format!("[RUST] ERROR: Failed to create Java metadata object: {:?}\n", e);
                    println!("{}", error_msg.trim());
                    log_error!("{}", error_msg);
                    // Throw IOException to Java
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Err(e) => {
            let error_msg = format!("[RUST] ERROR: Failed to read file metadata: {:?}\n", e);
            println!("{}", error_msg.trim());
            log_error!("{}", error_msg);
            // Throw IOException to Java
            let _ = env.throw_new("java/io/IOException", &format!("Failed to read file metadata: {}", e));
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getFilteredNativeBytesUsed(
    mut env: JNIEnv,
    _class: JClass,
    path_prefix: JString
) -> jlong {
    let prefix: String = env.get_string(&path_prefix).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_filtered_writer_memory_usage(prefix) {
        Ok(memory) => memory as jlong,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StructArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::FFI_ArrowArray;
    use arrow::ffi::FFI_ArrowSchema;
    use arrow_array::Array;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_ffi_schema() -> (Arc<Schema>, i64) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        (schema, schema_ptr)
    }

    fn cleanup_ffi_schema(schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn create_test_ffi_data() -> Result<(i64, i64), Box<dyn std::error::Error>> {
        use arrow::array::{Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array],
        )?;

        // Convert to struct array (what Java VectorSchemaRoot exports)
        let struct_array = StructArray::from(record_batch);
        let array_data = struct_array.into_data();

        // Create FFI representations
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        Ok((array_ptr, schema_ptr))
    }

    fn cleanup_ffi_data(array_ptr: i64, schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn get_temp_file_path(name: &str) -> (tempfile::TempDir, String) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join(name);
        let filename = file_path.to_string_lossy().to_string();
        (temp_dir, filename)
    }

    fn create_writer_and_assert_success(filename: &str) -> (Arc<Schema>, i64) {
        let (schema, schema_ptr) = create_test_ffi_schema();
        let result = NativeParquetWriter::create_writer(filename.to_string(), schema_ptr);
        assert!(result.is_ok());
        (schema, schema_ptr)
    }

    fn close_writer_and_cleanup_schema(filename: &str, schema_ptr: i64) {
        let _ = NativeParquetWriter::close_writer(filename.to_string());
        cleanup_ffi_schema(schema_ptr);
    }

    fn write_ffi_data_to_writer(filename: &str) -> (i64, i64) {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        let result = NativeParquetWriter::write_data(filename.to_string(), array_ptr, data_schema_ptr);
        assert!(result.is_ok());
        (array_ptr, data_schema_ptr)
    }

    #[test]
    fn test_create_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        assert!(WRITER_MANAGER.contains_key(&filename));
        assert!(FILE_MANAGER.contains_key(&filename));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_path() {
        let invalid_path = "/invalid/path/that/does/not/exist/test.parquet";
        let (_schema, schema_ptr) = create_test_ffi_schema();

        let result = NativeParquetWriter::create_writer(invalid_path.to_string(), schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No such file or directory"));

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_schema_pointer() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");

        // Test with null schema pointer
        let result = NativeParquetWriter::create_writer(filename, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
    }

    #[test]
    fn test_create_writer_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Second writer creation for same file should fail
        let result2 = NativeParquetWriter::create_writer(filename.clone(), schema_ptr);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer already exists"));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_success() {
        let (_temp_dir, filename) = get_temp_file_path("write_ffi_test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data using complete FFI flow
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);

        // Cleanup FFI data
        cleanup_ffi_data(array_ptr, data_schema_ptr);
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_write_data_no_writer() {
        let (array_ptr, schema_ptr) = create_test_ffi_data().unwrap();

        let result = NativeParquetWriter::write_data("nonexistent.parquet".to_string(), array_ptr, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));

        cleanup_ffi_data(array_ptr, schema_ptr);
    }

    #[test]
    fn test_write_data_multiple_batches() {
        let (_temp_dir, filename) = get_temp_file_path("multi_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write multiple batches using FFI
        for _ in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_invalid_pointers() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Test with schema and array pointers both null
        let result = NativeParquetWriter::write_data(filename.clone(), 0, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        // Test with one null pointer
        let result = NativeParquetWriter::write_data(filename.clone(), 0, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_close_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_close.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result = NativeParquetWriter::close_writer(filename.clone());

        assert!(result.is_ok());
        assert!(!WRITER_MANAGER.contains_key(&filename));

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_close_nonexistent_writer() {
        let result = NativeParquetWriter::close_writer("nonexistent.parquet".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));
    }

    #[test]
    fn test_close_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result1 = NativeParquetWriter::close_writer(filename.clone());
        assert!(result1.is_ok());
        let result2 = NativeParquetWriter::close_writer(filename);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer not found"));
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_flush_to_disk_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_flush.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result = NativeParquetWriter::flush_to_disk(filename.clone());
        assert!(result.is_ok());

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_flush_nonexistent_file() {
        let result = NativeParquetWriter::flush_to_disk("nonexistent.parquet".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "File not found");
    }

    #[test]
    fn test_get_filtered_writer_memory_usage_with_writers() {
        let (_temp_dir, filename1) = get_temp_file_path("test1.parquet");
        let (_temp_dir, filename2) = get_temp_file_path("test2.parquet");
        let prefix = _temp_dir.path().to_string_lossy().to_string();
        let (_schema1, schema_ptr1) = create_writer_and_assert_success(&filename1);
        let (_schema2, schema_ptr2) = create_writer_and_assert_success(&filename2);

        let result = NativeParquetWriter::get_filtered_writer_memory_usage(prefix);
        assert!(result.is_ok());
        let _memory_usage = result.unwrap();
        assert!(_memory_usage >= 0);

        close_writer_and_cleanup_schema(&filename1, schema_ptr1);
        close_writer_and_cleanup_schema(&filename2, schema_ptr2);
    }

    #[test]
    fn test_complete_writer_lifecycle() {
        let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
        let file_path = std::path::Path::new(&filename);

        // Step 1: Create schema and writer
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Step 2: Write multiple batches
        for _i in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        // Step 3: Close writer
        assert!(NativeParquetWriter::close_writer(filename.clone()).is_ok());

        // Step 4: Flush to disk
        assert!(NativeParquetWriter::flush_to_disk(filename.clone()).is_ok());

        // Step 5: Verify file exists and has content
        assert!(file_path.exists());
        assert!(file_path.metadata().unwrap().len() > 0);

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_concurrent_writer_creation() {
        let temp_dir = tempdir().unwrap();
        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for i in 0..10 {
            let temp_dir_path = temp_dir.path().to_path_buf();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                let file_path = temp_dir_path.join(format!("concurrent_{}.parquet", i));
                let filename = file_path.to_string_lossy().to_string();
                let (_schema, schema_ptr) = create_test_ffi_schema();

                if NativeParquetWriter::create_writer(filename.clone(), schema_ptr).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    let _ = NativeParquetWriter::close_writer(filename);
                }
                cleanup_ffi_schema(schema_ptr);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(success_count.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_concurrent_close_operations_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("close_race.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads trying to close the same writer
        for _ in 0..3 {
            let filename = filename.clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                if NativeParquetWriter::close_writer(filename).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only one thread should succeed in closing
        assert_eq!(success_count.load(Ordering::SeqCst), 1);
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_concurrent_writes_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("concurrent_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads writing to same file using FFI
        for _ in 0..5 {
            let filename = filename.clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                if NativeParquetWriter::write_data(filename, array_ptr, data_schema_ptr).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                cleanup_ffi_data(array_ptr, data_schema_ptr);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(success_count.load(Ordering::SeqCst), 5);

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_concurrent_writes_different_files() {
        let temp_dir = tempdir().unwrap();
        let file_count = 8;
        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        let mut filenames = vec![];
        let mut schema_ptrs = vec![];

        // Create writers for all files first
        for i in 0..file_count {
            let file_path = temp_dir.path().join(format!("concurrent_write_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
            filenames.push(filename);
            schema_ptrs.push(schema_ptr);
        }

        // Concurrent write operations to different files
        for i in 0..file_count {
            let filename = filenames[i].clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                // Write multiple batches to this file
                for _ in 0..2 {
                    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                    if NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).is_ok() {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    cleanup_ffi_data(array_ptr, data_schema_ptr);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All write operations should succeed (file_count * 2 batches per file)
        assert_eq!(success_count.load(Ordering::SeqCst), file_count * 2);

        // Cleanup
        for (i, filename) in filenames.iter().enumerate() {
            close_writer_and_cleanup_schema(filename, schema_ptrs[i]);
        }
    }
}
