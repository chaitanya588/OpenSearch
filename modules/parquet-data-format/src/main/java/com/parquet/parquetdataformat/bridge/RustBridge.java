package com.parquet.parquetdataformat.bridge;

import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * JNI bridge to the native Rust Parquet writer implementation.
 *
 * <p>This class provides the interface between Java and the native Rust library
 * that handles low-level Parquet file operations. It automatically loads the
 * appropriate native library for the current platform and architecture.
 *
 * <p>The native library is extracted from resources and loaded as a temporary file,
 * which is automatically cleaned up on JVM shutdown.
 *
 * <p>All native methods operate on Arrow C Data Interface pointers and return
 * integer status codes for error handling.
 */
public class RustBridge {

    static {
        NativeLibraryLoader.load("parquet_dataformat_jni");

        initLogger();
    }

    // Logger initialization method
    public static native void initLogger();

    // Enhanced native methods that handle validation and provide better error reporting
    public static native void createWriter(String file, long schemaAddress, String sortColumn, boolean reverseSort) throws IOException;
    public static native void write(String file, long arrayAddress, long schemaAddress) throws IOException;
    public static native ParquetFileMetadata closeWriter(String file) throws IOException;
    public static native void flushToDisk(String file) throws IOException;
    public static native ParquetFileMetadata getFileMetadata(String file) throws IOException;

    public static native long getFilteredNativeBytesUsed(String pathPrefix);


    // Native method declarations - these will be implemented in the JNI library
    public static native RowIdMapping mergeParquetFilesInRust(List<Path> inputFiles, String outputFile, String sortKey, boolean isReverse);

    /**
     * Streaming sorted k-way merge of Parquet files. Returns a RowIdMapping
     * that maps old (file, rowId) pairs to new sequential row IDs in the
     * sorted output. Used when a sort column is configured.
     */
    public static native RowIdMapping mergeParquetFilesSorted(List<Path> inputFiles, String outputFile, String sortKey, boolean isReverse);

    /**
     * Sort a single Parquet file in-place by the given sort column.
     * Used during the sort-on-close path for individual segment files.
     */
    public static native void sortParquetFile(String filePath, String sortColumn, boolean isReverse) throws IOException;
}
