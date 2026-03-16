/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.index.engine.exec.merge.RowIdMapping;

/**
 * Type-safe handle for native Parquet writer with lifecycle management.
 */
public class NativeParquetWriter implements Closeable {

    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private final String filePath;

    /**
     * Creates a new native Parquet writer.
     * @param filePath path to the Parquet file
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @param sortColumn column to sort by, or null if no sorting
     * @param reverseSort whether to sort in reverse order
     * @throws IOException if writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress, String sortColumn, boolean reverseSort) throws IOException {
        this.filePath = filePath;
        RustBridge.createWriter(filePath, schemaAddress, sortColumn, reverseSort);
    }

    /**
     * Writes a batch to the Parquet file.
     * @param arrayAddress Arrow C Data Interface array pointer
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @throws IOException if write fails
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        RustBridge.write(filePath, arrayAddress, schemaAddress);
    }

    /**
     * Flushes buffered data to disk.
     * @throws IOException if flush fails
     */
    public void flush() throws IOException {
        RustBridge.flushToDisk(filePath);
    }

    private ParquetFileMetadata metadata;
    private RowIdMapping sortPermutation;

    @Override
    public void close() {
        if (writerClosed.compareAndSet(false, true)) {
            try {
                metadata = RustBridge.closeWriter(filePath);
                // Retrieve the sort permutation cached during sort-on-close (if any).
                // This is a single-use retrieval — the native side removes it from cache.
                sortPermutation = RustBridge.getSortPermutation(filePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to close Parquet writer for " + filePath, e);
            }
        }
    }

    public ParquetFileMetadata getMetadata() {
        return metadata;
    }

    /**
     * Returns the sort permutation produced during sort-on-close, or null if
     * no sorting was configured or the file was empty.
     */
    public RowIdMapping getSortPermutation() {
        return sortPermutation;
    }

    public String getFilePath() {
        return filePath;
    }
}
