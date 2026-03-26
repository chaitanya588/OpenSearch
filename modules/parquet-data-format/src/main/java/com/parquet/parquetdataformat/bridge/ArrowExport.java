/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Container for Arrow C Data Interface exports.
 * Provides a safe wrapper around ArrowArray and ArrowSchema with proper resource management.
 * Optionally owns a dedicated export allocator that is closed when this export is closed.
 */
public record ArrowExport(ArrowArray arrowArray, ArrowSchema arrowSchema, BufferAllocator exportAllocator) implements AutoCloseable {

    /** Constructor without a dedicated export allocator (legacy/schema-only usage). */
    public ArrowExport(ArrowArray arrowArray, ArrowSchema arrowSchema) {
        this(arrowArray, arrowSchema, null);
    }

    public long getArrayAddress() {
        return arrowArray.memoryAddress();
    }

    public long getSchemaAddress() {
        return arrowSchema.memoryAddress();
    }

    @Override
    public void close() {
        // Close the C Data Interface structs (frees struct memory).
        // Do NOT call release() — Rust's FFI_ArrowArray::from_raw() takes ownership
        // and invokes the release callback when it drops the imported data.
        if (arrowArray != null) {
            arrowArray.close();
        }
        if (arrowSchema != null) {
            arrowSchema.close();
        }
        // Close the dedicated export allocator to free all export-related
        // bookkeeping allocations that exportVectorSchemaRoot created.
        if (exportAllocator != null) {
            exportAllocator.close();
        }
    }
}
