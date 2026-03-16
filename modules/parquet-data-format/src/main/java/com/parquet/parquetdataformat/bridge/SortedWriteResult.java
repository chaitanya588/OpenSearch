/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import org.opensearch.index.engine.exec.merge.RowIdMapping;

/**
 * Result of a sorted write operation containing both file metadata
 * and the sort permutation as a RowIdMapping.
 * Created by native Rust code via JNI in closeWriterSorted.
 */
public class SortedWriteResult {

    private final ParquetFileMetadata metadata;
    private final RowIdMapping rowIdMapping;

    public SortedWriteResult(ParquetFileMetadata metadata, RowIdMapping rowIdMapping) {
        this.metadata = metadata;
        this.rowIdMapping = rowIdMapping;
    }

    public ParquetFileMetadata getMetadata() {
        return metadata;
    }

    public RowIdMapping getRowIdMapping() {
        return rowIdMapping;
    }
}
