/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IndexingExecutionEngine<T extends DataFormat> extends Closeable {

    List<String> supportedFieldTypes();

    Writer<? extends DocumentInput<?>> createWriter(long writerGeneration)
        throws IOException; // A writer responsible for data format vended by this engine.

    Merger getMerger(); // Merger responsible for merging for specific data format

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();

    void loadWriterFiles(CatalogSnapshot catalogSnapshot)
        throws IOException; // Bootstrap hook to make engine aware of previously written files from CatalogSnapshot

    default long getNativeBytesUsed() {
        return 0;
    }

    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;

    /**
     * Sets the sort column for index sorting support.
     * @param sortColumn the column name to sort by, or null if no sorting
     */
    default void setSortColumn(String sortColumn) {
        // no-op by default
    }

    /**
     * Sets whether sorting should be in reverse order.
     * @param reverseSort true for descending sort order
     */
    default void setReverseSort(boolean reverseSort) {
        // no-op by default
    }
}
