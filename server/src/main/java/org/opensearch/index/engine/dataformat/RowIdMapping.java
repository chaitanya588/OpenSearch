/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Mapping interface for translating row IDs after a merge or sort operation.
 * Supports forward (old→new) lookup always, and optionally reverse (new→old) lookup
 * when constructed with reverse support enabled.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RowIdMapping {

    /**
     * Returns the new row ID corresponding to the given old row ID.
     *
     * @param oldId the original row ID
     * @return the new row ID, or -1 if not found
     */
    long getNewRowId(long oldId);

    /**
     * Returns the old row ID corresponding to the given new row ID.
     *
     * @param newId the new row ID
     * @return the old row ID
     * @throws UnsupportedOperationException if reverse mapping is not supported
     */
    long newToOld(long newId);

    /**
     * Returns whether reverse (new→old) lookup is supported.
     *
     * @return true if {@link #newToOld(long)} is available
     */
    boolean isNewToOldSupported();

    /**
     * Returns the total number of documents in this mapping.
     *
     * @return the total document count
     */
    int size();
}
