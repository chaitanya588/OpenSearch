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
 * Simple array-backed implementation of {@link RowIdMapping} for single-generation
 * flush sort permutations. Stores oldToNew and newToOld as int arrays.
 *
 * <p>Constructed from the raw [oldRowIds, newRowIds] arrays produced by the
 * primary data format's sort-on-close operation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ArrayRowIdMapping implements RowIdMapping {

    private final int[] oldToNew;
    private final int[] newToOld;
    private final int size;

    /**
     * Creates an ArrayRowIdMapping from parallel old/new row ID arrays.
     *
     * @param oldRowIds array of original row positions
     * @param newRowIds array of new row positions (same length as oldRowIds)
     * @param numDocs total number of documents
     */
    public ArrayRowIdMapping(long[] oldRowIds, long[] newRowIds, int numDocs) {
        this.size = numDocs;
        this.oldToNew = new int[numDocs];
        this.newToOld = new int[numDocs];
        // Initialize with identity mapping
        for (int i = 0; i < numDocs; i++) {
            oldToNew[i] = i;
            newToOld[i] = i;
        }
        for (int i = 0; i < oldRowIds.length; i++) {
            int oldDoc = (int) oldRowIds[i];
            int newDoc = (int) newRowIds[i];
            if (oldDoc >= 0 && oldDoc < numDocs && newDoc >= 0 && newDoc < numDocs) {
                oldToNew[oldDoc] = newDoc;
                newToOld[newDoc] = oldDoc;
            }
        }
    }

    /**
     * Creates an ArrayRowIdMapping from the raw [oldRowIds, newRowIds] format
     * produced by the Parquet sort-on-close operation.
     *
     * @param sortPermutation [0] = old_row_ids, [1] = new_row_ids
     * @return a new ArrayRowIdMapping, or null if input is null/empty
     */
    public static ArrayRowIdMapping fromRawPermutation(long[][] sortPermutation) {
        if (sortPermutation == null || sortPermutation.length != 2 || sortPermutation[0].length == 0) {
            return null;
        }
        int numDocs = sortPermutation[0].length;
        return new ArrayRowIdMapping(sortPermutation[0], sortPermutation[1], numDocs);
    }

    @Override
    public long getNewRowId(long oldId, long oldGeneration) {
        int idx = (int) oldId;
        if (idx < 0 || idx >= size) {
            return -1L;
        }
        return oldToNew[idx];
    }

    @Override
    public int oldToNew(int oldDocId) {
        if (oldDocId < 0 || oldDocId >= size) {
            return oldDocId;
        }
        return oldToNew[oldDocId];
    }

    @Override
    public int newToOld(int newDocId) {
        if (newDocId < 0 || newDocId >= size) {
            return newDocId;
        }
        return newToOld[newDocId];
    }

    @Override
    public int size() {
        return size;
    }
}
