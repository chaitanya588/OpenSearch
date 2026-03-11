/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact representation of row ID mappings using parallel arrays.
 * This reduces memory footprint from ~80-100 bytes per row (HashMap-based)
 * to ~16 bytes per row (array-based), achieving 5-6x memory reduction.
 */
public final class RowIdMapping {

    private final long[] oldRowIds;
    private final long[] newRowIds;
    private final String[] fileIds;
    private final String fileId;

    /**
     * Creates a RowIdMapping from parallel arrays.
     * Arrays must be sorted by oldRowIds for binary search.
     *
     * @param oldRowIds sorted array of old row IDs
     * @param newRowIds corresponding array of new row IDs
     * @param fileIds corresponding array of file IDs for each mapping
     * @param fileId the output file ID
     */
    public RowIdMapping(long[] oldRowIds, long[] newRowIds, String[] fileIds, String fileId) {
        Objects.requireNonNull(oldRowIds, "oldRowIds cannot be null");
        Objects.requireNonNull(newRowIds, "newRowIds cannot be null");
        Objects.requireNonNull(fileIds, "fileIds cannot be null");
        Objects.requireNonNull(fileId, "fileId cannot be null");
        
        if (oldRowIds.length != newRowIds.length || oldRowIds.length != fileIds.length) {
            throw new IllegalArgumentException("Array lengths must match");
        }
        
        this.oldRowIds = oldRowIds;
        this.newRowIds = newRowIds;
        this.fileIds = fileIds;
        this.fileId = fileId;
    }

    /**
     * Legacy constructor for backward compatibility.
     * Converts HashMap to compact array representation.
     *
     * @param mapping map of old RowId to new row ID
     * @param fileId the output file ID
     * @deprecated Use array-based constructor for better memory efficiency
     */
    @Deprecated
    public RowIdMapping(Map<RowId, Long> mapping, String fileId) {
        Objects.requireNonNull(mapping, "mapping cannot be null");
        Objects.requireNonNull(fileId, "fileId cannot be null");
        
        int size = mapping.size();
        this.oldRowIds = new long[size];
        this.newRowIds = new long[size];
        this.fileIds = new String[size];
        this.fileId = fileId;
        
        int i = 0;
        for (Map.Entry<RowId, Long> entry : mapping.entrySet()) {
            this.oldRowIds[i] = entry.getKey().getRowId();
            this.newRowIds[i] = entry.getValue();
            this.fileIds[i] = entry.getKey().getFileId();
            i++;
        }
        
        // Sort by oldRowIds for binary search
        sortArrays();
    }

    /**
     * Returns the mapping as a Map for backward compatibility.
     * Note: This creates objects on-demand and should be avoided for performance.
     *
     * @return unmodifiable map view of the mapping
     * @deprecated Use getNewRowId() for lookups instead
     */
    @Deprecated
    public Map<RowId, Long> getMapping() {
        Map<RowId, Long> map = new HashMap<>(oldRowIds.length);
        for (int i = 0; i < oldRowIds.length; i++) {
            map.put(new RowId(oldRowIds[i], fileIds[i]), newRowIds[i]);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Looks up the new row ID for a given old RowId.
     * Uses binary search for O(log n) lookup time.
     *
     * @param oldRowId the old row ID to look up
     * @return the new row ID, or -1 if not found
     */
    public long getNewRowId(RowId oldRowId) {
        if (oldRowId == null) {
            return -1L;
        }
        return getNewRowId(oldRowId.getRowId(), oldRowId.getFileId());
    }

    /**
     * Looks up the new row ID for a given old row ID and file ID.
     * Uses binary search for O(log n) lookup time.
     *
     * @param oldRowId the old row ID
     * @param oldFileId the old file ID
     * @return the new row ID, or -1 if not found
     */
    public long getNewRowId(long oldRowId, String oldFileId) {
        // Binary search for the oldRowId
        int left = 0;
        int right = oldRowIds.length - 1;
        
        while (left <= right) {
            int mid = (left + right) >>> 1;
            long midVal = oldRowIds[mid];
            
            if (midVal < oldRowId) {
                left = mid + 1;
            } else if (midVal > oldRowId) {
                right = mid - 1;
            } else {
                // Found matching oldRowId, now check fileId
                // There might be multiple entries with same oldRowId but different fileIds
                // Search around this position
                int idx = findMatchingEntry(mid, oldRowId, oldFileId);
                return idx >= 0 ? newRowIds[idx] : -1L;
            }
        }
        
        return -1L;
    }

    /**
     * Finds the entry matching both oldRowId and oldFileId.
     * Searches around the given position since there might be multiple entries
     * with the same oldRowId but different fileIds.
     */
    private int findMatchingEntry(int startIdx, long oldRowId, String oldFileId) {
        // Check the found position first
        if (oldRowIds[startIdx] == oldRowId && fileIds[startIdx].equals(oldFileId)) {
            return startIdx;
        }
        
        // Search backwards
        for (int i = startIdx - 1; i >= 0 && oldRowIds[i] == oldRowId; i--) {
            if (fileIds[i].equals(oldFileId)) {
                return i;
            }
        }
        
        // Search forwards
        for (int i = startIdx + 1; i < oldRowIds.length && oldRowIds[i] == oldRowId; i++) {
            if (fileIds[i].equals(oldFileId)) {
                return i;
            }
        }
        
        return -1;
    }

    public String getFileId() {
        return fileId;
    }

    /**
     * Returns the number of mappings.
     */
    public int size() {
        return oldRowIds.length;
    }

    /**
     * Gets the old row ID at the specified index.
     * For efficient iteration without object allocation.
     *
     * @param index the index in the mapping array
     * @return the old row ID at that index
     */
    public long getOldRowId(int index) {
        return oldRowIds[index];
    }

    /**
     * Gets the new row ID at the specified index.
     * For efficient iteration without object allocation.
     *
     * @param index the index in the mapping array
     * @return the new row ID at that index
     */
    public long getNewRowIdAt(int index) {
        return newRowIds[index];
    }

    /**
     * Gets the file ID at the specified index.
     * For efficient iteration without object allocation.
     *
     * @param index the index in the mapping array
     * @return the file ID at that index
     */
    public String getFileIdAt(int index) {
        return fileIds[index];
    }

    /**
     * Sorts the parallel arrays by oldRowIds for efficient binary search.
     */
    private void sortArrays() {
        // Create index array for sorting
        Integer[] indices = new Integer[oldRowIds.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        
        // Sort indices based on oldRowIds
        Arrays.sort(indices, (a, b) -> {
            int cmp = Long.compare(oldRowIds[a], oldRowIds[b]);
            if (cmp != 0) return cmp;
            return fileIds[a].compareTo(fileIds[b]);
        });
        
        // Reorder arrays based on sorted indices
        long[] sortedOldRowIds = new long[oldRowIds.length];
        long[] sortedNewRowIds = new long[newRowIds.length];
        String[] sortedFileIds = new String[fileIds.length];
        
        for (int i = 0; i < indices.length; i++) {
            sortedOldRowIds[i] = oldRowIds[indices[i]];
            sortedNewRowIds[i] = newRowIds[indices[i]];
            sortedFileIds[i] = fileIds[indices[i]];
        }
        
        System.arraycopy(sortedOldRowIds, 0, oldRowIds, 0, oldRowIds.length);
        System.arraycopy(sortedNewRowIds, 0, newRowIds, 0, newRowIds.length);
        System.arraycopy(sortedFileIds, 0, fileIds, 0, fileIds.length);
    }

    @Override
    public String toString() {
        return "RowIdMapping{" +
            "size=" + oldRowIds.length +
            ", fileId='" + fileId + '\'' +
            '}';
    }
}
