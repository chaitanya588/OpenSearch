/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Memory-efficient implementation of {@link RowIdMapping} using Lucene's {@link PackedLongValues}.
 *
 * <p>Stores a forward mapping (oldRowId → newRowId) always, and optionally a reverse mapping
 * (newRowId → oldRowId) when constructed with {@code reverseSupported = true}.
 *
 * <p>For merge flows (forward-only), construct with {@code reverseSupported = false}.
 * For flush/sort flows (bidirectional), construct with {@code reverseSupported = true}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PackedRowIdMapping implements RowIdMapping {

    private final PackedLongValues oldToNew;
    private final PackedLongValues newToOld;
    private final int size;
    private final boolean reverseSupported;

    /**
     * Creates a PackedRowIdMapping from a permutation array with explicit reverse support control.
     *
     * @param oldToNewArray array where {@code oldToNewArray[oldRowId] = newRowId}
     * @param reverseSupported if true, builds the reverse mapping for {@link #newToOld(long)}
     */
    public PackedRowIdMapping(long[] oldToNewArray, boolean reverseSupported) {
        this(oldToNewArray, 0, oldToNewArray.length, reverseSupported);
    }

    /**
     * Creates a PackedRowIdMapping from a slice of a source array without copying.
     * Reads directly from {@code sourceArray[offset]} to {@code sourceArray[offset + length - 1]}.
     *
     * @param sourceArray the source array containing the mapping values
     * @param offset the starting index in the source array
     * @param length the number of elements to use
     * @param reverseSupported if true, builds the reverse mapping for {@link #newToOld(long)}
     */
    public PackedRowIdMapping(long[] sourceArray, int offset, int length, boolean reverseSupported) {
        Objects.requireNonNull(sourceArray, "sourceArray cannot be null");
        this.size = length;
        this.reverseSupported = reverseSupported;

        PackedLongValues.Builder forwardBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (int i = offset; i < offset + length; i++) {
            forwardBuilder.add(sourceArray[i]);
        }
        this.oldToNew = forwardBuilder.build();

        if (reverseSupported) {
            long[] newToOldArray = new long[length];
            for (int i = 0; i < length; i++) {
                int newPos = (int) sourceArray[offset + i];
                if (newPos >= 0 && newPos < length) {
                    newToOldArray[newPos] = i;
                }
            }
            PackedLongValues.Builder reverseBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (long value : newToOldArray) {
                reverseBuilder.add(value);
            }
            this.newToOld = reverseBuilder.build();
        } else {
            this.newToOld = null;
        }
    }

    @Override
    public long getNewRowId(long oldId) {
        int idx = (int) oldId;
        if (idx < 0 || idx >= size) {
            return -1L;
        }
        return oldToNew.get(idx);
    }

    @Override
    public long newToOld(long newId) {
        if (newToOld == null) {
            throw new UnsupportedOperationException("Reverse mapping (newToOld) is not supported for this instance");
        }
        int idx = (int) newId;
        if (idx < 0 || idx >= size) {
            return newId;
        }
        return newToOld.get(idx);
    }

    @Override
    public boolean isNewToOldSupported() {
        return reverseSupported;
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Returns the estimated memory usage of this mapping in bytes.
     *
     * @return estimated memory in bytes
     */
    public long ramBytesUsed() {
        long bytes = oldToNew.ramBytesUsed();
        if (newToOld != null) {
            bytes += newToOld.ramBytesUsed();
        }
        return bytes;
    }

    @Override
    public String toString() {
        return "PackedRowIdMapping{"
            + "size="
            + size
            + ", reverseSupported="
            + reverseSupported
            + ", estimatedMemoryBytes="
            + ramBytesUsed()
            + '}';
    }
}
