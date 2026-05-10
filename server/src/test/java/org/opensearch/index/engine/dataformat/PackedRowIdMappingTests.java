/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link PackedRowIdMapping} in the server module.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    public void testForwardLookup() {
        long[] mappingArray = { 4, 3, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(4L, mapping.getNewRowId(0));
        assertEquals(3L, mapping.getNewRowId(1));
        assertEquals(2L, mapping.getNewRowId(2));
    }

    public void testOutOfBoundsReturnsNegativeOne() {
        long[] mappingArray = { 5, 6 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(-1L, mapping.getNewRowId(2));
        assertEquals(-1L, mapping.getNewRowId(-1));
    }

    public void testSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(5, mapping.size());
    }

    public void testIsNewToOldSupportedFalse() {
        long[] mappingArray = { 2, 0, 1 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertFalse(mapping.isNewToOldSupported());
    }

    public void testIsNewToOldSupportedTrue() {
        long[] mappingArray = { 2, 0, 1 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);
        assertTrue(mapping.isNewToOldSupported());
    }

    public void testNewToOldReturnsCorrectMapping() {
        // oldToNew = [2, 0, 1] means old0→new2, old1→new0, old2→new1
        // reverse: new0→old1, new1→old2, new2→old0
        long[] mappingArray = { 2, 0, 1 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);
        assertEquals(1L, mapping.newToOld(0));
        assertEquals(2L, mapping.newToOld(1));
        assertEquals(0L, mapping.newToOld(2));
    }

    public void testNewToOldThrowsWhenNotSupported() {
        long[] mappingArray = { 2, 0, 1 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        expectThrows(UnsupportedOperationException.class, () -> mapping.newToOld(0));
    }

    public void testNewToOldOutOfBoundsReturnsInput() {
        long[] mappingArray = { 1, 0 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);
        assertEquals(5L, mapping.newToOld(5));
        assertEquals(-1L, mapping.newToOld(-1));
    }

    public void testOffsetConstructor() {
        // sourceArray has extra elements; use offset=1, length=3
        long[] sourceArray = { 99, 2, 0, 1, 99 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(sourceArray, 1, 3, false);
        assertEquals(3, mapping.size());
        assertEquals(2L, mapping.getNewRowId(0));
        assertEquals(0L, mapping.getNewRowId(1));
        assertEquals(1L, mapping.getNewRowId(2));
    }

    public void testOffsetConstructorWithReverseSupport() {
        long[] sourceArray = { 99, 2, 0, 1, 99 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(sourceArray, 1, 3, true);
        assertTrue(mapping.isNewToOldSupported());
        // oldToNew from offset: [2, 0, 1] → reverse: new0→old1, new1→old2, new2→old0
        assertEquals(1L, mapping.newToOld(0));
        assertEquals(2L, mapping.newToOld(1));
        assertEquals(0L, mapping.newToOld(2));
    }

    public void testRamBytesUsedForwardOnly() {
        long[] mappingArray = new long[100];
        for (int i = 0; i < 100; i++) {
            mappingArray[i] = i;
        }
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertTrue(mapping.ramBytesUsed() > 0);
    }

    public void testRamBytesUsedWithReverse() {
        long[] mappingArray = new long[100];
        for (int i = 0; i < 100; i++) {
            mappingArray[i] = i;
        }
        PackedRowIdMapping forwardOnly = new PackedRowIdMapping(mappingArray, false);
        PackedRowIdMapping bidirectional = new PackedRowIdMapping(mappingArray, true);
        assertTrue(bidirectional.ramBytesUsed() > forwardOnly.ramBytesUsed());
    }

    public void testToStringContainsInfo() {
        long[] mappingArray = { 0, 1, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, true);
        String str = mapping.toString();
        assertTrue(str.contains("size=3"));
        assertTrue(str.contains("reverseSupported=true"));
        assertTrue(str.contains("estimatedMemoryBytes="));
    }

    public void testNullArrayThrows() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, false));
    }

    public void testEmptyMapping() {
        long[] mappingArray = {};
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(0, mapping.size());
        assertEquals(-1L, mapping.getNewRowId(0));
    }
}
