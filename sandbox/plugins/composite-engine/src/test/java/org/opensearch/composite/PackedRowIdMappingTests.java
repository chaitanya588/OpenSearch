/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link PackedRowIdMapping}.
 */
public class PackedRowIdMappingTests extends OpenSearchTestCase {

    /**
     * Basic lookup: single generation mapping.
     * 0→4, 1→3, 2→2
     */
    public void testBasicLookup() {
        long[] mappingArray = { 4, 3, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);

        assertEquals(4L, mapping.getNewRowId(0));
        assertEquals(3L, mapping.getNewRowId(1));
        assertEquals(2L, mapping.getNewRowId(2));
    }

    /**
     * Implements the RowIdMapping interface correctly.
     */
    public void testImplementsInterface() {
        long[] mappingArray = { 10, 20 };
        RowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(10L, mapping.getNewRowId(0));
        assertEquals(20L, mapping.getNewRowId(1));
    }

    /**
     * Out-of-bounds row ID returns -1.
     */
    public void testOutOfBoundsRowIdReturnsNegativeOne() {
        long[] mappingArray = { 5, 6 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(-1L, mapping.getNewRowId(2));
        assertEquals(-1L, mapping.getNewRowId(-1));
    }

    /**
     * Size returns total number of entries.
     */
    public void testSize() {
        long[] mappingArray = { 0, 1, 2, 3, 4 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(5, mapping.size());
    }

    /**
     * Memory usage is reported and positive.
     */
    public void testRamBytesUsed() {
        long[] mappingArray = new long[1000];
        for (int i = 0; i < 1000; i++) {
            mappingArray[i] = i;
        }
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertTrue("RAM bytes used should be positive", mapping.ramBytesUsed() > 0);
    }

    /**
     * Empty mapping works correctly.
     */
    public void testEmptyMapping() {
        long[] mappingArray = {};
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        assertEquals(0, mapping.size());
        assertEquals(-1L, mapping.getNewRowId(0));
    }

    /**
     * Null argument throws NullPointerException.
     */
    public void testNullArgumentThrows() {
        expectThrows(NullPointerException.class, () -> new PackedRowIdMapping(null, false));
    }

    /**
     * Multi-generation usage via Map keyed by generation.
     * Simulates the merge flow where each generation gets its own mapping.
     */
    public void testMultiGenerationViaMap() {
        // gen=1 (3 rows): 0→4, 1→3, 2→2
        // gen=2 (2 rows): 0→1, 1→0
        Map<Long, RowIdMapping> mappings = new HashMap<>();
        mappings.put(1L, new PackedRowIdMapping(new long[] { 4, 3, 2 }, false));
        mappings.put(2L, new PackedRowIdMapping(new long[] { 1, 0 }, false));

        // gen=1 lookups
        assertEquals(4L, mappings.get(1L).getNewRowId(0));
        assertEquals(3L, mappings.get(1L).getNewRowId(1));
        assertEquals(2L, mappings.get(1L).getNewRowId(2));

        // gen=2 lookups
        assertEquals(1L, mappings.get(2L).getNewRowId(0));
        assertEquals(0L, mappings.get(2L).getNewRowId(1));
    }

    /**
     * Three generations with non-sequential order (simulating real merge).
     */
    public void testThreeGenerationsNonSequentialOrder() {
        // gen=5 (2 rows): 0→2, 1→3
        // gen=0 (3 rows): 0→0, 1→4, 2→1
        // gen=3 (1 row): 0→5
        Map<Long, RowIdMapping> mappings = new HashMap<>();
        mappings.put(5L, new PackedRowIdMapping(new long[] { 2, 3 }, false));
        mappings.put(0L, new PackedRowIdMapping(new long[] { 0, 4, 1 }, false));
        mappings.put(3L, new PackedRowIdMapping(new long[] { 5 }, false));

        assertEquals(2L, mappings.get(5L).getNewRowId(0));
        assertEquals(3L, mappings.get(5L).getNewRowId(1));
        assertEquals(0L, mappings.get(0L).getNewRowId(0));
        assertEquals(4L, mappings.get(0L).getNewRowId(1));
        assertEquals(1L, mappings.get(0L).getNewRowId(2));
        assertEquals(5L, mappings.get(3L).getNewRowId(0));
    }

    /**
     * toString includes useful debug info.
     */
    public void testToString() {
        long[] mappingArray = { 0, 1, 2 };
        PackedRowIdMapping mapping = new PackedRowIdMapping(mappingArray, false);
        String str = mapping.toString();
        assertTrue(str.contains("size=3"));
        assertTrue(str.contains("estimatedMemoryBytes="));
    }
}
