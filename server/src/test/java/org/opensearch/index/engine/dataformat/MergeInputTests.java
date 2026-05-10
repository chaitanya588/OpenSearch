/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link MergeInput}.
 */
public class MergeInputTests extends OpenSearchTestCase {

    public void testHasRowIdMappingsReturnsFalseWhenNull() {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();
        assertFalse(input.hasRowIdMappings());
    }

    public void testHasRowIdMappingsReturnsFalseWhenEmpty() {
        MergeInput input = MergeInput.builder().segments(List.of()).rowIdMappings(Map.of()).newWriterGeneration(1L).build();
        assertFalse(input.hasRowIdMappings());
    }

    public void testHasRowIdMappingsReturnsTrueWhenPresent() {
        long[] oldToNew = { 1, 0 };
        RowIdMapping mapping = new PackedRowIdMapping(oldToNew, false);
        MergeInput input = MergeInput.builder().segments(List.of()).rowIdMappings(Map.of(1L, mapping)).newWriterGeneration(2L).build();
        assertTrue(input.hasRowIdMappings());
    }

    public void testGetRowIdMappingReturnsMapping() {
        long[] oldToNew = { 1, 0 };
        RowIdMapping mapping = new PackedRowIdMapping(oldToNew, false);
        MergeInput input = MergeInput.builder().segments(List.of()).rowIdMappings(Map.of(5L, mapping)).newWriterGeneration(6L).build();
        assertSame(mapping, input.getRowIdMapping(5L));
    }

    public void testGetRowIdMappingReturnsNullForMissingGeneration() {
        long[] oldToNew = { 1, 0 };
        RowIdMapping mapping = new PackedRowIdMapping(oldToNew, false);
        MergeInput input = MergeInput.builder().segments(List.of()).rowIdMappings(Map.of(5L, mapping)).newWriterGeneration(6L).build();
        assertNull(input.getRowIdMapping(99L));
    }

    public void testGetRowIdMappingReturnsNullWhenMappingsNull() {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();
        assertNull(input.getRowIdMapping(1L));
    }

    public void testNewWriterGeneration() {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(42L).build();
        assertEquals(42L, input.newWriterGeneration());
    }

    public void testAddSegment() {
        Segment seg = Segment.builder(1L).build();
        MergeInput input = MergeInput.builder().addSegment(seg).newWriterGeneration(2L).build();
        assertEquals(1, input.segments().size());
    }
}
