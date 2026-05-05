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
 * Tests for {@link FlushInput}.
 */
public class FlushInputTests extends OpenSearchTestCase {

    public void testEmptyHasNoSortPermutation() {
        assertFalse(FlushInput.EMPTY.hasSortPermutation());
        assertNull(FlushInput.EMPTY.sortPermutation());
    }

    public void testNullPermutationHasNoSortPermutation() {
        FlushInput input = new FlushInput((RowIdMapping) null);
        assertFalse(input.hasSortPermutation());
    }

    public void testValidPermutationHasSortPermutation() {
        long[] oldIds = { 0, 1, 2 };
        long[] newIds = { 2, 0, 1 };
        ArrayRowIdMapping mapping = new ArrayRowIdMapping(oldIds, newIds, 3);
        FlushInput input = new FlushInput(mapping);
        assertTrue(input.hasSortPermutation());
        assertNotNull(input.sortPermutation());
        assertEquals(3, input.sortPermutation().size());
        assertEquals(2, input.sortPermutation().oldToNew(0));
        assertEquals(0, input.sortPermutation().oldToNew(1));
        assertEquals(1, input.sortPermutation().oldToNew(2));
    }

    public void testFromRawPermutationNull() {
        assertNull(ArrayRowIdMapping.fromRawPermutation(null));
    }

    public void testFromRawPermutationEmpty() {
        assertNull(ArrayRowIdMapping.fromRawPermutation(new long[][] { new long[0], new long[0] }));
    }

    public void testFromRawPermutationWrongShape() {
        assertNull(ArrayRowIdMapping.fromRawPermutation(new long[][] { new long[] { 1, 2 } }));
    }
}
