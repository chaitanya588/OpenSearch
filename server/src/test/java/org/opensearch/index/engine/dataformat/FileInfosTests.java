/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Map;

/**
 * Tests for {@link FileInfos}.
 */
public class FileInfosTests extends OpenSearchTestCase {

    public void testRowIdMappingIsNullByDefault() {
        FileInfos infos = FileInfos.builder().build();
        assertNull(infos.rowIdMapping());
    }

    public void testRowIdMappingSetViaBuilder() {
        long[] oldToNew = { 2, 0, 1 };
        RowIdMapping mapping = new PackedRowIdMapping(oldToNew, true);
        FileInfos infos = FileInfos.builder().rowIdMapping(mapping).build();
        assertNotNull(infos.rowIdMapping());
        assertSame(mapping, infos.rowIdMapping());
        assertTrue(infos.rowIdMapping().isNewToOldSupported());
    }

    public void testEmptyFileInfosHasNoMapping() {
        FileInfos empty = FileInfos.empty();
        assertNull(empty.rowIdMapping());
        assertTrue(empty.writerFilesMap().isEmpty());
    }

    public void testPutWriterFileSet() {
        DataFormat format = new MockDataFormat();
        Path dir = createTempDir();
        WriterFileSet fileSet = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data.dat").addNumRows(5).build();

        FileInfos infos = FileInfos.builder().putWriterFileSet(format, fileSet).build();
        assertTrue(infos.getWriterFileSet(format).isPresent());
        assertEquals(5, infos.getWriterFileSet(format).get().numRows());
    }

    public void testPutAll() {
        DataFormat format = new MockDataFormat();
        Path dir = createTempDir();
        WriterFileSet fileSet = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data.dat").addNumRows(3).build();

        FileInfos infos = FileInfos.builder().putAll(Map.of(format, fileSet)).build();
        assertTrue(infos.getWriterFileSet(format).isPresent());
    }

    public void testGetWriterFileSetReturnsEmptyForUnknownFormat() {
        DataFormat format = new MockDataFormat();
        FileInfos infos = FileInfos.builder().build();
        assertFalse(infos.getWriterFileSet(format).isPresent());
    }
}
