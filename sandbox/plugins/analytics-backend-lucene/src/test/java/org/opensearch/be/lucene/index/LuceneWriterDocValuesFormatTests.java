/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests for {@link LuceneWriterDocValuesFormat}.
 * Verifies that the __row_id__ field is rewritten to sequential 0..N
 * while other fields are passed through unchanged.
 */
public class LuceneWriterDocValuesFormatTests extends OpenSearchTestCase {

    /**
     * Verifies that the SequentialRowIdProducer produces sequential values 0..N-1
     * for the __row_id__ field when documents are written with non-sequential row IDs.
     */
    public void testRowIdFieldIsRewrittenToSequential() throws IOException {
        Path dir = createTempDir();

        LuceneWriterCodec codec = new LuceneWriterCodec(Codec.getDefault(), 0);
        codec.enableRowIdRewrite();

        try (Directory directory = new MMapDirectory(dir)) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // Write docs with non-sequential row IDs (100, 200, 300)
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField(LuceneDocumentInput.ROW_ID_FIELD, (i + 1) * 100));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            // Read back and verify row IDs are sequential 0, 1, 2
            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIds);

                for (int i = 0; i < 3; i++) {
                    assertTrue(rowIds.advanceExact(i));
                    assertEquals(1, rowIds.docValueCount());
                    assertEquals(i, rowIds.nextValue());
                }
            }
        }
    }

    /**
     * Verifies that non-row-id SortedNumericDocValues fields are passed through unchanged.
     */
    public void testNonRowIdFieldIsPassedThrough() throws IOException {
        Path dir = createTempDir();

        LuceneWriterCodec codec = new LuceneWriterCodec(Codec.getDefault(), 0);
        codec.enableRowIdRewrite();

        try (Directory directory = new MMapDirectory(dir)) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField("my_field", (i + 1) * 100));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            // Read back and verify values are unchanged (100, 200, 300)
            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues values = leaf.getSortedNumericDocValues("my_field");
                assertNotNull(values);

                for (int i = 0; i < 3; i++) {
                    assertTrue(values.advanceExact(i));
                    assertEquals(1, values.docValueCount());
                    assertEquals((i + 1) * 100L, values.nextValue());
                }
            }
        }
    }

    /**
     * Verifies that fieldsProducer delegates to the wrapped format.
     */
    public void testFieldsProducerDelegates() throws IOException {
        Path dir = createTempDir();

        LuceneWriterCodec codec = new LuceneWriterCodec(Codec.getDefault(), 0);
        codec.enableRowIdRewrite();

        try (Directory directory = new MMapDirectory(dir)) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("test_field", 42));
                writer.addDocument(doc);
                writer.commit();
            }

            // If fieldsProducer didn't delegate properly, reading would fail
            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues values = leaf.getSortedNumericDocValues("test_field");
                assertNotNull(values);
                assertTrue(values.advanceExact(0));
                assertEquals(42L, values.nextValue());
            }
        }
    }

    /**
     * Verifies SequentialRowIdProducer with a single document.
     */
    public void testSingleDocRowIdIsZero() throws IOException {
        Path dir = createTempDir();

        LuceneWriterCodec codec = new LuceneWriterCodec(Codec.getDefault(), 0);
        codec.enableRowIdRewrite();

        try (Directory directory = new MMapDirectory(dir)) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField(LuceneDocumentInput.ROW_ID_FIELD, 999));
                writer.addDocument(doc);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIds);
                assertTrue(rowIds.advanceExact(0));
                assertEquals(0L, rowIds.nextValue());
            }
        }
    }

    /**
     * Verifies that without enableRowIdRewrite, row IDs are written as-is.
     */
    public void testRowIdNotRewrittenWhenDisabled() throws IOException {
        Path dir = createTempDir();

        // Do NOT call enableRowIdRewrite
        LuceneWriterCodec codec = new LuceneWriterCodec(Codec.getDefault(), 0);

        try (Directory directory = new MMapDirectory(dir)) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(codec);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField(LuceneDocumentInput.ROW_ID_FIELD, (i + 1) * 100));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            // Row IDs should be the original values (100, 200, 300)
            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIds);

                for (int i = 0; i < 3; i++) {
                    assertTrue(rowIds.advanceExact(i));
                    assertEquals((i + 1) * 100L, rowIds.nextValue());
                }
            }
        }
    }
}
