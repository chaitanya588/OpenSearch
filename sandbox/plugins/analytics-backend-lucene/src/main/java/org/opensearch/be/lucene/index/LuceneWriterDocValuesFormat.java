/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * A {@link DocValuesFormat} that intercepts writes to the {@code __row_id__} field and
 * replaces the values with sequential 0..N. This allows the reorder merge and the row ID
 * rewrite to happen in a single pass during flush sort.
 *
 * <p>All other fields are delegated unchanged to the wrapped format.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class LuceneWriterDocValuesFormat extends DocValuesFormat {

    private static final String ROW_ID = LuceneDocumentInput.ROW_ID_FIELD;

    private final DocValuesFormat delegate;

    LuceneWriterDocValuesFormat(DocValuesFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        DocValuesConsumer delegateConsumer = delegate.fieldsConsumer(state);
        return new DocValuesConsumer() {
            @Override
            public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                if (ROW_ID.equals(field.name)) {
                    delegateConsumer.addSortedNumericField(field, new SequentialRowIdProducer(state.segmentInfo.maxDoc()));
                } else {
                    delegateConsumer.addSortedNumericField(field, valuesProducer);
                }
            }

            @Override
            public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegateConsumer.addNumericField(field, valuesProducer);
            }

            @Override
            public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegateConsumer.addBinaryField(field, valuesProducer);
            }

            @Override
            public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegateConsumer.addSortedField(field, valuesProducer);
            }

            @Override
            public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                delegateConsumer.addSortedSetField(field, valuesProducer);
            }

            @Override
            public void close() throws IOException {
                delegateConsumer.close();
            }
        };
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }

    /**
     * A DocValuesProducer that returns sequential 0..N values for the __row_id__ field.
     */
    private static class SequentialRowIdProducer extends DocValuesProducer {
        private final int maxDoc;

        SequentialRowIdProducer(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo fi) {
            return new SortedNumericDocValues() {
                private int docID = -1;

                @Override
                public long nextValue() {
                    return docID;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public boolean advanceExact(int target) {
                    docID = target;
                    return true;
                }

                @Override
                public int docID() {
                    return docID;
                }

                @Override
                public int nextDoc() {
                    return ++docID < maxDoc ? docID : NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) {
                    docID = target;
                    return docID < maxDoc ? docID : NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo fi) {
            return null;
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo fi) {
            return null;
        }

        @Override
        public SortedDocValues getSorted(FieldInfo fi) {
            return null;
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo fi) {
            return null;
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo fi) {
            return null;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public void close() {}
    }
}
