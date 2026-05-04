/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-generation Lucene writer that creates segments in an isolated temporary directory.
 *
 * Each instance owns its own {@link IndexWriter} and {@link Directory}. Documents are
 * added via {@link #addDoc(LuceneDocumentInput)}, and on {@link #flush(FlushInput)}, the writer
 * performs a force merge to exactly 1 segment to maintain a 1:1 mapping between the
 * Lucene segment and the corresponding Parquet file for the same writer generation.
 *
 * The writer uses a large RAM buffer (256 MB by default) to minimize the chance of
 * intermediate flushes, since all writes to a single writer come from a single thread
 * (the writer pool in {@code DataFormatAwareEngine} locks the writer during use).
 *
 * After flush, the returned {@link FileInfos} contains the temp directory path so that
 * {@link LuceneIndexingExecutionEngine#refresh} can incorporate the segment into the
 * shared {@link LuceneCommitter} writer via {@code addIndexes}.
 *
 * Row ID invariant: Each document must have a {@code __row_id__} field set via
 * {@link LuceneDocumentInput#setRowId}. The row ID must equal the Lucene doc ID
 * (0-based sequential within this writer). This is asserted during flush to ensure
 * the 1:1 offset correspondence with Parquet.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneWriter implements Writer<LuceneDocumentInput> {

    private static final Logger logger = LogManager.getLogger(LuceneWriter.class);

    /** Segment info attribute key storing the writer generation for post-addIndexes correlation. */
    public static final String WRITER_GENERATION_ATTRIBUTE = "writer_generation";

    /** Large RAM buffer to avoid intermediate segment flushes within a single writer. */
    private static final double RAM_BUFFER_SIZE_MB = 256.0;

    private final long writerGeneration;
    private final LuceneDataFormat dataFormat;
    private final Path tempDirectory;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final ReentrantLock lock;
    private volatile long docCount;

    /**
     * Creates a new LuceneWriter for the given generation.
     *
     * @param writerGeneration the writer generation number
     * @param dataFormat       the Lucene data format descriptor
     * @param baseDirectory    the base directory under which to create the temp directory
     * @param analyzer         the analyzer to use for tokenized fields, or null for default
     * @param codec            the codec to use, or null for default
     * @param indexSort        the index sort to apply (null when Lucene is secondary format)
     * @throws IOException if directory creation or IndexWriter opening fails
     */
    public LuceneWriter(
        long writerGeneration,
        LuceneDataFormat dataFormat,
        Path baseDirectory,
        Analyzer analyzer,
        Codec codec,
        Sort indexSort
    ) throws IOException {
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.lock = new ReentrantLock();
        this.docCount = 0;

        // Create an isolated temp directory for this writer's segment
        this.tempDirectory = baseDirectory.resolve("lucene_gen_" + writerGeneration);
        logger.info("Creating directory for temp lucene writer: " + tempDirectory);
        Files.createDirectory(tempDirectory);
        this.directory = new MMapDirectory(tempDirectory);

        IndexWriterConfig iwc = analyzer != null ? new IndexWriterConfig(analyzer) : new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB);
        // When Lucene is primary, apply the customer's IndexSort so segments
        // are natively sorted and compatible with the shared writer's IndexSort.
        // When Lucene is secondary, no IndexSort — reorder is done via
        // ReorderingOneMerge.reorder() in configureSortedMerge().
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }

        iwc.setCodec(new LuceneWriterCodec(codec, writerGeneration));
        this.indexWriter = new IndexWriter(directory, iwc);
    }

    /**
     * Adds a document to this writer's isolated IndexWriter.
     * The document is obtained from the input's {@link LuceneDocumentInput#getFinalInput()}.
     *
     * @param input the document input containing the Lucene document to index
     * @return a success result containing the current doc ID (0-based sequential)
     * @throws IOException if the underlying IndexWriter fails to add the document
     */
    @Override
    public WriteResult addDoc(LuceneDocumentInput input) throws IOException {
        Document doc = input.getFinalInput();
        assert doc.getField(LuceneDocumentInput.ROW_ID_FIELD) != null : "Document missing required "
            + LuceneDocumentInput.ROW_ID_FIELD
            + " field at doc position "
            + docCount;
        assert doc.getField(LuceneDocumentInput.ROW_ID_FIELD).numericValue().longValue() == docCount : "Row ID mismatch: expected "
            + docCount
            + " but got "
            + doc.getField(LuceneDocumentInput.ROW_ID_FIELD).numericValue().longValue();
        indexWriter.addDocument(doc);
        long currentDocId = docCount;
        docCount++;
        return new WriteResult.Success(1L, 1L, currentDocId);
    }

    /**
     * Force-merges all buffered documents into exactly one segment, commits the IndexWriter,
     * and returns a {@link FileInfos} describing the resulting segment files in the temp directory.
     * <p>
     * After flush, the IndexWriter and Directory are closed. The temp directory files remain
     * on disk for {@link LuceneIndexingExecutionEngine#refresh} to incorporate via
     * {@code addIndexes}.
     *
     * <p>If the {@link FlushInput} carries a sort permutation from the primary data format
     * (e.g., Parquet sort-on-close), the Lucene segment is reordered using Lucene's IndexSort
     * mechanism with a custom SortField that remaps {@code ___row_id} values through the
     * permutation. This ensures the Lucene doc order matches the sorted Parquet row order.
     *
     * @param flushInput optional context; if it carries a sort permutation, the segment is sorted
     * @return file infos containing the temp directory path and segment file names,
     *         or {@link FileInfos#empty()} if no documents were added
     * @throws IOException if force merge, commit, or file listing fails
     */
    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        if (docCount == 0) {
            return FileInfos.empty();
        }

        // If sort permutation is provided, configure the reorder merge policy
        // and enable sequential __row_id__ rewrite on the codec
        if (flushInput.hasSortPermutation()) {
            configureSortedMerge(flushInput.sortPermutation());
        }

        // Common path: forceMerge to 1 segment, commit, build FileInfos
        indexWriter.forceMerge(1, true);
        indexWriter.commit();

        // Verify the invariant: exactly 1 segment with docCount documents
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        assert segmentInfos.size() == 1 : "Expected exactly 1 segment after force merge, got " + segmentInfos.size();

        SegmentCommitInfo segmentInfo = segmentInfos.info(0);
        assert segmentInfo.info.maxDoc() == docCount : "Expected " + docCount + " docs in segment, got " + segmentInfo.info.maxDoc();

        // Build the WriterFileSet pointing to the temp directory
        WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
            .directory(tempDirectory)
            .writerGeneration(writerGeneration)
            .addNumRows(docCount);

        // Add all files in the segment
        for (String file : directory.listAll()) {
            if (file.startsWith("segments") == false && file.equals("write.lock") == false) {
                wfsBuilder.addFile(file);
            }
        }

        // Since flush is once only, we can close the write post this.
        indexWriter.close();
        directory.close();

        return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
    }

    /**
     * Configures the child writer for sorted flush: sets a ReorderingMergePolicy
     * that physically reorders docs via OneMerge.reorder(), and enables sequential
     * __row_id__ rewrite on the codec so the merge writes 0..N in one pass.
     */
    private void configureSortedMerge(long[][] sortPermutation) {
        long[] oldRowIds = sortPermutation[0];
        long[] newRowIds = sortPermutation[1];
        int numDocs = (int) docCount;
        int[] oldToNew = new int[numDocs];
        int[] newToOld = new int[numDocs];
        Arrays.fill(oldToNew, -1);
        Arrays.fill(newToOld, -1);
        for (int i = 0; i < oldRowIds.length; i++) {
            int oldDoc = (int) oldRowIds[i];
            int newDoc = (int) newRowIds[i];
            if (oldDoc >= 0 && oldDoc < numDocs && newDoc >= 0 && newDoc < numDocs) {
                oldToNew[oldDoc] = newDoc;
                newToOld[newDoc] = oldDoc;
            }
        }

        indexWriter.getConfig().setMergePolicy(new ReorderingMergePolicy(oldToNew, newToOld, numDocs));
        Codec currentCodec = indexWriter.getConfig().getCodec();
        if (currentCodec instanceof LuceneWriterCodec lwc) {
            lwc.enableRowIdRewrite();
        }
    }

    /**
     * MergePolicy that wraps the standard merge selection but returns
     * ReorderingOneMerge instances that override reorder() with our DocMap.
     */
    static class ReorderingMergePolicy extends MergePolicy {
        private final int[] oldToNew;
        private final int[] newToOld;
        private final int totalDocs;
        private volatile boolean reorderDone = false;

        ReorderingMergePolicy(int[] oldToNew, int[] newToOld, int totalDocs) {
            this.oldToNew = oldToNew;
            this.newToOld = newToOld;
            this.totalDocs = totalDocs;
        }

        @Override
        public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
            return null; // no automatic merges
        }

        @Override
        public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) {
            if (reorderDone) {
                return null; // already reordered, stop the loop
            }
            reorderDone = true;

            List<SegmentCommitInfo> segments = new ArrayList<>();
            for (int i = 0; i < segmentInfos.size(); i++) {
                segments.add(segmentInfos.info(i));
            }
            if (segments.isEmpty()) {
                return null;
            }
            MergeSpecification spec = new MergeSpecification();
            spec.add(new ReorderingOneMerge(segments, oldToNew, newToOld, totalDocs));
            return spec;
        }

        @Override
        public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) {
            return null;
        }
    }

    /**
     * Custom OneMerge that overrides {@code reorder()} to provide the sort permutation
     * as a {@link Sorter.DocMap}. This causes Lucene to physically reorder docs during
     * the merge according to the Parquet sort order.
     */
    static class ReorderingOneMerge extends MergePolicy.OneMerge {
        private final int[] oldToNew;
        private final int[] newToOld;
        private final int totalDocs;

        ReorderingOneMerge(List<SegmentCommitInfo> segments, int[] oldToNew, int[] newToOld, int totalDocs) {
            super(segments);
            this.oldToNew = oldToNew;
            this.newToOld = newToOld;
            this.totalDocs = totalDocs;
        }

        @Override
        public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) throws IOException {
            return new Sorter.DocMap() {
                @Override
                public int oldToNew(int docID) {
                    if (docID >= 0 && docID < oldToNew.length) {
                        return oldToNew[docID];
                    }
                    return docID;
                }

                @Override
                public int newToOld(int docID) {
                    if (docID >= 0 && docID < newToOld.length) {
                        return newToOld[docID];
                    }
                    return docID;
                }

                @Override
                public int size() {
                    return totalDocs;
                }
            };
        }

        @Override
        public void setMergeInfo(SegmentCommitInfo info) {
            super.setMergeInfo(info);
            if (info != null) {
                info.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(0));
            }
        }
    }

    /**
     * Syncs all files in the temp directory to durable storage.
     *
     * @throws IOException if the sync fails
     */
    @Override
    public void sync() throws IOException {
        directory.sync(Arrays.asList(directory.listAll()));
        directory.syncMetaData();
    }

    /** {@inheritDoc} Returns the writer generation number assigned at construction. */
    @Override
    public long generation() {
        return writerGeneration;
    }

    /** Acquires the writer's reentrant lock. Used by the writer pool to serialize access. */
    @Override
    public void lock() {
        lock.lock();
    }

    /** Attempts to acquire the writer's reentrant lock without blocking. */
    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    /** Releases the writer's reentrant lock. */
    @Override
    public void unlock() {
        lock.unlock();
    }

    /**
     * Closes this writer, rolling back the IndexWriter if still open, closing the directory,
     * and deleting the temp directory. Safe to call multiple times.
     *
     * @throws IOException if cleanup fails
     */
    @Override
    public void close() throws IOException {
        // Close the IndexWriter and Directory if they haven't been closed by flush()
        try {
            if (indexWriter.isOpen()) {
                indexWriter.rollback();
            }
        } catch (Exception e) {
            logger.warn("Failed to rollback IndexWriter for generation[{}]: {}", writerGeneration, e);
        }
        try {
            directory.close();
        } catch (Exception e) {
            logger.warn("Failed to close directory for generation[{}]: {}", writerGeneration, e);
        }
        IOUtils.rm(tempDirectory);
    }
}
