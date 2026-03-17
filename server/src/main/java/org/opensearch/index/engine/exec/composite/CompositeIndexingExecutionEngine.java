/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeDataFormatWriterPool;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriter;
import org.opensearch.index.engine.exec.merge.LuceneMerger;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    private static final Logger logger = LogManager.getLogger(CompositeIndexingExecutionEngine.class);

    private final CompositeDataFormatWriterPool dataFormatWriterPool;
    private final Any dataFormat;
    private final AtomicLong writerGeneration;
    private final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();
    private final String sortColumn;
    private final boolean reverseSort;

    public CompositeIndexingExecutionEngine(
        EngineConfig engineConfig,
        MapperService mapperService,
        PluginsService pluginsService,
        ShardPath shardPath,
        long initialWriterGeneration
    ) {
        this.writerGeneration = new AtomicLong(initialWriterGeneration);

        // Extract sort configuration from index settings first, before creating
        // delegates or the writer pool, so that delegates have sort info set
        // before any call to createWriter() can occur.
        Sort indexSort = engineConfig.getIndexSort();
        if (indexSort != null && indexSort.getSort().length > 0) {
            SortField primarySortField = indexSort.getSort()[0];
            this.sortColumn = primarySortField.getField();
            this.reverseSort = primarySortField.getReverse();
        } else {
            this.sortColumn = null;
            this.reverseSort = false;
        }

        List<DataFormat> dataFormats = new ArrayList<>();
        pluginsService.filterPlugins(DataSourcePlugin.class).forEach(dataSourcePlugin -> {
            dataFormats.add(dataSourcePlugin.getDataFormat());
            IndexingExecutionEngine<?> delegate = dataSourcePlugin.indexingEngine(engineConfig, mapperService, shardPath);
            delegate.setSortColumn(this.sortColumn);
            delegate.setReverseSort(this.reverseSort);
            delegates.add(delegate);
        });
        this.dataFormat = new Any(dataFormats, dataFormats.get(1));
        this.dataFormatWriterPool = new CompositeDataFormatWriterPool(
            () -> new CompositeDataFormatWriter(this, writerGeneration.getAndIncrement()),
            LinkedList::new,
            Runtime.getRuntime().availableProcessors()
        );
    }

    @Override
    public Any getDataFormat() {
        return dataFormat;
    }

    public long getNextWriterGeneration() {
        return writerGeneration.getAndIncrement();
    }

    /**
     * Updates the writer generation counter to be at least minGeneration + 1.
     * This is used during replication/recovery to ensure the replica's writer generation
     * is always greater than any replicated file's generation, preventing file name collisions.
     *
     * @param minGeneration The minimum generation value from replicated files
     */
    public void updateWriterGenerationIfNeeded(long minGeneration) {
        writerGeneration.updateAndGet(current -> Math.max(current, minGeneration + 1));
    }

    /**
     * Gets the current writer generation without incrementing.
     *
     * @return The current writer generation value
     */
    public long getCurrentWriterGeneration() {
        return writerGeneration.get();
    }

    @Override
    public List<String> supportedFieldTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            delegate.loadWriterFiles(catalogSnapshot);
        }
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            Map<String, Collection<String>> formatSpecificFilesToDelete = new HashMap<>();
            formatSpecificFilesToDelete.put(delegate.getDataFormat().name(), filesToDelete.get(delegate.getDataFormat().name()));
            delegate.deleteFiles(formatSpecificFilesToDelete);
        }
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter(long generation) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createCompositeWriter() {
        return dataFormatWriterPool.getAndLock();
    }

    @Override
    public RefreshResult refresh(RefreshInput ignore) throws IOException {
        RefreshResult finalResult;
        final long refreshStartNanos = System.nanoTime();
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = dataFormatWriterPool.checkoutAll();
            List<Segment> refreshedSegment = ignore.getExistingSegments();
            List<Segment> newSegmentList = new ArrayList<>();
            int totalWritersFlushed = 0;
            long totalFlushNanos = 0;
            long totalSortMergeNanos = 0;
            long totalCloseNanos = 0;

            // flush to disk, then sort-merge before closing writers
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                Segment newSegment = new Segment(dataFormatWriter.getWriterGeneration());

                long flushStart = System.nanoTime();
                FileInfos fileInfos = dataFormatWriter.flush(null);
                long flushElapsed = System.nanoTime() - flushStart;
                totalFlushNanos += flushElapsed;

                fileInfos.getWriterFilesMap().forEach((key, value) -> {
                    newSegment.addSearchableFiles(key.name(), value);
                });

                // Post-flush sort: use the sort permutation from flush (captured during
                // sort-on-close) to reorder Lucene. No redundant Parquet re-merge needed.
                long sortMergeStart = System.nanoTime();
                applySortMergeOnChildWriter(dataFormatWriter, newSegment, fileInfos.getSortPermutation());
                long sortMergeElapsed = System.nanoTime() - sortMergeStart;
                totalSortMergeNanos += sortMergeElapsed;

                long closeStart = System.nanoTime();
                dataFormatWriter.close();
                long closeElapsed = System.nanoTime() - closeStart;
                totalCloseNanos += closeElapsed;

                if (!newSegment.getDFGroupedSearchableFiles().isEmpty()) {
                    newSegmentList.add(newSegment);
                }
                totalWritersFlushed++;
            }

            if (newSegmentList.isEmpty()) {
                long totalElapsedMs = (System.nanoTime() - refreshStartNanos) / 1_000_000;
                logger.debug("Refresh completed with no new segments (writers checked out: {}, total: {}ms)",
                    totalWritersFlushed, totalElapsedMs);
                return null;
            } else {
                refreshedSegment.addAll(newSegmentList);
            }

            // call refresh for delegates
            long delegateRefreshStart = System.nanoTime();
            for (IndexingExecutionEngine<?> delegate : delegates) {
                delegate.refresh(new RefreshInput());
            }
            long delegateRefreshNanos = System.nanoTime() - delegateRefreshStart;

            // make indexing engines aware of everything
            finalResult = new RefreshResult();
            finalResult.setRefreshedSegments(refreshedSegment);

            long totalElapsedMs = (System.nanoTime() - refreshStartNanos) / 1_000_000;
            logger.info("Refresh completed: writers={}, newSegments={}, flushMs={}, sortMergeMs={}, closeMs={}, delegateRefreshMs={}, totalMs={}",
                totalWritersFlushed,
                newSegmentList.size(),
                totalFlushNanos / 1_000_000,
                totalSortMergeNanos / 1_000_000,
                totalCloseNanos / 1_000_000,
                delegateRefreshNanos / 1_000_000,
                totalElapsedMs);

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            long totalElapsedMs = (System.nanoTime() - refreshStartNanos) / 1_000_000;
            logger.error("Refresh failed after {}ms", totalElapsedMs, ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Uses the sort permutation captured during Parquet sort-on-close to reorder
     * the Lucene segment. This eliminates the redundant Parquet re-merge that was
     * previously needed just to produce a RowIdMapping.
     *
     * The Parquet file is already sorted in-place during closeWriter (sort-on-close),
     * so we only need to apply the permutation to the Lucene segment.
     *
     * @param dataFormatWriter the composite writer whose child writers are still open
     * @param segment the segment to reorder (modified in-place on success)
     * @param sortPermutation the RowIdMapping from sort-on-close, or null if no sort happened
     */
    private void applySortMergeOnChildWriter(CompositeDataFormatWriter dataFormatWriter, Segment segment, RowIdMapping sortPermutation) {
        if (sortPermutation == null || sortPermutation.size() == 0) {
            return; // No sorting configured or empty file — nothing to reorder
        }

        try {
            long originalGeneration = dataFormatWriter.getWriterGeneration();

            // Reorder Lucene segment using the sort permutation from Parquet sort-on-close
            LuceneWriter luceneWriter = findLuceneWriter(dataFormatWriter);
            if (luceneWriter == null) {
                return;
            }

            LuceneMerger childLuceneMerger = new LuceneMerger(
                luceneWriter.getIndexWriter(),
                luceneWriter.getDirectoryPath()
            );

            WriterFileSet luceneFiles = segment.getDFGroupedSearchableFiles().get(DataFormat.LUCENE.name());
            if (luceneFiles == null) {
                return;
            }

            MergeResult luceneMergeResult = childLuceneMerger.merge(
                List.of(luceneFiles), sortPermutation, originalGeneration
            );

            WriterFileSet sortedLuceneFiles = luceneMergeResult.getMergedWriterFileSetForDataformat(DataFormat.LUCENE);
            if (sortedLuceneFiles != null) {
                segment.addSearchableFiles(DataFormat.LUCENE.name(), sortedLuceneFiles);
            }

            // Commit the child writer so that its segments_N file reflects the sorted
            // segment (_1) instead of the old unsorted segment (_0). This is critical
            // because addLuceneIndexes opens the child directory via NIOFSDirectory which
            // reads segments_N to discover which segments to copy to the parent writer.
            luceneWriter.sync();

            logger.info("Applied post-flush sort reorder for segment generation [{}] using sort-on-close permutation ({} entries)",
                originalGeneration, sortPermutation.size());

        } catch (Exception e) {
            // Non-fatal: the segment is still valid, just unsorted
            logger.warn("Post-flush sort reorder failed for segment generation [{}], continuing with unsorted segment",
                dataFormatWriter.getWriterGeneration(), e);
        }
    }

    /**
     * Finds the LuceneWriter from a CompositeDataFormatWriter's underlying writers.
     */
    private LuceneWriter findLuceneWriter(CompositeDataFormatWriter dataFormatWriter) {
        for (Map.Entry<DataFormat, Writer<? extends DocumentInput<?>>> entry : dataFormatWriter.getWriters()) {
            if (entry.getKey().equals(DataFormat.LUCENE) && entry.getValue() instanceof LuceneWriter) {
                return (LuceneWriter) entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Merger getMerger() {
        throw new UnsupportedOperationException("Merger for Composite Engine is not used");
    }

    public List<IndexingExecutionEngine<?>> getDelegates() {
        return Collections.unmodifiableList(delegates);
    }

    public String getSortColumn() {
        return sortColumn;
    }

    public boolean isReverseSort() {
        return reverseSort;
    }

    public CompositeDataFormatWriterPool getDataFormatWriterPool() {
        return dataFormatWriterPool;
    }

    public long getNativeBytesUsed() {
        return delegates.stream().mapToLong(IndexingExecutionEngine::getNativeBytesUsed).sum();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegates);
    }
}
