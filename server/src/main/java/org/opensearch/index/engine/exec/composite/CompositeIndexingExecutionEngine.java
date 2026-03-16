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
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.MergeInput;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeDataFormatWriterPool;
import org.opensearch.index.engine.exec.coord.Segment;
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

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

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
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = dataFormatWriterPool.checkoutAll();
            List<Segment> refreshedSegment = ignore.getExistingSegments();
            List<Segment> newSegmentList = new ArrayList<>();
            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                Segment newSegment = new Segment(dataFormatWriter.getWriterGeneration());
                FileInfos fileInfos = dataFormatWriter.flush(null);
                fileInfos.getWriterFilesMap().forEach((key, value) -> {
                    newSegment.addSearchableFiles(key.name(), value);
                });
                dataFormatWriter.close();
                if (!newSegment.getDFGroupedSearchableFiles().isEmpty()) {
                    newSegmentList.add(newSegment);
                }
            }

            if (newSegmentList.isEmpty()) {
                return null;
            } else {
                refreshedSegment.addAll(newSegmentList);
            }

            // call refresh for delegats
            for (IndexingExecutionEngine<?> delegate : delegates) {
                delegate.refresh(new RefreshInput());
            }

            // make indexing engines aware of everything
            finalResult = new RefreshResult();
            finalResult.setRefreshedSegments(refreshedSegment);

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Performs a single-segment sort merge on a freshly flushed segment while the child
     * writers are still alive. Sorts the primary data format (Parquet) to produce a
     * RowIdMapping, then uses that mapping to reorder the Lucene segment via a temporary
     * LuceneMerger operating on the child CustomIndexWriter.
     *
     * If the sort merge succeeds, the segment's WriterFileSets are updated in-place
     * with the sorted files. If it fails, the original unsorted segment is left intact.
     *
     * @param dataFormatWriter the composite writer whose child writers are still open
     * @param segment the segment to sort-merge (modified in-place on success)
     */
    private void applySortMergeOnChildWriter(CompositeDataFormatWriter dataFormatWriter, Segment segment) {
        DataFormat primaryFormat = dataFormat.getPrimaryDataFormat();
        WriterFileSet primaryFiles = segment.getDFGroupedSearchableFiles().get(primaryFormat.name());
        if (primaryFiles == null) {
            return;
        }

        try {
            // Use the original writer generation for the sort merge output.
            // This is an in-place reorder of the same logical segment, not a new segment,
            // so it should keep the same generation to avoid mismatches between
            // Segment.generation and WriterFileSet.writerGeneration.
            long originalGeneration = dataFormatWriter.getWriterGeneration();

            // Step 1: Sort the primary format (Parquet) — this produces the RowIdMapping
            Merger primaryMerger = delegates.stream()
                .filter(d -> d.getDataFormat().equals(primaryFormat))
                .findFirst()
                .orElseThrow()
                .getMerger();

            MergeInput mergeInput = new MergeInput(List.of(primaryFiles), originalGeneration, sortColumn, reverseSort);
            MergeResult primaryMergeResult = primaryMerger.merge(mergeInput);
            RowIdMapping rowIdMapping = primaryMergeResult.getRowIdMapping();
            if (rowIdMapping == null || rowIdMapping.size() == 0) {
                return; // No reordering needed
            }

            // Update primary format files in the segment with sorted output
            WriterFileSet sortedPrimaryFiles = primaryMergeResult.getMergedWriterFileSetForDataformat(primaryFormat);
            if (sortedPrimaryFiles != null) {
                segment.addSearchableFiles(primaryFormat.name(), sortedPrimaryFiles);

                // Delete the original unsorted parquet files from disk.
                // These files were written during flush() but were never added to any
                // CatalogSnapshot (the segment's WriterFileSet was replaced above before
                // reaching advanceCatalogSnapshot). Since IndexFileDeleter only tracks
                // files via CatalogSnapshot ref-counting, these orphans would never be
                // cleaned up otherwise.
                deleteUnsortedFiles(primaryFiles, sortedPrimaryFiles);
            }

            // Step 2: Reorder Lucene segment using RowIdMapping on the child writer
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
                List.of(luceneFiles), rowIdMapping, originalGeneration
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

            logger.info("Applied post-flush sort merge for segment generation [{}]", originalGeneration);

        } catch (Exception e) {
            // Non-fatal: the segment is still valid, just unsorted
            logger.warn("Post-flush sort merge failed for segment generation [{}], continuing with unsorted segment",
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

    /**
     * Deletes the original unsorted files from disk after sort merge has produced
     * sorted replacements. Only deletes files that are in the old set but NOT in
     * the new sorted set (to be safe in case any filenames overlap).
     */
    private void deleteUnsortedFiles(WriterFileSet unsortedFiles, WriterFileSet sortedFiles) {
        Path directory = Path.of(unsortedFiles.getDirectory());
        for (String file : unsortedFiles.getFiles()) {
            if (!sortedFiles.getFiles().contains(file)) {
                try {
                    Path filePath = directory.resolve(file);
                    Files.deleteIfExists(filePath);
                    logger.debug("Deleted unsorted file after sort merge: [{}]", filePath);
                } catch (IOException e) {
                    logger.warn("Failed to delete unsorted file [{}]: {}", file, e.getMessage());
                }
            }
        }
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
