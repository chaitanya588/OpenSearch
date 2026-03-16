/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitPoint;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.LuceneCommitEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriter;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriterCodec;
import org.opensearch.index.engine.exec.merge.CustomIndexWriter;
import org.opensearch.index.engine.exec.merge.LuceneMerger;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter.ROW_ID;

public class LuceneExecutionEngine implements IndexingExecutionEngine<DataFormat.LuceneDataFormat>, Committer {

    private final Logger logger;
    private final MapperService mapperService;
    private final ShardPath shardPath;
    private final DataFormat dataFormat;
    private final EngineConfig engineConfig;
    private LuceneCommitEngine luceneCommitEngine;
    private LuceneMerger luceneMerger;

    public LuceneExecutionEngine(EngineConfig engineConfig, MapperService mapperService, ShardPath shardPath, DataFormat dataFormat) {
        this.logger = Loggers.getLogger(LuceneExecutionEngine.class, shardPath.getShardId());
        this.engineConfig = engineConfig;
        this.dataFormat = dataFormat;
        this.mapperService = mapperService;
        this.shardPath = shardPath;
    }

    /**
     * Initializes the commit-time IndexWriter and LuceneCommitEngine.
     * Must be called before any Committer methods are used.
     */
    public void initializeCommitEngine(
        Store store,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        boolean primaryMode
    ) throws IOException {
        Logger commitLogger = Loggers.getLogger(LuceneCommitEngine.class, store.shardId());
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(
            commitLogger, translogDeletionPolicy, null, globalCheckpointSupplier
        );
        CustomIndexWriter commitIndexWriter = null;
        if (primaryMode) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setIndexDeletionPolicy(combinedDeletionPolicy);
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());
            // Force compound file format for merged segments (fewer files, simpler management)
            iwc.setUseCompoundFile(true);
//            iwc.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
            // DEBUG: log every incRef/decRef to trace unreferenced file issues
            enableVerboseRefCounts();
            iwc.setInfoStream(new PrintStreamInfoStream(System.out));
            commitIndexWriter = new CustomIndexWriter(store.directory(), iwc);
        }
        this.luceneCommitEngine = new LuceneCommitEngine(store, combinedDeletionPolicy, commitIndexWriter);
        this.luceneMerger = new LuceneMerger(commitIndexWriter, Path.of(store.directory().toString()));
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public LuceneWriter createWriter(long writerGeneration) throws IOException {
        Path directoryPath = Files.createTempDirectory(Long.toString(System.nanoTime()));
        return new LuceneWriter(directoryPath, createWriter(directoryPath, writerGeneration), writerGeneration);
    }

    private CustomIndexWriter createWriter(Path directoryPath, long writerGeneration) throws IOException {
        try {
            IndexWriterConfig iwc = getIndexWriterConfig(writerGeneration);
            Directory directory = NIOFSDirectory.open(directoryPath);
            return new CustomIndexWriter(directory, iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    /** DEBUG only: sets IndexFileDeleter.VERBOSE_REF_COUNTS=true via reflection (field is package-private). */
    private static void enableVerboseRefCounts() {
        try {
            Class<?> ifd = Class.forName("org.apache.lucene.index.IndexFileDeleter");
            java.lang.reflect.Field f = ifd.getDeclaredField("VERBOSE_REF_COUNTS");
            f.setAccessible(true);
            f.set(null, true);
        } catch (Exception e) {
//            logger.warn("Could not enable VERBOSE_REF_COUNTS on IndexFileDeleter", e);
        }
    }

    private IndexWriterConfig getIndexWriterConfig(long writerGeneration) {
        final IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // For optimized (composite) indices, Parquet owns sorting and Lucene is
        // reordered via the sort permutation after flush. Setting an index sort
        // on the Lucene writer would cause Lucene to attempt its own sort during
        // merge, conflicting with the Parquet-driven sort order.
        if (!engineConfig.getIndexSettings().isOptimizedIndex()) {
            iwc.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
        }
        iwc.setCodec(new LuceneWriterCodec(engineConfig.getCodec().getName(), engineConfig.getCodec(), writerGeneration));
        return iwc;
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) {
        // Noop, as refresh is handled in layers above
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) {

    }

    @Override
    public Merger getMerger() {
        return luceneMerger;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        // NO-OP, as refresh is being handled at CompositeIndexingExecutionEngine
        return new RefreshResult();
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    // --- Committer delegation ---

    @Override
    public DirectoryReader addLuceneIndexes(List<Segment> segments) {
        return luceneCommitEngine.addLuceneIndexes(segments);
    }

    @Override
    public CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot) {
        return luceneCommitEngine.commit(commitData, catalogSnapshot);
    }

    @Override
    public Map<String, String> getLastCommittedData() {
        return luceneCommitEngine.getLastCommittedData();
    }

    @Override
    public CommitStats getCommitStats() {
        return luceneCommitEngine.getCommitStats();
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return luceneCommitEngine.getSafeCommitInfo();
    }

    public GatedCloseable<org.apache.lucene.index.IndexCommit> acquireSafeIndexCommit() throws EngineException {
        return luceneCommitEngine.acquireSafeIndexCommit();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(luceneCommitEngine);
    }
}
