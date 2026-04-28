/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockReaderManager;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Integration tests for composite merge operations across single and multiple data format engines.
 *
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeMergeIT" \
 *   -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    // ── Mock DataFormatPlugin using test framework stubs ──

    public static class MockParquetDataFormatPlugin extends MockDataFormatPlugin implements SearchBackEndPlugin<Object> {
        private static final MockDataFormat PARQUET_FORMAT = new MockDataFormat("parquet", 0L, Set.of());

        public MockParquetDataFormatPlugin() {
            super(PARQUET_FORMAT);
        }

        @Override
        public Map<String, DataFormatDescriptor> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
            return Map.of("parquet", new DataFormatDescriptor("parquet", new PrecomputedChecksumStrategy()));
        }

        @Override
        public String name() {
            return "mock-parquet-backend";
        }

        @Override
        public List<String> getSupportedFormats() {
            return List.of("parquet");
        }

        @Override
        public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
            return new MockReaderManager("parquet");
        }
    }

    // ── Test setup ──

    @Override
    public void setUp() throws Exception {
        enableMerge();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist if test failed before creation
        }
        super.tearDown();
        disableMerge();
    }

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, MockParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    // ── Tests ──

    /**
     * Verifies that background merges are triggered automatically after refresh
     * when enough segments accumulate to exceed the TieredMergePolicy threshold.
     * <p>
     * Flow: index docs across many refresh cycles → each refresh calls
     * triggerPossibleMerges() → MergeScheduler picks up merge candidates
     * asynchronously → segment count decreases.
     */
    public void testBackgroundMergeSingleEngine() throws Exception {
        createIndex(INDEX_NAME, singleEngineSettings());
        ensureGreen(INDEX_NAME);

        // Create enough segments to exceed TieredMergePolicy's default threshold (~10)
        int totalSegmentsCreated = indexDocsAcrossMultipleRefreshes(15, 5);

        // Wait for async background merges to complete
        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + totalSegmentsCreated + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < totalSegmentsCreated
            );
        }, 30, TimeUnit.SECONDS);

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
    }

    /**
     * Verifies that background merges work correctly with a dual-engine composite index
     * (mock parquet primary + Lucene secondary) and that the Lucene secondary receives
     * the RowIdMapping from the primary merge and applies it to remap ___row_id doc values.
     * <p>
     * Flow: index docs across many refresh cycles → each refresh creates segments in both
     * formats → background merges trigger → primary (mock parquet) merges first and produces
     * a RowIdMapping → secondary (Lucene) merges using that mapping → ___row_id values in
     * the merged Lucene segment are remapped according to the primary's mapping.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testBackgroundMergeDualEngineWithRowIdRemapping() throws Exception {
        createIndex(INDEX_NAME, dualEngineSettings());
        ensureGreen(INDEX_NAME);

        // Create enough segments to exceed TieredMergePolicy's default threshold (~10)
        int totalSegmentsCreated = indexDocsAcrossMultipleRefreshes(15, 5);

        // Wait for async background merges to complete
        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + totalSegmentsCreated + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < totalSegmentsCreated
            );
        }, 30, TimeUnit.SECONDS);

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        // Verify both formats are present in the catalog snapshot
        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet", "lucene"), snapshot.getDataFormats());

        // Verify merged segments have files for both formats
        for (Segment segment : snapshot.getSegments()) {
            assertTrue("Each segment should have parquet files", segment.dfGroupedSearchableFiles().containsKey("parquet"));
            assertTrue("Each segment should have lucene files", segment.dfGroupedSearchableFiles().containsKey("lucene"));
        }

        // Access the shard's Lucene IndexWriter to read ___row_id doc values from the merged index
        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        assertNotNull("Engine should not be null", engine);

        // Read ___row_id values from the Lucene index via the shard's store directory.
        // After merge with RowIdMapping, the ___row_id values should be sequential (0..n-1)
        // because the MockMerger produces an offset-based identity mapping.
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(shard.store().directory())) {
            int totalDocs = reader.numDocs();
            assertTrue("Should have indexed documents", totalDocs > 0);

            // Collect all ___row_id values across all leaves
            Set<Long> allRowIds = new HashSet<>();
            for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                org.apache.lucene.index.SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues("___row_id");
                if (rowIdDV != null) {
                    for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                        if (rowIdDV.advanceExact(i)) {
                            allRowIds.add(rowIdDV.nextValue());
                        }
                    }
                }
            }

            // Verify ___row_id values are present and unique (no duplicates from bad remapping)
            assertFalse("Should have ___row_id doc values", allRowIds.isEmpty());
            assertEquals("Each document should have a unique ___row_id (no duplicates from merge remapping)", totalDocs, allRowIds.size());

            // Verify ___row_id values are non-negative
            for (long rowId : allRowIds) {
                assertTrue("___row_id should be non-negative, got: " + rowId, rowId >= 0);
            }
        }
    }

    /**
     * Verifies that after a dual-engine merge, the Lucene segment has documents sorted by
     * ascending {@code ___row_id} values and that the row IDs are contiguous (0..n-1) with
     * no gaps or duplicates.
     *
     * <p>This is the concrete correctness test for the IndexSort-based merge approach:
     * the primary (mock parquet) merge produces a RowIdMapping, and the secondary (Lucene)
     * merge uses it via {@code addIndexes(CodecReader...)} with the RowIdRemappingSortField
     * to reorder documents and rewrite {@code ___row_id} values.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
    public void testMergedLuceneSegmentHasAscendingRowIds() throws Exception {
        createIndex(INDEX_NAME, dualEngineSettings());
        ensureGreen(INDEX_NAME);

        // Index enough docs across refresh cycles to create multiple segments and trigger merge
        int refreshCycles = 15;
        int docsPerCycle = 5;
        indexDocsAcrossMultipleRefreshes(refreshCycles, docsPerCycle);

        // Wait for background merges to complete
        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue("Expected merges to reduce segment count below " + refreshCycles, snapshot.getSegments().size() < refreshCycles);
        }, 30, TimeUnit.SECONDS);

        // Read the Lucene index and verify ___row_id ordering in each merged segment
        IndexShard shard = getPrimaryShard();
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(shard.store().directory())) {
            int totalDocs = reader.numDocs();
            assertTrue("Should have indexed documents, got " + totalDocs, totalDocs > 0);

            // For each leaf (segment), verify ___row_id values are strictly ascending
            for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                int maxDoc = ctx.reader().maxDoc();
                org.apache.lucene.index.SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues("___row_id");
                if (rowIdDV == null || maxDoc == 0) {
                    continue;
                }

                long previousRowId = -1;
                for (int doc = 0; doc < maxDoc; doc++) {
                    assertTrue("Doc " + doc + " should have ___row_id", rowIdDV.advanceExact(doc));
                    long rowId = rowIdDV.nextValue();

                    assertTrue("___row_id should be non-negative, got " + rowId + " at doc " + doc, rowId >= 0);
                    assertTrue(
                        "___row_id values should be ascending within a segment: doc "
                            + doc
                            + " has rowId="
                            + rowId
                            + " but previous was "
                            + previousRowId,
                        rowId > previousRowId
                    );
                    previousRowId = rowId;
                }
            }

            // Collect all ___row_id values across all segments and verify uniqueness
            Set<Long> allRowIds = new HashSet<>();
            for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                org.apache.lucene.index.SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues("___row_id");
                if (rowIdDV == null) continue;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowIdDV.advanceExact(doc)) {
                        long rowId = rowIdDV.nextValue();
                        assertTrue("Duplicate ___row_id found: " + rowId, allRowIds.add(rowId));
                    }
                }
            }

            assertEquals("Total unique ___row_id values should equal total docs", totalDocs, allRowIds.size());

            // Verify the row IDs form a contiguous range [0, totalDocs)
            for (long i = 0; i < totalDocs; i++) {
                assertTrue(
                    "Missing ___row_id value: " + i + " (expected contiguous range 0.." + (totalDocs - 1) + ")",
                    allRowIds.contains(i)
                );
            }
        }
    }

    /**
     * Verifies sorted merge with age DESC (nulls first), name ASC (nulls last).
     */
    public void testSortedMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(sortedSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int refreshCycles = 15;
        indexDocsWithNullsAcrossRefreshes(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + refreshCycles + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < refreshCycles
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        verifyRowCount(snapshot, totalDocs);
        verifySortOrder(snapshot);
    }

    // ── Helpers ──

    private Settings singleEngineSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings dualEngineSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private Settings sortedSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    private int indexDocsAcrossMultipleRefreshes(int refreshCycles, int docsPerCycle) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("field_text", randomAlphaOfLength(10), "field_number", randomIntBetween(1, 1000))
                    .execute()
                    .actionGet(30_000);
                assertEquals(RestStatus.CREATED, response.status());
            }
            // Use execute().actionGet() with a timeout to avoid hanging indefinitely on refresh
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet(30_000);
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
        return refreshCycles;
    }

    private IndexShard getPrimaryShard() {
        String nodeName = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setStore(true).get();
        ShardStats shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];
        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    private MergeStats getMergeStats() {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setMerge(true).get();
        return statsResponse.getIndex(INDEX_NAME).getShards()[0].getStats().getMerge();
    }

    private void indexDocsWithNullsAcrossRefreshes(int refreshCycles, int docsPerCycle) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response;
                if (i % 5 == 0) {
                    response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(10)).get();
                } else {
                    response = client().prepareIndex()
                        .setIndex(INDEX_NAME)
                        .setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(0, 100))
                        .get();
                }
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
    }

    private void verifyRowCount(DataformatAwareCatalogSnapshot snapshot, int expectedTotalDocs) throws IOException {
        Path parquetDir = getParquetDir();
        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total rows across all segments should match ingested docs", expectedTotalDocs, totalRows);
    }

    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private void verifySortOrder(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }
                if (rows.size() <= 1) continue;

                for (int i = 1; i < rows.size(); i++) {
                    Object prevAge = rows.get(i - 1).get("age");
                    Object currAge = rows.get(i).get("age");

                    if (prevAge == null && currAge == null) continue;
                    if (prevAge == null) continue;
                    if (currAge == null) {
                        fail("age null should come before non-null, but found non-null at " + (i - 1) + " and null at " + i);
                    }

                    int prevAgeVal = ((Number) prevAge).intValue();
                    int currAgeVal = ((Number) currAge).intValue();

                    assertTrue(
                        "age should be DESC but found " + prevAgeVal + " before " + currAgeVal + " at row " + i,
                        prevAgeVal >= currAgeVal
                    );

                    if (prevAgeVal == currAgeVal) {
                        Object prevName = rows.get(i - 1).get("name");
                        Object currName = rows.get(i).get("name");

                        if (prevName != null && currName == null) continue;
                        if (prevName == null && currName != null) {
                            fail("name nulls should be last, but found null at " + (i - 1) + " and non-null at " + i);
                        }
                        if (prevName != null && currName != null) {
                            assertTrue(
                                "name should be ASC but found '" + prevName + "' before '" + currName + "' at row " + i,
                                ((String) prevName).compareTo((String) currName) <= 0
                            );
                        }
                    }
                }
            }
        }
    }

    private Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
    }
}
