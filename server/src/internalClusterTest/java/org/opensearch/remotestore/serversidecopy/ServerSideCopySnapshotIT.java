/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.serversidecopy;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.serversidecopy.mocks.MockServerSideCopyBlobContainer;
import org.opensearch.remotestore.serversidecopy.mocks.MockServerSideCopyRepositoryPlugin;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end validation of Approach 2A: on a remote-store enabled domain, a normal (full-copy) snapshot populates the
 * snapshot repository by copying segment blobs store-side out of the segment store, instead of re-reading them from the
 * node and uploading them again.
 * <p>
 * Both the segment store and the snapshot repository use a filesystem repository whose blob containers implement
 * {@code tryServerSideCopy}, standing in for two S3 repositories that can serve {@code CopyObject}. They point at
 * different locations, so the copy genuinely crosses repositories.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerSideCopySnapshotIT extends AbstractSnapshotIntegTestCase {

    // The "__rs" suffix marks these as remote store system repositories, which the base class excludes from its
    // end-of-test snapshot repository consistency checks.
    private static final String SEGMENT_REPO = "test-segment-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private static final String TRANSLOG_REPO = "test-translog-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;
    private static final String SNAPSHOT_REPO = "test-snapshot-repo";
    private static final String INDEX_NAME = "test-idx";

    private Path segmentRepoPath;
    private Path translogRepoPath;
    private Path snapshotRepoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockServerSideCopyRepositoryPlugin.class))
            .collect(Collectors.toList());
    }

    @Before
    public void setupRepositories() {
        segmentRepoPath = randomRepoPath().toAbsolutePath();
        translogRepoPath = randomRepoPath().toAbsolutePath();
        snapshotRepoPath = randomRepoPath().toAbsolutePath();
        MockServerSideCopyBlobContainer.resetCounters();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    SEGMENT_REPO,
                    segmentRepoPath,
                    MockServerSideCopyRepositoryPlugin.TYPE,
                    TRANSLOG_REPO,
                    translogRepoPath,
                    MockServerSideCopyRepositoryPlugin.TYPE
                )
            )
            .build();
    }

    public void testFullCopySnapshotCopiesSegmentsServerSide() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        // The snapshot repository is a separate location from the segment store, so the copy crosses repositories.
        createRepository(
            SNAPSHOT_REPO,
            MockServerSideCopyRepositoryPlugin.TYPE,
            Settings.builder().put("location", snapshotRepoPath).put("compress", false)
        );

        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    // Remote store is enabled via the node attributes above; segment replication is a prerequisite.
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            )
        );
        ensureGreen(INDEX_NAME);

        final int numDocs = 50;
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        // Flush and force merge so the segment topology is stable and every segment is committed before snapshotting.
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        assertNoFailures(client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get());
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        waitForSegmentUploadsToCatchUp();

        MockServerSideCopyBlobContainer.resetCounters();

        final CreateSnapshotResponse createResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPO, "snap-1")
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        assertEquals(SnapshotState.SUCCESS, createResponse.getSnapshotInfo().state());
        assertEquals(0, createResponse.getSnapshotInfo().failedShards());

        // The snapshot's segment data must have arrived via store-side copy, with nothing streamed through the node.
        assertThat(
            "expected at least one segment blob to be copied store-side",
            MockServerSideCopyBlobContainer.SERVER_SIDE_COPY_COUNT.get(),
            greaterThan(0)
        );
        assertThat(
            "no segment data blob should have been streamed from the node",
            MockServerSideCopyBlobContainer.STREAMED_DATA_BLOB_COUNT.get(),
            equalTo(0)
        );

        // The copied snapshot must still restore to a correct, searchable index.
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
        final RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPO, "snap-1")
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());
        ensureGreen(INDEX_NAME);
        assertEquals(numDocs, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
        for (int i = 0; i < numDocs; i++) {
            assertTrue("doc " + i + " missing after restore", client().prepareGet(INDEX_NAME, Integer.toString(i)).get().isExists());
        }
    }

    /** Polls the shard's remote store stats until nothing is left to upload, so the snapshot sees a complete segment store. */
    private void waitForSegmentUploadsToCatchUp() throws Exception {
        assertBusy(() -> {
            final RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
            final RemoteSegmentTransferTracker.Stats stats = Arrays.stream(response.getRemoteStoreStats())
                .filter(stat -> stat.getShardRouting().primary())
                .map(RemoteStoreStats::getSegmentStats)
                .findFirst()
                .orElseThrow(AssertionError::new);
            assertEquals("segment upload failures", 0L, stats.totalUploadsFailed);
            assertEquals("segments still pending upload", 0L, stats.bytesLag);
            assertEquals("local and remote refresh out of sync", stats.localRefreshNumber, stats.remoteRefreshNumber);
        });
    }
}
