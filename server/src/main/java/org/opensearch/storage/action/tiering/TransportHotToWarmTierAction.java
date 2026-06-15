/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.Index;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.opensearch.storage.common.tiering.TieringUtils.resolveRequestIndex;

/**
 * Transport Tiering action to move indices from hot to warm.
 * For DFA (pluggable data format) indices, this action:
 * 1. Adds a write block to prevent new writes
 * 2. Performs pre-tiering sync (flush + refresh + remote store sync) on all primary shards
 * 3. Proceeds with the tiering operation
 *
 * Non-DFA indices skip steps 1 and 2 and go directly to tiering.
 */
public class TransportHotToWarmTierAction extends TransportTierAction {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTierAction.class);
    private static final int MAX_PREPARE_RETRIES = 3;

    private final TransportPrepareTieringAction prepareTieringAction;
    private final HotToWarmTieringService hotToWarmTieringService;

    /**
     * Constructs a TransportHotToWarmTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param hotToWarmTieringService the hot to warm tiering service
     * @param prepareTieringAction the prepare tiering action for DFA indices
     */
    @Inject
    public TransportHotToWarmTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        HotToWarmTieringService hotToWarmTieringService,
        TransportPrepareTieringAction prepareTieringAction
    ) {
        super(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            HotToWarmTierAction.NAME,
            hotToWarmTieringService
        );
        this.prepareTieringAction = prepareTieringAction;
        this.hotToWarmTieringService = hotToWarmTieringService;
    }

    @Override
    protected void clusterManagerOperation(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        if (TieringUtils.isDfaIndex(state.metadata().index(request.getIndex()))) {
            // Validate FIRST — before any state-mutating or expensive operations.
            // If validation fails (e.g. warm nodes full, too many concurrent requests),
            // reject immediately without adding a write block or running prepare.
            // Note: validation also runs inside TieringService.tier() for double-safety.
            try {
                Index index = resolveRequestIndex(indexNameExpressionResolver, request.getIndex(), state);
                hotToWarmTieringService.preflightValidate(state, index);
            } catch (Exception e) {
                logger.info("Preflight validation failed for DFA index [{}]: {}", request.getIndex(), e.getMessage());
                listener.onFailure(e);
                return;
            }
            logger.info("Index [{}] is a DFA index, adding write block and performing pre-tiering sync", request.getIndex());
            addWriteBlockAndPrepare(request, state, listener);
        } else {
            super.clusterManagerOperation(request, state, listener);
        }
    }

    /**
     * Step 1: write-block the DFA index so no new writes land during prepare. Applies both the
     * {@code blocks.write} setting (persisted in index metadata, cleanly reverted on cancel/failure) and
     * the {@link IndexMetadata#INDEX_WRITE_BLOCK} cluster block (enforced immediately). On success,
     * proceeds to step 2 (prepare tiering).
     */
    private void addWriteBlockAndPrepare(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "add-write-block-for-tiering [" + request.getIndex() + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata indexMetadata = currentState.metadata().index(request.getIndex());
                    if (indexMetadata == null) {
                        throw new IllegalStateException("Index [" + request.getIndex() + "] not found");
                    }
                    // Block writes before pre-tiering sync: persist blocks.write (survives, revertible on
                    // cancel/failure) and apply the INDEX_WRITE_BLOCK cluster block (enforced immediately).
                    Settings.Builder indexSettingsBuilder = Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true);

                    IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                        .settings(indexSettingsBuilder)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion());

                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.addIndexBlock(request.getIndex(), IndexMetadata.INDEX_WRITE_BLOCK);

                    return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block added for index [{}], proceeding with pre-tiering sync", request.getIndex());
                    executePrepareTiering(request, newState, listener, 1);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> "Failed to add write block for index [" + request.getIndex() + "]", e);
                    listener.onFailure(
                        new IllegalStateException("Failed to add write block for DFA index [" + request.getIndex() + "]. Please retry.", e)
                    );
                }
            }
        );
    }

    /**
     * Step 2: Execute the prepare tiering action (flush + refresh + waitForRemoteStoreSync) on primary shards.
     * Retries up to MAX_PREPARE_RETRIES times on shard failures before giving up.
     * On success, proceeds to step 3 (tier).
     * On final failure, removes the write block to avoid leaving the index in a stuck, write-blocked state.
     */
    private void executePrepareTiering(
        IndexTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener,
        int attempt
    ) {
        PrepareTieringRequest prepareTieringRequest = new PrepareTieringRequest(request.getIndex());
        // Use the cluster setting for timeout instead of the short AcknowledgedRequest default (30s).
        // This controls both the transport channel timeout and the merge drain timeout on the data node.
        prepareTieringRequest.timeout(TieringUtils.PREPARE_TIERING_TIMEOUT.get(clusterService.getSettings()));

        prepareTieringAction.execute(prepareTieringRequest, new ActionListener<BroadcastResponse>() {
            @Override
            public void onResponse(BroadcastResponse broadcastResponse) {
                if (broadcastResponse.getFailedShards() > 0) {
                    if (attempt < MAX_PREPARE_RETRIES) {
                        logger.warn(
                            "Pre-tiering sync attempt [{}/{}] had {} failed shard(s) for index [{}], retrying",
                            attempt,
                            MAX_PREPARE_RETRIES,
                            broadcastResponse.getFailedShards(),
                            request.getIndex()
                        );
                        executePrepareTiering(request, state, listener, attempt + 1);
                        return;
                    }
                    // Build a targeted error message based on failure type. MergeDrainTimeoutException is
                    // detected by matching MERGE_DRAIN_TIMEOUT_MARKER in the (wire-preserved) message rather
                    // than by instanceof: the type is intentionally not registered for serialization, so the
                    // concrete class does not survive transport, but the message always does. Message-based
                    // detection therefore works in any mixed-version cluster with no versionAdded/registry id.
                    DefaultShardOperationFailedException[] failures = broadcastResponse.getShardFailures();
                    String mergeTimeoutSampleMessage = null;
                    int mergeTimeoutCount = 0;
                    int otherFailureCount = 0;

                    for (DefaultShardOperationFailedException f : failures) {
                        String timeoutMessage = findMergeDrainTimeoutMessage(f.getCause());
                        if (timeoutMessage != null) {
                            mergeTimeoutCount++;
                            if (mergeTimeoutSampleMessage == null) {
                                mergeTimeoutSampleMessage = timeoutMessage;
                            }
                        } else {
                            otherFailureCount++;
                        }
                    }

                    String errorMsg;
                    if (mergeTimeoutCount > 0 && otherFailureCount == 0) {
                        // All failures are merge drain timeouts — surface the per-shard detail (the
                        // sample's message already carries shard id, merge counts, and the timeout).
                        errorMsg = "Tiering preparation timed out: "
                            + mergeTimeoutCount
                            + " shard(s) still waiting for merges to drain after "
                            + MAX_PREPARE_RETRIES
                            + " attempts. Example: "
                            + mergeTimeoutSampleMessage;
                    } else if (mergeTimeoutCount > 0) {
                        // Mixed failures
                        errorMsg = "Pre-tiering sync failed for index ["
                            + request.getIndex()
                            + "] after "
                            + MAX_PREPARE_RETRIES
                            + " attempts: "
                            + mergeTimeoutCount
                            + " shard(s) timed out waiting for merges, "
                            + otherFailureCount
                            + " shard(s) failed for other reasons. "
                            + "Consider increasing cluster.tiering.prepare_timeout or retry later.";
                    } else {
                        // No merge timeouts — generic message with first failure details
                        String firstFailure = failures.length == 0
                            ? "unknown"
                            : (failures[0].getCause() != null ? failures[0].getCause().getMessage() : "unknown");
                        errorMsg = "Pre-tiering sync failed for index ["
                            + request.getIndex()
                            + "] after "
                            + MAX_PREPARE_RETRIES
                            + " attempts: "
                            + broadcastResponse.getFailedShards()
                            + " shard(s) failed. "
                            + "First failure: "
                            + firstFailure
                            + ". Please retry.";
                    }
                    logger.error(errorMsg);
                    removeWriteBlock(request.getIndex());
                    listener.onFailure(new IllegalStateException(errorMsg));
                    return;
                }
                logger.info("Pre-tiering sync completed for index [{}], proceeding with tiering", request.getIndex());
                try {
                    TransportHotToWarmTierAction.super.clusterManagerOperation(request, state, listener);
                } catch (Exception e) {
                    removeWriteBlock(request.getIndex());
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (attempt < MAX_PREPARE_RETRIES) {
                    logger.warn(
                        "Pre-tiering sync attempt [{}/{}] failed for index [{}], retrying: {}",
                        attempt,
                        MAX_PREPARE_RETRIES,
                        request.getIndex(),
                        e
                    );
                    executePrepareTiering(request, state, listener, attempt + 1);
                    return;
                }
                String errorMsg = "Pre-tiering sync failed for DFA index ["
                    + request.getIndex()
                    + "] after "
                    + MAX_PREPARE_RETRIES
                    + " attempts. Please retry.";
                logger.error(errorMsg, e);
                removeWriteBlock(request.getIndex());
                listener.onFailure(new IllegalStateException(errorMsg, e));
            }
        });
    }

    /**
     * Walks the cause chain looking for a merge-drain timeout, identified by the stable message
     * marker {@link MergeDrainTimeoutException#MERGE_DRAIN_TIMEOUT_MARKER}. Detection is message-based
     * (not {@code instanceof}) on purpose: the exception type is not registered for serialization, so
     * the concrete class does not survive transport, but its message always does. This keeps detection
     * working across mixed-version clusters with no version/registry coupling. Returns the matching
     * message, or {@code null} if no merge-drain timeout is present in the chain.
     */
    private static String findMergeDrainTimeoutMessage(Throwable t) {
        int depth = 0;
        while (t != null && depth++ < 10) {
            final String message = t.getMessage();
            if (message != null && message.contains(MergeDrainTimeoutException.MERGE_DRAIN_TIMEOUT_MARKER)) {
                return message;
            }
            t = t.getCause();
        }
        return null;
    }

    /**
     * Removes the write block from the index.
     * Called on prepare failure to avoid leaving the index in a stuck write-blocked state.
     * Best-effort — if this fails, the user can manually remove the block via index settings.
     */
    private void removeWriteBlock(String indexName) {
        clusterService.submitStateUpdateTask(
            "remove-write-block-for-tiering [" + indexName + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                    if (indexMetadata == null) {
                        return currentState;
                    }
                    Settings.Builder indexSettingsBuilder = Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false);

                    IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                        .settings(indexSettingsBuilder)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion());

                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);

                    return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> "Failed to remove write block for index ["
                            + indexName
                            + "] after tiering failure. The block can be removed manually via index settings.",
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block removed for index [{}] after tiering failure", indexName);
                }
            }
        );
    }
}
