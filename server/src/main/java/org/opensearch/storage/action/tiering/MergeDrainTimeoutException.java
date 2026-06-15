/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception thrown when a shard's merge drain times out during tiering preparation.
 * <p>
 * This is intentionally a plain {@link IOException} that is <b>not</b> registered with the
 * {@code OpenSearchException} serialization registry. Detection on the coordinator is done by
 * matching {@link #MERGE_DRAIN_TIMEOUT_MARKER} in the exception message rather than by
 * {@code instanceof}. The message is always preserved across the wire (an unregistered exception
 * is serialized as a {@code NotSerializableExceptionWrapper}), so detection works in any
 * mixed-version cluster without needing a {@code versionAdded} guard or a registry id — which
 * keeps it safe to backport into an already-released line.
 *
 * @opensearch.internal
 */
public class MergeDrainTimeoutException extends IOException {

    /**
     * Stable marker substring embedded in every merge-drain-timeout message. The throw site builds
     * the message with this marker and the coordinator detects the timeout by searching for it in
     * the (wire-preserved) message. Changing this string is a wire-compatibility concern — keep it
     * stable across versions.
     */
    public static final String MERGE_DRAIN_TIMEOUT_MARKER = "timed out waiting for merges to drain";

    public MergeDrainTimeoutException(ShardId shardId, int activeMerges, int pendingMerges, String timeoutValue) {
        super(buildMessage(shardId, activeMerges, pendingMerges, timeoutValue));
    }

    private static String buildMessage(ShardId shardId, int activeMerges, int pendingMerges, String timeoutValue) {
        return "Shard ["
            + shardId
            + "] "
            + MERGE_DRAIN_TIMEOUT_MARKER
            + ". Active merges: "
            + activeMerges
            + ", pending merges: "
            + pendingMerges
            + ". "
            + "Consider increasing cluster.tiering.prepare_timeout (current: "
            + timeoutValue
            + ") "
            + "or wait for merges to complete before retrying.";
    }
}
