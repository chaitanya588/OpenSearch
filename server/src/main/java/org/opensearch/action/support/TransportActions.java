/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.ShardNotFoundException;

/**
 * Utility class for transport actions
 *
 * @opensearch.internal
 */
public class TransportActions {

    public static boolean isShardNotAvailableException(final Throwable e) {
        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        return (actual instanceof ShardNotFoundException
            || actual instanceof IndexNotFoundException
            || actual instanceof IllegalIndexShardStateException
            || actual instanceof NoShardAvailableActionException
            || actual instanceof UnavailableShardsException
            || actual instanceof AlreadyClosedException);
    }

    /**
     * If a failure is already present, should this failure override it or not for read operations.
     */
    public static boolean isReadOverrideException(Exception e) {
        return !isShardNotAvailableException(e);
    }

    public static boolean isRetryableSearchException(final Exception e) {
        return (OpenSearchException.status(e).getStatusFamilyCode() != 4) && (e.getCause() instanceof TaskCancelledException == false)
            // There exists a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the
            // target node but already deleted on the source node. Search request should still work.
            || (e.getCause() instanceof IndexNotFoundException)
            || (e.getCause() instanceof OpenSearchRejectedExecutionException);
    }

}
