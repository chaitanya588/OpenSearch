/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.index.IndexService;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

public class AutoForceMergeManager {

    private final ThreadPool threadPool;
    private final OsService osService;
    private final IndicesService indicesService;
    private final TimeValue interval;
    private final AsyncForceMergeTask task;
    private final Integer optimalMaxSegmentsCount;

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    public AutoForceMergeManager(ThreadPool threadPool, OsService osService,
                                 IndicesService indicesService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.interval = TimeValue.timeValueMinutes(1); // 30 minutes interval
        task = new AsyncForceMergeTask();
        optimalMaxSegmentsCount = 1;
    }

    private void triggerForceMerge() {
        if (!(new ConfigurationValidator().validate().isAllowed())) {
            logger.info("Domain configuration is not meeting the criteria");
            return;
        }
        if (!(new NodeValidator().validate().isAllowed())) {
            logger.info("Node capacity constraints are not allowing to trigger auto ForceMerge");
            return;
        }
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary()) {
                    if (new ShardValidator(shard).validate().isAllowed()) {
                        ForceMergeRequest localForceMergeRequest = new ForceMergeRequest()
                            .maxNumSegments(optimalMaxSegmentsCount);
                        try {
                            shard.forceMerge(localForceMergeRequest);
                        } catch (IOException exception) {
                            logger.error("Consuming Shard Level ForceMerge action" + exception);
                        }
                    }
                }
            }
        }
    }

    //Configuration based validator such as tiering enabled domain
    private class ConfigurationValidator implements ValidationStrategy {
        @Override
        public ValidationResult validate() {
            return new ValidationResult(true);
        }
    }


    //Node Capacity validator
    private class NodeValidator implements ValidationStrategy {
        @Override
        public ValidationResult validate() {
            collectOsStats();
            return new ValidationResult(true);
        }
    }

    //Shard validator
    private class ShardValidator implements ValidationStrategy {
        IndexShard indexShard;
        public ShardValidator(final IndexShard indexShard) {
            this.indexShard = indexShard;
        }

        @Override
        public ValidationResult validate() {
            collectIndexAndShardStats(this.indexShard);
            return new ValidationResult(true);
        }
    }

    private void collectOsStats() {
        double cpuPercent = osService.stats().getCpu().getPercent();
        double loadAverage1m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage5m = osService.stats().getCpu().getLoadAverage()[0];
        double loadAverage15m = osService.stats().getCpu().getLoadAverage()[0];
        // CPU Metrics
        logger.info("CPU Metrics - CPU Percent: {}%, Load Averages: 1m: {}, 5m: {}, 15m: {}",
            cpuPercent, loadAverage1m, loadAverage5m, loadAverage15m);

        long totalMemory = osService.stats().getMem().getTotal().getBytes();
        long freeMemory = osService.stats().getMem().getFree().getBytes();
        long usedMemory = osService.stats().getMem().getUsed().getBytes();
        short freeMemoryPercent = osService.stats().getMem().getFreePercent();
        short usedMemoryPercent = osService.stats().getMem().getUsedPercent();

        // Memory Metrics
        logger.info("Memory Metrics - Total Memory: {}%, Free Memory: {}, Used Memory: {}, Free Memory Percent: {}, Used Memory Percent: {}",
            totalMemory, freeMemory, usedMemory, freeMemoryPercent, usedMemoryPercent);
    }

    private void collectIndexAndShardStats(final IndexShard shard) {
        // Get shard stats
        CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Merge, CommonStatsFlags.Flag.Translog, CommonStatsFlags.Flag.Refresh, CommonStatsFlags.Flag.Segments);
        CommonStats commonStats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);

        // Merge stats
        MergeStats mergeStats = commonStats.getMerge();
        long totalMerges = mergeStats.getTotal();
        long totalMergeTime = mergeStats.getTotalTimeInMillis();

        logger.info("Shard Merge Stats for the shard {} - Total Merges: {}, Total Merge Time: {} ms",
            shard.shardId().id(), totalMerges, totalMergeTime);

        // Translog stats
        TranslogStats translogStats = commonStats.getTranslog();
        long translogAge = translogStats.getEarliestLastModifiedAge();
        long translogSize = translogStats.getUncommittedSizeInBytes();

        logger.info("Translog Stats for Primary Shard {} - translogAge: {}",
            shard.shardId(), translogAge);
    }

    private final class AsyncForceMergeTask extends AbstractAsyncTask {
        public AsyncForceMergeTask() {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            logger.info("Not running intentionally now");
            //triggerForceMerge();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FORCE_MERGE;
        }
    }

    private interface ValidationStrategy {
        ValidationResult validate();
    }

    private static final class ValidationResult {
        private final boolean allowed;
        public ValidationResult(boolean allowed){
            this.allowed = allowed;
        }
        public boolean isAllowed() {
            return allowed;
        }
    }
}
