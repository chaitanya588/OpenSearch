/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.CommitterFactory;

import java.io.IOException;

/**
 * {@link CommitterFactory} implementation that creates {@link LuceneCommitter} instances.
 * <p>
 * Registered by {@link org.opensearch.be.lucene.LucenePlugin} via the
 * {@link org.opensearch.plugins.EnginePlugin} SPI. When the composite engine initializes
 * a shard, it calls {@link #getCommitter(CommitterConfig)} to obtain a committer that owns
 * the shared Lucene {@link org.apache.lucene.index.IndexWriter} for durable segment commits.
 * <p>
 * Determines whether Lucene is a secondary format by checking the
 * {@code index.composite.primary_data_format} setting. If the primary format is not
 * {@code "lucene"}, then Lucene is secondary and IndexSort is not applied on the shared writer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneCommitterFactory implements CommitterFactory {

    /** The index setting key for the primary data format in composite mode. */
    private static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";

    /** Creates a new factory instance. */
    public LuceneCommitterFactory() {}

    /**
     * Creates a new {@link LuceneCommitter} for the given settings.
     * <p>
     * If the index's primary data format is not "lucene", the committer is created
     * in secondary mode (no IndexSort on the shared writer).
     *
     * @param committerConfig the committer config
     * @return a new committer
     * @throws IOException if committer initialization fails
     */
    public Committer getCommitter(CommitterConfig committerConfig) throws IOException {
        return new LuceneCommitter(committerConfig);
    }
}
