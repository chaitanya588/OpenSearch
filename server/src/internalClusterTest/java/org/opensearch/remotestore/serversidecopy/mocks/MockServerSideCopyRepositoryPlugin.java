/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.serversidecopy.mocks;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.util.Collections;
import java.util.Map;

/**
 * Registers the {@code fs_server_side_copy} repository type.
 */
public class MockServerSideCopyRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String TYPE = "fs_server_side_copy";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            TYPE,
            metadata -> new MockServerSideCopyRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );
    }
}
