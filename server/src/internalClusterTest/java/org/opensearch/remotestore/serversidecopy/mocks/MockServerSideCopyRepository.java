/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.serversidecopy.mocks;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.repositories.fs.ReloadableFsRepository;

/**
 * Filesystem repository whose containers support store-side copy, standing in for an S3 repository that can serve
 * {@code CopyObject}. Used as both the remote (segment) store and the snapshot repository so that a shard snapshot can
 * copy segment blobs straight across.
 */
public class MockServerSideCopyRepository extends ReloadableFsRepository {

    public MockServerSideCopyRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        this(metadata, environment, namedXContentRegistry, clusterService, recoverySettings, null);
    }

    public MockServerSideCopyRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        NativeRemoteObjectStoreProvider nativeStoreProvider
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings, nativeStoreProvider);
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final FsBlobStore fsBlobStore = (FsBlobStore) super.createBlobStore();
        return new MockServerSideCopyBlobStore(fsBlobStore.bufferSizeInBytes(), fsBlobStore.path(), isReadOnly());
    }
}
