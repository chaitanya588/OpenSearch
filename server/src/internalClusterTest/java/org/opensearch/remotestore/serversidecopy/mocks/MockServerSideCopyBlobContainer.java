/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.serversidecopy.mocks;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A filesystem blob container that supports "server-side" copy by copying the file directly between two container
 * directories, standing in for S3 {@code CopyObject}. Bytes never pass through {@link #writeBlob}, which lets tests
 * assert that the snapshot took the copy path rather than the streaming upload path.
 */
public class MockServerSideCopyBlobContainer extends FsBlobContainer {

    /** Number of blobs copied store-side across all containers in this JVM. */
    public static final AtomicInteger SERVER_SIDE_COPY_COUNT = new AtomicInteger();

    /** Number of data blobs ({@code __*}) written through the streaming upload path across all containers. */
    public static final AtomicInteger STREAMED_DATA_BLOB_COUNT = new AtomicInteger();

    public static void resetCounters() {
        SERVER_SIDE_COPY_COUNT.set(0);
        STREAMED_DATA_BLOB_COUNT.set(0);
    }

    public MockServerSideCopyBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobStore, blobPath, path);
    }

    @Override
    public boolean tryServerSideCopy(BlobContainer sourceContainer, String sourceBlobName, String destBlobName, long blobSize)
        throws IOException {
        if (sourceContainer instanceof MockServerSideCopyBlobContainer == false) {
            return false;
        }
        final Path sourceFile = ((MockServerSideCopyBlobContainer) sourceContainer).path.resolve(sourceBlobName);
        if (Files.exists(sourceFile) == false) {
            return false;
        }
        Files.createDirectories(path);
        Files.copy(sourceFile, path.resolve(destBlobName), StandardCopyOption.REPLACE_EXISTING);
        SERVER_SIDE_COPY_COUNT.incrementAndGet();
        return true;
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        countIfDataBlob(blobName);
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        Map<String, String> metadata
    ) throws IOException {
        countIfDataBlob(blobName);
        super.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
    }

    private static void countIfDataBlob(String blobName) {
        // Snapshot segment data blobs are named "__<uuid>"; metadata blobs use readable prefixes such as
        // "index-", "snap-" and "meta-".
        if (Strings.isNullOrEmpty(blobName) == false && blobName.startsWith("__")) {
            STREAMED_DATA_BLOB_COUNT.incrementAndGet();
        }
    }
}
