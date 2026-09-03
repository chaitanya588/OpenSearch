/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * POC contract tests for {@link BlobContainer#tryServerSideCopy} (Approach 2A).
 *
 * <p>Validates the fallback contract that the snapshot path depends on: providers that cannot perform a store-side copy
 * must report {@code false} rather than throwing, and a provider that can must be able to copy between two independent
 * containers (different stores / buckets / credentials) without the bytes passing through the caller.
 */
public class ServerSideCopyContractTests extends OpenSearchTestCase {

    /** Default implementation: unsupported everywhere, so existing providers keep using the streaming path. */
    public void testDefaultImplementationReportsUnsupported() throws Exception {
        final Path root = createTempDir();
        try (FsBlobStore store = new FsBlobStore(randomIntBetween(1, 8) * 1024, root, false)) {
            final BlobContainer container = store.blobContainer(BlobPath.cleanPath().add("shard-0"));
            assertFalse(
                "filesystem provider must report server-side copy as unsupported",
                container.tryServerSideCopy(container, "source-blob", "dest-blob", 42L)
            );
        }
    }

    /**
     * A provider that supports store-side copy moves the blob between two independent containers without the caller
     * reading or writing the bytes. Mirrors what S3BlobContainer does with CopyObject.
     */
    public void testSupportingProviderCopiesBetweenIndependentContainers() throws Exception {
        final byte[] payload = "segment-file-contents".getBytes(StandardCharsets.UTF_8);
        final AtomicReference<String> copyRecord = new AtomicReference<>();

        // Two separate "stores", as a segment repository and a manual snapshot repository would be.
        final RecordingCopyContainer source = new RecordingCopyContainer("source-bucket", BlobPath.cleanPath().add("segments"), null);
        source.blobs.put("_0.cfs__abc", payload);
        final RecordingCopyContainer dest = new RecordingCopyContainer("dest-bucket", BlobPath.cleanPath().add("snapshots"), copyRecord);

        assertTrue(dest.tryServerSideCopy(source, "_0.cfs__abc", "__uuid-blob", payload.length));

        // The copy was issued by the destination, naming the source's bucket and key.
        assertEquals("source-bucket/segments/_0.cfs__abc -> dest-bucket/snapshots/__uuid-blob", copyRecord.get());
        assertArrayEquals(payload, dest.blobs.get("__uuid-blob"));

        // Neither side streamed the bytes through the caller.
        assertEquals(0, source.readBlobCalls);
        assertEquals(0, dest.writeBlobCalls);
    }

    /** A rejected copy (e.g. cross-account AccessDenied) must degrade to false so the caller falls back. */
    public void testRejectedCopyFallsBack() throws Exception {
        final RecordingCopyContainer source = new RecordingCopyContainer("source-bucket", BlobPath.cleanPath(), null);
        final RecordingCopyContainer dest = new RecordingCopyContainer("dest-bucket", BlobPath.cleanPath(), null);
        dest.rejectCopies = true;

        assertFalse(dest.tryServerSideCopy(source, "src", "dst", 10L));
        assertEquals(0, dest.writeBlobCalls);
    }

    /** Container that implements store-side copy by moving bytes internally, never through the caller. */
    private static class RecordingCopyContainer implements BlobContainer {
        private final String bucket;
        private final BlobPath path;
        private final AtomicReference<String> copyRecord;
        final Map<String, byte[]> blobs = new java.util.HashMap<>();
        int readBlobCalls = 0;
        int writeBlobCalls = 0;
        boolean rejectCopies = false;

        RecordingCopyContainer(String bucket, BlobPath path, AtomicReference<String> copyRecord) {
            this.bucket = bucket;
            this.path = path;
            this.copyRecord = copyRecord;
        }

        private String key(String blobName) {
            return bucket + "/" + path.buildAsString() + blobName;
        }

        @Override
        public boolean tryServerSideCopy(BlobContainer sourceContainer, String sourceBlobName, String destBlobName, long blobSize) {
            if (sourceContainer instanceof RecordingCopyContainer == false) {
                return false;
            }
            if (rejectCopies) {
                return false;
            }
            final RecordingCopyContainer src = (RecordingCopyContainer) sourceContainer;
            final byte[] data = src.blobs.get(sourceBlobName);
            if (data == null) {
                return false;
            }
            blobs.put(destBlobName, data);
            if (copyRecord != null) {
                copyRecord.set(src.key(sourceBlobName) + " -> " + key(destBlobName));
            }
            return true;
        }

        @Override
        public BlobPath path() {
            return path;
        }

        @Override
        public boolean blobExists(String blobName) {
            return blobs.containsKey(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            readBlobCalls++;
            final byte[] data = blobs.get(blobName);
            if (data == null) {
                throw new IOException("missing blob " + blobName);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            readBlobCalls++;
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            writeBlobCalls++;
            blobs.put(blobName, inputStream.readAllBytes());
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(java.util.List<String> blobNames) {
            blobNames.forEach(blobs::remove);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, BlobContainer> children() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            throw new UnsupportedOperationException();
        }
    }
}
