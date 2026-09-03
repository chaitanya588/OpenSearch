/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * POC validation for server-side S3 copy between two independent repositories (Approach 2A).
 *
 * <p>These tests cover the copy mechanism only: that a CopyObject request is issued against the
 * <em>destination</em> repository's client, carrying the source repository's bucket/key/owner and the destination's
 * encryption settings, and that unsupported or rejected copies degrade to {@code false} so the caller falls back to
 * the regular streaming upload.
 */
public class S3ServerSideCopyTests extends OpenSearchTestCase {

    /** Source and destination in different buckets, each with its own client and bucket owner. */
    public void testCrossRepositoryCopyIssuesCopyObjectAgainstDestinationClient() throws Exception {
        final String sourceBucket = "source-segment-store-bucket";
        final String sourceOwner = "674014474418";
        final String destBucket = "dest-manual-snapshot-bucket";
        final String destOwner = "753498678932";

        // Source repository: only bucket/key/owner are read from it, never its client.
        final S3Client sourceClient = mock(S3Client.class);
        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn(sourceBucket);
        when(sourceBlobStore.expectedBucketOwner()).thenReturn(sourceOwner);
        when(sourceBlobStore.clientReference()).thenReturn(new AmazonS3Reference(sourceClient));
        final S3BlobContainer sourceContainer = new S3BlobContainer(
            new BlobPath().add("remote-store").add("index-uuid").add("0").add("segments").add("data"),
            sourceBlobStore
        );

        // Destination repository: its client signs the request.
        final S3Client destClient = mock(S3Client.class);
        when(destClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder().build());
        final S3BlobStore destBlobStore = mock(S3BlobStore.class);
        when(destBlobStore.bucket()).thenReturn(destBucket);
        when(destBlobStore.expectedBucketOwner()).thenReturn(destOwner);
        when(destBlobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());
        when(destBlobStore.clientReference()).thenReturn(new AmazonS3Reference(destClient));
        final S3BlobContainer destContainer = new S3BlobContainer(
            new BlobPath().add("snapshots").add("indices").add("index-id").add("0"),
            destBlobStore
        );

        final boolean copied = destContainer.tryServerSideCopy(sourceContainer, "_0.cfs__abc123", "__uuid-blob", 4096L);

        assertTrue("cross-repository server-side copy should succeed", copied);

        // The source repository's client must never be used.
        verify(sourceClient, never()).copyObject(any(CopyObjectRequest.class));

        final ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(destClient).copyObject(captor.capture());
        final CopyObjectRequest request = captor.getValue();

        assertEquals(sourceBucket, request.sourceBucket());
        assertEquals("remote-store/index-uuid/0/segments/data/_0.cfs__abc123", request.sourceKey());
        assertEquals(sourceOwner, request.expectedSourceBucketOwner());

        assertEquals(destBucket, request.destinationBucket());
        assertEquals("snapshots/indices/index-id/0/__uuid-blob", request.destinationKey());
        assertEquals(destOwner, request.expectedBucketOwner());

        // Destination-side encryption is applied, so the copy lands under the destination's configuration.
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    /** Destination SSE-KMS settings must be applied to the copy, independent of how the source was encrypted. */
    public void testCopyAppliesDestinationSseKmsSettings() throws Exception {
        final String destKmsKey = "arn:aws:kms:eu-west-1:753498678932:key/dest-key";

        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn("source-bucket");
        final S3BlobContainer sourceContainer = new S3BlobContainer(new BlobPath(), sourceBlobStore);

        final S3Client destClient = mock(S3Client.class);
        when(destClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder().build());
        final S3BlobStore destBlobStore = mock(S3BlobStore.class);
        when(destBlobStore.bucket()).thenReturn("dest-bucket");
        when(destBlobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(destBlobStore.serverSideEncryptionKmsKey()).thenReturn(destKmsKey);
        when(destBlobStore.serverSideEncryptionBucketKey()).thenReturn(true);
        when(destBlobStore.clientReference()).thenReturn(new AmazonS3Reference(destClient));
        final S3BlobContainer destContainer = new S3BlobContainer(new BlobPath(), destBlobStore);

        assertTrue(destContainer.tryServerSideCopy(sourceContainer, "src-blob", "dest-blob", 128L));

        final ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(destClient).copyObject(captor.capture());
        final CopyObjectRequest request = captor.getValue();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals(destKmsKey, request.ssekmsKeyId());
        assertEquals(Boolean.TRUE, request.bucketKeyEnabled());
    }

    /** AccessDenied (the cross-account case without a source bucket-policy grant) must fall back, not fail. */
    public void testAccessDeniedFallsBackInsteadOfThrowing() throws Exception {
        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn("source-bucket");
        final S3BlobContainer sourceContainer = new S3BlobContainer(new BlobPath(), sourceBlobStore);

        final S3Client destClient = mock(S3Client.class);
        when(destClient.copyObject(any(CopyObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );
        final S3BlobStore destBlobStore = mock(S3BlobStore.class);
        when(destBlobStore.bucket()).thenReturn("dest-bucket");
        when(destBlobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());
        when(destBlobStore.clientReference()).thenReturn(new AmazonS3Reference(destClient));
        final S3BlobContainer destContainer = new S3BlobContainer(new BlobPath(), destBlobStore);

        assertFalse(
            "AccessDenied must return false so the snapshot falls back to streaming",
            destContainer.tryServerSideCopy(sourceContainer, "src-blob", "dest-blob", 128L)
        );
    }

    /** A non-S3 source (different provider) is not copyable: return false without calling S3. */
    public void testNonS3SourceIsRejected() throws Exception {
        final S3Client destClient = mock(S3Client.class);
        final S3BlobStore destBlobStore = mock(S3BlobStore.class);
        when(destBlobStore.bucket()).thenReturn("dest-bucket");
        when(destBlobStore.clientReference()).thenReturn(new AmazonS3Reference(destClient));
        final S3BlobContainer destContainer = new S3BlobContainer(new BlobPath(), destBlobStore);

        final BlobContainer nonS3Source = mock(FsBlobContainer.class);

        assertFalse(destContainer.tryServerSideCopy(nonS3Source, "src-blob", "dest-blob", 128L));
        verify(destClient, never()).copyObject(any(CopyObjectRequest.class));
    }

    /** The default interface implementation must report "unsupported" so other providers keep streaming. */
    public void testDefaultImplementationReturnsFalse() throws Exception {
        final BlobContainer plainContainer = new BlobContainer() {
            @Override
            public BlobPath path() {
                return new BlobPath();
            }

            @Override
            public boolean blobExists(String blobName) {
                return false;
            }

            @Override
            public java.io.InputStream readBlob(String blobName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.io.InputStream readBlob(String blobName, long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeBlobAtomic(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
                throw new UnsupportedOperationException();
            }

            @Override
            public org.opensearch.common.blobstore.DeleteResult delete() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(java.util.List<String> blobNames) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobs() {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.Map<String, BlobContainer> children() {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
                throw new UnsupportedOperationException();
            }
        };

        assertFalse(plainContainer.tryServerSideCopy(plainContainer, "a", "b", 1L));
    }
}
