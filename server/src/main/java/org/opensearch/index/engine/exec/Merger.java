/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.util.List;

public interface Merger {
    /**
     * @param fileMetadataList List of FileMetadata to merge
     * @return MergeResult - having RowIdMapping and mergedFileMetadata
     */
    MergeResult merge(List<WriterFileSet> fileMetadataList, long writerGeneration);

    /**
     * @param fileMetadataList List of FileMetadata to merge
     * @param rowIdMapping Mapping of old segment + old rowId to new rowId
     * @return MergeResult - having mergedFileMetadata
     */
    MergeResult merge(List<WriterFileSet> fileMetadataList, RowIdMapping rowIdMapping, long writerGeneration);

    /**
     * Merge with full input including sort configuration.
     *
     * @param mergeInput encapsulates file list, writer generation, and sort config
     * @return MergeResult - having RowIdMapping and mergedFileMetadata
     */
    default MergeResult merge(MergeInput mergeInput) {
        return merge(mergeInput.getFileMetadataList(), mergeInput.getWriterGeneration());
    }

    /**
     * Merge with full input and an existing RowIdMapping (for secondary data formats).
     *
     * @param mergeInput encapsulates file list, writer generation, and sort config
     * @param rowIdMapping Mapping of old segment + old rowId to new rowId
     * @return MergeResult - having mergedFileMetadata
     */
    default MergeResult merge(MergeInput mergeInput, RowIdMapping rowIdMapping) {
        return merge(mergeInput.getFileMetadataList(), rowIdMapping, mergeInput.getWriterGeneration());
    }
}
