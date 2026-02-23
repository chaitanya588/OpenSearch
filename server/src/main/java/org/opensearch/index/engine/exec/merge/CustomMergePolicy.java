/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.Map;

public class CustomMergePolicy extends MergePolicy {

    private final RowIdMapping rowIdMapping;

    public CustomMergePolicy(RowIdMapping rowIdMapping) {
        this.rowIdMapping = rowIdMapping;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        MergeSpecification spec = new MergeSpecification();
        spec.add(new CustomOneMerge(segmentInfos.asList(), rowIdMapping));
        return spec;
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        MergeSpecification spec = new MergeSpecification();
        spec.add(new CustomOneMerge(segmentInfos.asList(), rowIdMapping));
        return spec;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
        return null;
    }
}
