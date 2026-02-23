/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class CustomIndexWriter extends IndexWriter {

    public CustomIndexWriter(Directory directory, IndexWriterConfig config) throws IOException {
        super(directory, config);
    }

    public void executeMerge(MergePolicy.OneMerge merge) throws IOException {
        merge(merge);
    }
}
