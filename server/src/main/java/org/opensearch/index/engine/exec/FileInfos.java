/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.opensearch.index.engine.exec.merge.RowIdMapping;

public final class FileInfos {

    private final Map<DataFormat, WriterFileSet> writerFilesMap;
    private RowIdMapping sortPermutation;

    private FileInfos() {
        this.writerFilesMap = new HashMap<>();
    }

    public Map<DataFormat, WriterFileSet> getWriterFilesMap() {
        return Collections.unmodifiableMap(writerFilesMap);
    }

    private void putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
        writerFilesMap.put(format, writerFileSet);
    }

    public Optional<WriterFileSet> getWriterFileSet(DataFormat format) {
        return Optional.ofNullable(writerFilesMap.get(format));
    }

    /**
     * Returns the sort permutation produced during sort-on-close, or null
     * if no sorting was configured or the file was empty.
     */
    public RowIdMapping getSortPermutation() {
        return sortPermutation;
    }

    private void setSortPermutation(RowIdMapping sortPermutation) {
        this.sortPermutation = sortPermutation;
    }

    public static FileInfos empty() {
        return new FileInfos();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<DataFormat, WriterFileSet> writerFilesMap = new HashMap<>();
        private RowIdMapping sortPermutation;

        public Builder putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
            writerFilesMap.put(format, writerFileSet);
            return this;
        }

        public Builder putAll(Map<DataFormat, WriterFileSet> map) {
            writerFilesMap.putAll(map);
            return this;
        }

        public Builder sortPermutation(RowIdMapping sortPermutation) {
            this.sortPermutation = sortPermutation;
            return this;
        }

        public FileInfos build() {
            FileInfos fileInfos = new FileInfos();
            writerFilesMap.forEach(fileInfos::putWriterFileSet);
            fileInfos.setSortPermutation(sortPermutation);
            return fileInfos;
        }
    }
}
