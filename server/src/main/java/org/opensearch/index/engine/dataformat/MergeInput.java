/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * input data for a merge operation.
 * Use {@link Builder} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergeInput(List<Segment> segments, Map<Long, RowIdMapping> rowIdMappings, long newWriterGeneration) {

    public MergeInput {
        segments = List.copyOf(segments);
        rowIdMappings = rowIdMappings != null ? Collections.unmodifiableMap(new HashMap<>(rowIdMappings)) : null;
    }

    private MergeInput(Builder builder) {
        this(new ArrayList<>(builder.segments), builder.rowIdMappings, builder.newWriterGeneration);
    }

    /**
     * Returns the {@link WriterFileSet} for the given data format from each segment.
     *
     * @param formatName the data format name (e.g. "parquet")
     * @return list of writer file sets for the format across all segments
     */
    public List<WriterFileSet> getFilesForFormat(String formatName) {
        return segments.stream().map(seg -> seg.dfGroupedSearchableFiles().get(formatName)).filter(Objects::nonNull).toList();
    }

    /**
     * Returns whether row ID mappings are available.
     */
    public boolean hasRowIdMappings() {
        return rowIdMappings != null && rowIdMappings.isEmpty() == false;
    }

    /**
     * Returns the row ID mapping for a specific generation, or null if not found.
     *
     * @param generation the writer generation
     * @return the row ID mapping for that generation, or null
     */
    public RowIdMapping getRowIdMapping(long generation) {
        return rowIdMappings != null ? rowIdMappings.get(generation) : null;
    }

    /**
     * Returns a new builder for constructing {@link MergeInput} instances.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MergeInput}.
     */
    @ExperimentalApi
    public static class Builder {
        private List<Segment> segments = new ArrayList<>();
        private Map<Long, RowIdMapping> rowIdMappings;
        private long newWriterGeneration;

        private Builder() {}

        /**
         * Sets the list of segments to merge.
         *
         * @param segments the segments to merge
         * @return this builder
         */
        public Builder segments(List<Segment> segments) {
            this.segments = new ArrayList<>(segments);
            return this;
        }

        /**
         * Adds a segment to merge.
         *
         * @param segment the segment to add
         * @return this builder
         */
        public Builder addSegment(Segment segment) {
            this.segments.add(segment);
            return this;
        }

        /**
         * Sets the row ID mappings for secondary data format merges, keyed by writer generation.
         *
         * @param rowIdMappings the row ID mappings per generation
         * @return this builder
         */
        public Builder rowIdMappings(Map<Long, RowIdMapping> rowIdMappings) {
            this.rowIdMappings = rowIdMappings;
            return this;
        }

        /**
         * Sets the writer generation for the merged output.
         *
         * @param newWriterGeneration the new writer generation
         * @return this builder
         */
        public Builder newWriterGeneration(long newWriterGeneration) {
            this.newWriterGeneration = newWriterGeneration;
            return this;
        }

        /**
         * Builds an immutable {@link MergeInput}.
         *
         * @return the constructed MergeInput
         */
        public MergeInput build() {
            return new MergeInput(this);
        }
    }
}
