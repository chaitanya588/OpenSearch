/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.exec.coord.Segment;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.MergeInput;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;

public abstract class MergeHandler {

    private final Any compositeDataFormat;

    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;

    private CompositeEngine compositeEngine;
    private Map<DataFormat, Merger> dataFormatMergerMap;
    private final Deque<OneMerge> mergingSegments = new ArrayDeque<>();
    private final Set<Segment> currentlyMergingSegments = new HashSet<>();
    private final Logger logger;
    private final ShardId shardId;
    private final String sortColumn;
    private final boolean reverseSort;

//        public static void main(String[] args) throws Exception {
//            String indexPath = "/Volumes/workplace/OpenSource/MustangOpenSearchDevelopment/OpenSearch/build/" +
//                "testclusters/runTask-0/data/nodes/0/indices/2OMQ7arMSUOaxV5kIU4Y2A/0/index/";
//
//            try (Directory dir = FSDirectory.open(Paths.get(indexPath));
//                 DirectoryReader reader = DirectoryReader.open(dir)) {
//
//                System.out.println("Total docs: " + reader.numDocs());
//
//                // Print index schema
//                System.out.println("\n=== Index Schema ===");
//                for (LeafReaderContext context : reader.leaves()) {
//                    for (FieldInfo fieldInfo : context.reader().getFieldInfos()) {
//                        System.out.println("Field: " + fieldInfo.name +
//                            ", DocValues: " + fieldInfo.getDocValuesType() +
//                            ", IndexOptions: " + fieldInfo.getIndexOptions());
//                    }
//                    break; // Only need one segment
//                }
//                System.out.println("===================\n");
//
//                int segmentIdx = 0;
//                for (LeafReaderContext context : reader.leaves()) {
//                    LeafReader leafReader = context.reader();
//                    StoredFields storedFields = leafReader.storedFields();
//                    System.out.println("\n=== Segment " + segmentIdx + " (" + leafReader + ") ===");
//
//                    // Debug: Check which fields have DocValues in this segment
//                    System.out.println("Fields with DocValues in this segment:");
//                    for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
//                        if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
//                            if (fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
//                                NumericDocValues dv = leafReader.getNumericDocValues(fieldInfo.name);
//                                System.out.println("  " + fieldInfo.name + ": " + fieldInfo.getDocValuesType() + " (dv=" + (dv != null ? "present" : "NULL") + ")");
//                            } else {
//                                System.out.println("  " + fieldInfo.name + ": " + fieldInfo.getDocValuesType());
//                            }
//                        }
//                    }
//
//                    for (int i = 0; i < leafReader.maxDoc(); i++) {
//                        if (leafReader.getLiveDocs() != null && !leafReader.getLiveDocs().get(i)) continue;
//
//                        System.out.println("\nDoc " + (context.docBase + i) + " (local: " + i + "):");
//
//                        Document doc = storedFields.document(i);
//
//                        // Print ALL stored fields
//                        System.out.println("  Stored fields in document:");
//                        for (IndexableField field : doc.getFields()) {
//                            System.out.println("    " + field.name() + ": " + field.stringValue() + " (binary: " + (field.binaryValue() != null) + ")");
//                        }
//
//                        // Decode _id
//                        IndexableField idField = doc.getField("_id");
//                        if (idField != null && idField.binaryValue() != null) {
//                            org.apache.lucene.util.BytesRef bytesRef = idField.binaryValue();
//                            String id = org.opensearch.index.mapper.Uid.decodeId(bytesRef.bytes, bytesRef.offset, bytesRef.length);
//                            System.out.println("  _id: " + id);
//                        }
//
//                        // Read all fields from DocValues
//                        for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
//                            String fieldName = fieldInfo.name;
//                            if (fieldName.startsWith("_")) continue;
//
//                            if (fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
//                                SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(fieldName);
//                                if (dv != null && dv.advanceExact(i)) {
//                                    long value = dv.nextValue();
//                                    if (fieldName.equals("score")) {
//                                        System.out.println("  " + fieldName + " (DV): " + Double.longBitsToDouble(value));
//                                    } else if (fieldName.equals("active")) {
//                                        System.out.println("  " + fieldName + " (DV): " + (value == 1));
//                                    } else if (fieldName.equals("created_date")) {
//                                        System.out.println("  " + fieldName + " (DV): " + value + " (epoch millis)");
//                                    } else {
//                                        System.out.println("  " + fieldName + " (DV): " + value);
//                                    }
//                                }
//                            } else if (fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
//                                SortedSetDocValues dv = leafReader.getSortedSetDocValues(fieldName);
//                                if (dv != null && dv.advanceExact(i)) {
//                                    long ord = dv.nextOrd();
//                                    if (ord != -1) System.out.println("  " + fieldName + " (DV): " + dv.lookupOrd(ord).utf8ToString());
//                                }
//                            }
//                        }
//                    }
//                    segmentIdx++;
//                }
//            }
//        }


    public MergeHandler(
        CompositeEngine compositeEngine,
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        Any dataFormats,
        ShardId shardId,
        String sortColumn,
        boolean reverseSort
    ) {
        this.shardId = shardId;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.compositeDataFormat = dataFormats;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.compositeEngine = compositeEngine;
        this.sortColumn = sortColumn;
        this.reverseSort = reverseSort;
        dataFormatMergerMap = new HashMap<>();

        compositeIndexingExecutionEngine.getDelegates().forEach(engine -> {
            try {
                dataFormatMergerMap.put(engine.getDataFormat(), engine.getMerger());
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to create merger for data format [{}]",
                    engine.getDataFormat(), e));
                throw new RuntimeException(e);
            }
        });
    }

    public abstract Collection<OneMerge> findMerges();

    public abstract Collection<OneMerge> findForceMerges(int maxSegmentCount);

    public synchronized void updatePendingMerges() {
        Collection<OneMerge> oneMerges = findMerges();
        for (OneMerge oneMerge : oneMerges) {
            boolean isValidMerge = true;
            for (Segment segment : oneMerge.getSegmentsToMerge()) {
                if (currentlyMergingSegments.contains(segment)) {
                    isValidMerge = false;
                    break;
                }
            }
            if (isValidMerge) {
                registerMerge(oneMerge);
            }
        }
    }

    public synchronized void registerMerge(OneMerge merge) {
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            // Validate segments exist in catalog
            List<Segment> catalogSegments = catalogSnapshotReleasableRef.getRef().getSegments();
            for (Segment mergeSegment : merge.getSegmentsToMerge()) {
                if (!catalogSegments.contains(mergeSegment)) {
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to acquire snapshots", e));
            throw new RuntimeException(e);
        }
        mergingSegments.add(merge);
        currentlyMergingSegments.addAll(merge.getSegmentsToMerge());
        logger.debug(() -> new ParameterizedMessage("Registered merge [{}], mergingSegments: [{}]",
            merge, mergingSegments));
    }

    public boolean hasPendingMerges() {
        return !mergingSegments.isEmpty();
    }

    public synchronized OneMerge getNextMerge() {
        if(mergingSegments.isEmpty()) {
            return null;
        }
        return mergingSegments.removeFirst();
    }

    public synchronized void onMergeFinished(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        updatePendingMerges();
    }

    public synchronized void onMergeFailure(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        logger.warn(() -> new ParameterizedMessage("Merge failed for OneMerge [{}]", oneMerge));
    }

    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        mergingSegments.remove(oneMerge);
        oneMerge.getSegmentsToMerge().forEach(currentlyMergingSegments::remove);
    }

    public MergeResult doMerge(OneMerge oneMerge) {

        long mergedWriterGeneration = compositeIndexingExecutionEngine.getNextWriterGeneration();
        Map<DataFormat, WriterFileSet> mergedWriterFileSet = new HashMap<>();
        boolean mergeSuccessful = false;

        try {
            List<WriterFileSet> filesToMerge =
                getFilesToMerge(oneMerge, compositeDataFormat.getPrimaryDataFormat());

            long timeNS = System.nanoTime();

            // Merging primary data format
            MergeInput primaryMergeInput = new MergeInput(filesToMerge, mergedWriterGeneration, sortColumn, reverseSort);
            MergeResult primaryMergeResult = dataFormatMergerMap
                .get(compositeDataFormat.getPrimaryDataFormat())
                .merge(primaryMergeInput);

            mergedWriterFileSet.put(
                compositeDataFormat.getPrimaryDataFormat(),
                primaryMergeResult.getMergedWriterFileSetForDataformat(compositeDataFormat.getPrimaryDataFormat())
            );

            long timeNS1 = System.nanoTime();
            long tookMS = TimeValue.nsecToMSec(timeNS1 - timeNS);
            logger.info("Parquet Merge time - {}", tookMS);

            // Merging other format as per the old segment + row id -> new row id mapping.
            compositeIndexingExecutionEngine.getDelegates().stream()
                .filter(engine -> !engine.getDataFormat().equals(compositeDataFormat.getPrimaryDataFormat()))
                .forEach(indexingExecutionEngine -> {
                    DataFormat df = indexingExecutionEngine.getDataFormat();
                    List<WriterFileSet> files = getFilesToMerge(oneMerge, df);

                    MergeInput secondaryMergeInput = new MergeInput(files, mergedWriterGeneration, sortColumn, reverseSort);
                    MergeResult secondaryMerge = dataFormatMergerMap.get(df)
                        .merge(secondaryMergeInput, primaryMergeResult.getRowIdMapping());

                    mergedWriterFileSet.put(df,
                        secondaryMerge.getMergedWriterFileSetForDataformat(df));
                });

            long tookMS1 = TimeValue.nsecToMSec(System.nanoTime() - timeNS1);
            logger.info("Lucene Merge time - {}", tookMS1);

            MergeResult mergeResult = new MergeResult(primaryMergeResult.getRowIdMapping(), mergedWriterFileSet);
            mergeSuccessful = true;
            return mergeResult;

        } finally {
            if (!mergeSuccessful && !mergedWriterFileSet.isEmpty()) {
                cleanupStaleMergedFiles(mergedWriterFileSet);
            }
        }
    }

    private void cleanupStaleMergedFiles(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        for (WriterFileSet wfs : mergedWriterFileSet.values()) {
            for (String file : wfs.getFiles()) {
                Path path = Path.of(wfs.getDirectory(), file);
                try {
                    Files.deleteIfExists(path);
                    logger.info("Stale Merged File Deleted at : [{}]", path);
                } catch (Exception exception) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Failed to delete stale merged file [{}]",
                            path
                        ),
                        exception
                    );
                }
            }
        }
    }

    private List<WriterFileSet> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat) {
        List<WriterFileSet> writerFileSets = new ArrayList<>();
        for (Segment segment : oneMerge.getSegmentsToMerge()) {
            writerFileSets.add(segment.getDFGroupedSearchableFiles().get(dataFormat.name()));
        }
        return writerFileSets;
    }
}
