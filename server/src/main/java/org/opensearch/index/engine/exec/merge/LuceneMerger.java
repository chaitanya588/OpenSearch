package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter.ROW_ID;

public class LuceneMerger implements Merger {

    private static final Logger logger = LogManager.getLogger(LuceneMerger.class);

    private static final Field SEGMENT_INFOS_FIELD;
    private static final Field SEGMENTS_FIELD;

    static {
        try {
            SEGMENT_INFOS_FIELD = IndexWriter.class.getDeclaredField("segmentInfos");
            SEGMENT_INFOS_FIELD.setAccessible(true);
            SEGMENTS_FIELD = SegmentInfos.class.getDeclaredField("segments");
            SEGMENTS_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final CustomIndexWriter indexWriter;
    private final Path targetDirectoryPath;

    public LuceneMerger(CustomIndexWriter indexWriter, Path targetDirectoryPath) {
        this.indexWriter = indexWriter;
        this.targetDirectoryPath = targetDirectoryPath;
    }

    @Override
    public MergeResult merge(List<WriterFileSet> fileMetadataList, long writerGeneration) {
        return merge(fileMetadataList, null, writerGeneration);
    }

    @Override
    public MergeResult merge(List<WriterFileSet> fileMetadataList, RowIdMapping rowIdMapping, long writerGeneration) {
        try {
            if (rowIdMapping != null) {
                logger.info("LuceneMerger starting merge with RowIdMapping: fileId={}, mappingSize={}",
                    rowIdMapping.getFileId(), rowIdMapping.getMapping().size());
            }

            // Collect the writer generations of the segments we want to merge.
            // These match the "writer_generation" attribute stored on each Lucene segment
            // by LuceneWriterCodec when the segment was originally written.
            Set<Long> generationsToMerge = new HashSet<>();
            for (WriterFileSet fileSet : fileMetadataList) {
                generationsToMerge.add(fileSet.getWriterGeneration());
            }

            // Get actual SegmentCommitInfo references from IndexWriter via reflection
            // and snapshot segment names before merge to identify the new output segment
            List<SegmentCommitInfo> segmentsToMerge = new ArrayList<>();
            Set<String> segmentNamesBefore = new HashSet<>();
            for (SegmentCommitInfo sci : getSegmentsViaReflection(indexWriter)) {
                segmentNamesBefore.add(sci.info.name);
                String generationAttr = sci.info.getAttribute("writer_generation");
                if (generationAttr != null && generationsToMerge.contains(Long.parseLong(generationAttr))) {
                    segmentsToMerge.add(sci);
                }
            }

            if (!segmentsToMerge.isEmpty()) {
                MergePolicy.OneMerge oneMerge = new CustomOneMerge(segmentsToMerge, rowIdMapping);
                indexWriter.executeMerge(oneMerge);

                // Collect files from newly produced segment(s) using reflection
                Set<String> segmentFiles = new HashSet<>();
                for (SegmentCommitInfo sci : getSegmentsViaReflection(indexWriter)) {
                    if (!segmentNamesBefore.contains(sci.info.name)) {
                        segmentFiles.addAll(sci.files());
                    }
                }

                // Validate ROW_ID doc values against RowIdMapping (requires opening reader)
//                if (rowIdMapping != null) {
//                    validateRowIdMapping(rowIdMapping, segmentsToMerge, segmentNamesBefore);
//                }

                WriterFileSet mergedFileSet = WriterFileSet.builder()
                    .directory(targetDirectoryPath)
                    .writerGeneration(writerGeneration)
                    .addFiles(segmentFiles)
                    .build();

                Map<DataFormat, WriterFileSet> mergedWriterFileSet = Map.of(DataFormat.LUCENE, mergedFileSet);

                return new MergeResult(rowIdMapping, mergedWriterFileSet);
            }

            // No segments to merge — return empty result
            return new MergeResult(rowIdMapping, Map.of());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access segments via reflection", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<SegmentCommitInfo> getSegmentsViaReflection(IndexWriter writer) throws IllegalAccessException {
        SegmentInfos segmentInfos = (SegmentInfos) SEGMENT_INFOS_FIELD.get(writer);
        return (List<SegmentCommitInfo>) SEGMENTS_FIELD.get(segmentInfos);
    }

    /**
     * Validates ROW_ID doc values in merged segment against RowIdMapping.
     * Only called when debug logging is enabled.
     */
    private void validateRowIdMapping(RowIdMapping rowIdMapping, List<SegmentCommitInfo> segmentsToMerge,
                                      Set<String> segmentNamesBefore) throws IOException {
        // Build fileId -> baseDoc offset map
        Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
        int baseDoc = 0;
        for (SegmentCommitInfo info : segmentsToMerge) {
            String writerGen = info.info.getAttribute("writer_generation");
            if (writerGen != null) {
                fileIdToBaseDoc.put(writerGen, baseDoc);
            }
            baseDoc += info.info.maxDoc();
        }

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SegmentReader sr = Lucene.segmentReader(ctx.reader());
                if (segmentNamesBefore.contains(sr.getSegmentInfo().info.name)) {
                    continue;
                }

                int totalDocs = sr.maxDoc();
                long[] mergedRowIds = new long[totalDocs];
                NumericDocValues rowIdDV = sr.getNumericDocValues(ROW_ID);
                if (rowIdDV == null) {
                    logger.debug("RowIdMapping validation skipped: no {} doc values in merged segment", ROW_ID);
                    return;
                }
                while (rowIdDV.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                    mergedRowIds[rowIdDV.docID()] = rowIdDV.longValue();
                }

                int matched = 0, mismatched = 0, skipped = 0;
                for (Map.Entry<RowId, Long> entry : rowIdMapping.getMapping().entrySet()) {
                    RowId oldRowId = entry.getKey();
                    int newDocPos = entry.getValue().intValue();

                    if (fileIdToBaseDoc.get(oldRowId.getFileId()) == null) {
                        skipped++;
                        continue;
                    }
                    if (newDocPos >= totalDocs) {
                        mismatched++;
                        continue;
                    }

                    if (mergedRowIds[newDocPos] == oldRowId.getRowId()) {
                        matched++;
                    } else {
                        mismatched++;
                    }
                }
                logger.debug("RowIdMapping validation: matched={}, mismatched={}, skipped={}", matched, mismatched, skipped);
            }
        }
    }
}
