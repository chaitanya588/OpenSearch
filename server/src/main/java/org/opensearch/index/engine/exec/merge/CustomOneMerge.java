package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class CustomOneMerge extends MergePolicy.OneMerge {

    private final RowIdMapping rowIdMapping;
    private final long writerGeneration;

    public CustomOneMerge(
        List<SegmentCommitInfo> segments,
        RowIdMapping rowIdMapping,
        long writerGeneration
    ) {
        super(segments);
        this.rowIdMapping = rowIdMapping;
        this.writerGeneration = writerGeneration;
    }

    @Override
    public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) throws IOException {
        int totalDocs = reader.maxDoc();

        // Build oldToNew: maps current doc position -> new position after reorder
        // Build newToOld: maps new position -> original doc position
        int[] oldToNew = new int[totalDocs];
        int[] newToOld = new int[totalDocs];

        // Track doc offset per segment, keyed by writer_generation attribute
        // (matches RowId.fileId which is now the generation number string)
        Map<String, Integer> fileIdToBaseDoc = new HashMap<>();
        int baseDoc = 0;
        for (SegmentCommitInfo segmentInfo : segments) {
            String writerGen = segmentInfo.info.getAttribute("writer_generation");
            if (writerGen != null) {
                fileIdToBaseDoc.put(writerGen, baseDoc);
            }
            baseDoc += segmentInfo.info.maxDoc();
        }

        // Build mapping using efficient array-based iteration
        // Avoid getMapping() which creates HashMap objects
        for (int i = 0; i < rowIdMapping.size(); i++) {
            long oldRowId = rowIdMapping.getOldRowId(i);
            long newRowId = rowIdMapping.getNewRowIdAt(i);
            String oldFileId = rowIdMapping.getFileIdAt(i);

            Integer segmentBase = fileIdToBaseDoc.get(oldFileId);
            if (segmentBase != null) {
                int oldDocId = segmentBase + (int) oldRowId;
                int newDocId = (int) newRowId;

                if (oldDocId < totalDocs && newDocId < totalDocs) {
                    oldToNew[oldDocId] = newDocId;
                    newToOld[newDocId] = oldDocId;
                }
            }
        }

        return new Sorter.DocMap() {
            @Override
            public int oldToNew(int docID) {
                return oldToNew[docID];
            }

            @Override
            public int newToOld(int docID) {
                return newToOld[docID];
            }

            @Override
            public int size() {
                return totalDocs;
            }
        };
    }

    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
        super.setMergeInfo(info);
        if (info != null) {
            info.info.putAttribute("writer_generation", String.valueOf(writerGeneration));
        }
    }
}
