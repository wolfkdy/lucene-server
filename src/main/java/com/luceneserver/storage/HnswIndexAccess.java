package com.luceneserver.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;

public class HnswIndexAccess extends IndexAccess {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(HnswIndexAccess.class);

    public static class HnswConfig {
        public int efConstruction;
        public int maxConn;
        public int dimensions;
        public String similarity;
    }

    private final VectorSimilarityFunction similarity;
    private final int dimensions;

    public static HnswIndexAccess createInstance(
            Path dir, Clock c, HnswConfig cfg, MergeScheduler ms, long maxSegmentSize, long ramBufferSizeMB) throws IOException {
        FSDirectory index = FSDirectory.open(dir);
        Lucene95Codec knnVectorsCodec = new Lucene95Codec(Lucene95Codec.Mode.BEST_SPEED) {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                int maxConn = cfg.maxConn;
                int beamWidth = cfg.efConstruction;
                KnnVectorsFormat knnFormat = new Lucene95HnswVectorsFormat(maxConn, beamWidth);
                return new HighDimensionKnnVectorsFormat(knnFormat, cfg.dimensions);
            }
        };
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer()).setCodec(knnVectorsCodec);
        config.setMergeScheduler(ms);
        TieredMergePolicy mp = new TieredMergePolicy();
        mp.setMaxMergedSegmentMB(maxSegmentSize);
        config.setMergePolicy(mp);
        config.setRAMBufferSizeMB(ramBufferSizeMB);


        final VectorSimilarityFunction sim;
        if (cfg.similarity.equals("euclidean")) {
            sim = VectorSimilarityFunction.EUCLIDEAN;
        } else if (cfg.similarity.equals("cosine")) {
            sim = VectorSimilarityFunction.COSINE;
        } else if (cfg.similarity.equals("dotProduct")) {
            sim = VectorSimilarityFunction.DOT_PRODUCT;
        } else {
            throw new IllegalArgumentException("unknown similarity type " + cfg.similarity);
        }
        return new HnswIndexAccess(index, config, c, sim, cfg.dimensions);
    }

    public String[] knn(float[] query, int k, int candidates) throws IOException {
        rwLock.readLock().lock();
        try {
            return doKnnInLock(query, k, candidates);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private String[] doKnnInLock(float[] q, int k, int m) throws IOException {
        IndexSearcher searcher = new IndexSearcher(directoryReader.get());
        Query query = new KnnFloatVectorQuery("vector", q, m);
        TopDocs topDocs = searcher.search(query, k);
        ScoreDoc[] hits = topDocs.scoreDocs;
        String[] ids = new String[hits.length];
        int i = 0;
        for (ScoreDoc hit: hits) {
            Document d = searcher.storedFields().document(hit.doc);
            ids[i++] = d.get("id");
        }
        return ids;
    }

    /*
     * TODO(deyukong):
     * NOTE(deyukong): the multi-thread access model of this class is NOT determined.
     * should it be 1-writer-n-readers or n-writers-n-readers? As writing is in  batch mode,
     * a single-writer probably may be enough, if so, a explicitly synchronized keyword
     * should be marked somewhere.
     */
    @Override
    protected void doBatchWrite(WriteBatch batch) throws IOException {
        for (WriteBatch.Op o : batch.ops) {
            WriteBatch.HnswOp op = (WriteBatch.HnswOp) o;
            Document doc = null;
            if (op.isInsert() || op.isUpdate()) {
                doc = new Document();
                doc.add(new StringField("id", op.id, Field.Store.YES));
                if (op.vector.length != dimensions) {
                    throw new IllegalArgumentException("invalid vector length");
                }
                doc.add(new KnnFloatVectorField("vector", op.vector, similarity));
            }
            if (op.isInsert()) {
                indexWriter.addDocument(doc);
            } else if (op.isDelete()) {
                indexWriter.deleteDocuments(new Term("id", op.id));
            } else if (op.isUpdate()) {
                indexWriter.updateDocument(new Term("id", op.id), doc);
            }
        }

        if (batch.autoCommit) {
            commitAndRefreshReaderInLock();
        }
    }

    private HnswIndexAccess(FSDirectory d, IndexWriterConfig cfg, Clock c, VectorSimilarityFunction sim, int dimensions) throws IOException {
        super(d, cfg, c);
        this.similarity = sim;
        this.dimensions = dimensions;
    }

    private static class HighDimensionKnnVectorsFormat extends KnnVectorsFormat {
        private final KnnVectorsFormat knnFormat;
        private final int maxDimensions;

        public HighDimensionKnnVectorsFormat(KnnVectorsFormat knnFormat, int maxDimensions) {
            super(knnFormat.getName());
            this.knnFormat = knnFormat;
            this.maxDimensions = maxDimensions;
        }

        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return knnFormat.fieldsWriter(state);
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
            return knnFormat.fieldsReader(state);
        }

        @Override
        public int getMaxDimensions(String fieldName) {
            return maxDimensions;
        }
    }
}
