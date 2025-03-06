package com.example.storage;

import com.example.server.MessageProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class HnswIndexAccess extends IndexAccess {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(HnswIndexAccess.class);

    public static class HnswConfig {
        public int efConstruction;
        public int maxConn;
        public int dimensions;
    }
    public static HnswIndexAccess createInstance(Path dir, HnswConfig cfg) throws IOException {
        Directory index = FSDirectory.open(dir);
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
        IndexWriter writer = new IndexWriter(index, config);
        IndexReader reader = DirectoryReader.open(writer);
        return new HnswIndexAccess(writer, reader);
    }

    private HnswIndexAccess(IndexWriter writer, IndexReader reader) throws IOException {
        super(writer, reader);
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
