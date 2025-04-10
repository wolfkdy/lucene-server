package com.luceneserver.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class SearchIndexAccess extends IndexAccess {

    public static class SearchConfig {
        public String defaultAnalyzer;
        public Map<String, String> perFieldAnalyzer;
    }
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(SearchIndexAccess.class);

    private SearchIndexAccess(FSDirectory d, IndexWriterConfig cfg, Clock c) throws IOException {
        super(d, cfg, c);
    }

    private static Analyzer newAnalyzerFromString(String s) {
        if (s.equals("lucene.standard")) {
            return new StandardAnalyzer();
        } else if (s.equals("lucene.simple")) {
            return new SimpleAnalyzer();
        } else {
            throw new IllegalArgumentException("invalid analyzer " + s);
        }
    }
    public static SearchIndexAccess createInstance(
            Path dir, Clock c, SearchConfig cfg, MergeScheduler ms, long maxSegmentSize, long ramBufferSizeMB) throws IOException {
        FSDirectory index = FSDirectory.open(dir);
        Map<String, Analyzer> analyzerMap = new HashMap<>();
        cfg.perFieldAnalyzer.forEach((k, v) -> {
            analyzerMap.put(k, newAnalyzerFromString(v));
        });
        Analyzer defaultAnalyzer = newAnalyzerFromString(cfg.defaultAnalyzer);
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzerMap);
        IndexWriterConfig config = new IndexWriterConfig(wrapper);
        config.setMergeScheduler(ms);
        TieredMergePolicy mp = new TieredMergePolicy();
        mp.setMaxMergedSegmentMB(maxSegmentSize);
        config.setMergePolicy(mp);
        config.setRAMBufferSizeMB(ramBufferSizeMB);
        return new SearchIndexAccess(index, config, c);
    }

    @Override
    protected void doBatchWrite(WriteBatch batch) throws IOException {
        for (WriteBatch.Op o : batch.ops) {
            WriteBatch.SearchOp op = (WriteBatch.SearchOp) o;
            Document doc = null;
            if (op.isInsert() || op.isUpdate()) {
                doc = new Document();
                doc.add(new StringField("id", op.id, Field.Store.YES));
                for (WriteBatch.SearchOp.Field f : op.fields) {
                    FieldType type = new FieldType();
                    type.setOmitNorms(!f.norms);
                    type.setIndexOptions(f.indexOptions);
                    type.setStored(f.store);
                    Field field = new Field(f.fieldName, f.text, type);
                    doc.add(field);
                }
            }
            if (op.isInsert()) {
                indexWriter.addDocument(doc);
            } else if (op.isDelete()) {
                indexWriter.deleteDocuments(new Term("id", op.id));
            } else if (op.isUpdate()) {
                indexWriter.updateDocument(new Term("id", op.id), doc);
            }
        }
    }
}
