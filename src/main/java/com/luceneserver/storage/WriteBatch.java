package com.luceneserver.storage;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;

public class WriteBatch {
    public static abstract class Op {
        public String op;
        public String id;
        public boolean isInsert() {
            return op.equals("i");
        }
        public boolean isDelete() {
            return op.equals("d");
        }
        public boolean isUpdate() {
            return op.equals("u");
        }
    }
    public String indexName;
    public boolean autoCommit;
    public Op[] ops;

    public static class HnswOp extends Op {
        public float[] vector;
    }
    public static class SearchOp extends Op {
        public static class Field {
            public String fieldName;
            public String text;
            public boolean store;
            public IndexOptions indexOptions;
            public boolean norms;
        }
        public Field[] fields;
    }
}
