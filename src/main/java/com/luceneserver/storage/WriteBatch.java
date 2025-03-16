package com.luceneserver.storage;

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
}
