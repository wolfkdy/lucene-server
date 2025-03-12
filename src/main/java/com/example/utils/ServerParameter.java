package com.example.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ServerParameter<T> {
    private final String name;
    private AtomicReference<T> data = new AtomicReference<>();
    private final boolean canBeChangedAtRuntime;
    private final static ConcurrentHashMap<String, ServerParameter<?>> allParams = new ConcurrentHashMap<>();
    public T get() {
        return data.get();
    }

    public void setAtRuntime(T newData) {
        if (!canBeChangedAtRuntime) {
            throw new RuntimeException("param " + name + " can not change at runtime");
        }
        doSet(newData);
    }

    public static void load(Map<String, Object> m) {
        for (Map.Entry<String, Object> entry : m.entrySet()) {
            if (!allParams.containsKey(entry.getKey())) {
                throw new IllegalArgumentException(entry.getKey() + " is not a known param");
            }
            allParams.get(entry.getKey()).doSet(entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void doSet(Object newData) {
        T typedNewData = (T)newData;
        validate(typedNewData);
        data.set(typedNewData);
    }
    public void validate(T data) {}
    public ServerParameter(String name, T defaultData, boolean canBeChangedAtRuntime) {
        this.name = name;
        this.canBeChangedAtRuntime = canBeChangedAtRuntime;
        doSet(defaultData);
        if (ServerParameter.allParams.containsKey(name)) {
            throw new IllegalArgumentException(name + " already exists in serverParams");
        }
        ServerParameter.allParams.put(name, this);
    }
}
