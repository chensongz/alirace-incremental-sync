package com.zbz;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by bgk on 6/7/17.
 */

public class KeyValue {

    private final Map<String, Object> kvs = new HashMap<>();
    public KeyValue put(String key, Object value) {
        kvs.put(key, value);
        return this;
    }

    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    public KeyValue put(String key, String value) {

        kvs.put(key, value);
        return this;
    }

    public long getLong(String key) {
        if (kvs.containsKey(key)) {
            return (Long)kvs.get(key);
        }
        return 0L;
    }

    public String getString(String key) {
        if (kvs.containsKey(key)) {
            return (String)kvs.get(key);
        }
        return null;
    }

    public Object getObject(String key) {
        if (kvs.containsKey(key)) {
            return kvs.get(key);
        }
        return null;
    }

    public Set<String> keySet() {
        return kvs.keySet();
    }

    public Collection<Object> values() {
        return kvs.values();
    }

    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}