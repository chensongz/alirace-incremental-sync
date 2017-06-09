package com.zbz;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by bgk on 6/9/17.
 */
public class Table {
    private List<String> fields = new LinkedList<>();

    public void put(String fieldname) {
        fields.add(fieldname);
    }

    public List<String> getFields() {
        return fields;
    }
}
