package com.zbz;

/**
 * Created by bgk on 6/7/17.
 */
public class Field {
    private String name;
    // 1 represent long , 2 represent String
    private byte type;
    private String value;

    public Field(String name, byte type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public byte getType() {
        return type;
    }

    public String getValue() {
        return value;
    }


}
