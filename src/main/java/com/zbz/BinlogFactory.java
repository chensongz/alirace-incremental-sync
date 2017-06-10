package com.zbz;

/**
 * Created by bgk on 6/9/17.
 */
public class BinlogFactory {
    public static Binlog createBinlog(String line) {
        String[] strings = line.split("\\|");
        Binlog binlog = new Binlog();
        switch (strings[5]) {
            case "I":
                binlog.setOperation(Binlog.I);
                break;
            case "U":
                binlog.setOperation(Binlog.U);
                break;
            case "D":
                binlog.setOperation(Binlog.D);
                break;

        }
        String[] fieldStrings;
        String fieldname;
        String fieldType;
        String oldValue;
        String newValue;
        int i = 6;
        while (i < strings.length) {
            fieldStrings = strings[i++].split(":");
            fieldname = fieldStrings[0];
            fieldType = fieldStrings[1];
            oldValue = strings[i++];
            newValue = strings[i++];
            if (fieldStrings[2].equals("1")) {
                // if field is primary key
                binlog.setPrimaryKey(fieldStrings[0]);
                if (binlog.getOperation() == 3) {
                    // if delete operation
                    binlog.setPrimaryOldValue(newValue);
                    binlog.setPrimaryValue(oldValue);
                } else {
                    binlog.setPrimaryOldValue(oldValue);
                    binlog.setPrimaryValue(newValue);
                }
            } else {
                // if field is not primary key
                Field field = new Field(fieldname, Byte.parseByte(fieldType), newValue);
                binlog.addField(field);
            }

        }
        return binlog;
    }

    public static Binlog createBinlog(String line, String table) {
        String[] strings = line.split("\\|");
        if (strings[4].equals(table)) {
            return createBinlog(line);
        }
        return null;
    }
}
