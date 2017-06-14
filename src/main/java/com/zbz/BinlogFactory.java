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
                if (binlog.getOperation() == Binlog.D) {
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

    public static Binlog createBinlog(String line, String schema, String table) {
        String[] strings = line.split("\\|");
        if (strings[3].equals(schema) && strings[4].equals(table)) {
            return createBinlog(line);
        }
        return null;
    }

    public static Binlog parse(String binlogLine) {
        String[] strings = binlogLine.split("\\|");
//        for (String s : strings) {
//            System.out.print(s);
//        }
//        System.out.println("");
        Binlog binlog = new Binlog();
//        System.out.println("operation:" + strings[0]);
        binlog.setOperation(Byte.parseByte(strings[0]));
        String primaryInfo = strings[1];
        String[] primaryInfos = primaryInfo.split(":");
        binlog.setPrimaryKey(primaryInfos[0]);
        binlog.setPrimaryOldValue(primaryInfos[1]);
        binlog.setPrimaryValue(primaryInfos[2]);
        String[] fieldStrings;
        String fieldname;
        String fieldType;
        String fieldValue;
        int i = 2;
        while (i < strings.length) {
            fieldStrings = strings[i++].split(":");
            fieldname = fieldStrings[0];
            fieldType = fieldStrings[1];
            fieldValue = fieldStrings[2];
            Field field = new Field(fieldname, Byte.parseByte(fieldType), fieldValue);
            binlog.addField(field);
        }

        return binlog;
    }
}
