package com.zbz;

import java.util.Arrays;

/**
 * Created by bgk on 6/9/17.
 */
public class BinlogFactory {

    public static Binlog createBinlog(String line) {
        Binlog binlog = new Binlog();

        int fieldCnt = -1;
        int i;
        int currentIndex = 0;
        while ((i = line.indexOf('|', currentIndex)) >= 0) {
            fieldCnt++;
            if (fieldCnt == 5) {
                switch (line.charAt(currentIndex)) {
                    case 'I':
                        binlog.setOperation(Binlog.I);
                        break;
                    case 'U':
                        binlog.setOperation(Binlog.U);
                        break;
                    case 'D':
                        binlog.setOperation(Binlog.D);
                        break;
                }
                currentIndex = i + 1;
            } else if (fieldCnt > 5) {
                String fieldString = line.substring(currentIndex, i);
                int j;

                j = fieldString.indexOf(':');
                String fieldname = fieldString.substring(0, j);
                fieldString = fieldString.substring(j + 1);
                j = fieldString.indexOf(':');
                String isPrimaryKey = fieldString.substring(j + 1);

                int idx = line.indexOf('|', currentIndex);
                currentIndex = idx + 1;
                idx = line.indexOf('|', currentIndex);
                String oldValue = line.substring(currentIndex, idx);
                currentIndex = idx + 1;
                idx = line.indexOf('|', currentIndex);
                String newValue = line.substring(currentIndex, idx);

                if (isPrimaryKey.equals("1")) {
                    // if field is primary key
                    binlog.setPrimaryKey(fieldname);
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
                    binlog.addField(fieldname, newValue);
                }
                currentIndex = idx + 1;
            } else {
                currentIndex = i + 1;
            }
        }
        return binlog;
    }

    public static Binlog createBinlog0(String line) {
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
        String oldValue;
        String newValue;
        int i = 6;
        while (i < strings.length) {
            fieldStrings = strings[i++].split(":");
            fieldname = fieldStrings[0];
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
                binlog.addField(fieldname, newValue);
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
        Binlog binlog = new Binlog();
        binlog.setOperation(Byte.parseByte(strings[0]));
        String primaryInfo = strings[1];
        String[] primaryInfos = primaryInfo.split(":");
        binlog.setPrimaryKey(primaryInfos[0]);
        binlog.setPrimaryOldValue(primaryInfos[1]);
        binlog.setPrimaryValue(primaryInfos[2]);
        String[] fieldStrings;
        String fieldname;
        String fieldValue;
        int i = 2;
        while (i < strings.length) {
            fieldStrings = strings[i++].split(":");
            fieldname = fieldStrings[0];
            fieldValue = fieldStrings[1];
            binlog.addField(fieldname, fieldValue);
        }

        return binlog;
    }
}
