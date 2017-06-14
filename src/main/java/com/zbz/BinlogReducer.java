package com.zbz;

import java.util.HashMap;

/**
 * Created by Victor on 2017/6/10.
 */
public class BinlogReducer {
    private static final int CAPACITY = 500;

    private HashMap<Long, Binlog> binlogHashMap = new HashMap<>();
    private String schema;
    private String table;

    public static Binlog updateOldBinlog(Binlog oldBinlog, Binlog newBinlog) {
        byte transferOperation = newBinlog.getOperation();
        switch (oldBinlog.getOperation()) {
            case Binlog.I:
                switch (newBinlog.getOperation()) {
                    case Binlog.U:
                        // I -> U => I
                        transferOperation = Binlog.I;
                        break;
                    case Binlog.D:
                        // I -> D => D
                        transferOperation = Binlog.ID;
                        break;
                    default:
                        break;
                }
                break;
            case Binlog.U:
                switch (newBinlog.getOperation()) {
                    case Binlog.U:
                        // U -> U => U
                        transferOperation = Binlog.U;
                        break;
                    case Binlog.D:
                        // U -> D => D
                        transferOperation = Binlog.D;
                        break;
                    default:
                        break;
                }
                break;
            case Binlog.D:
                // from D to others
                switch (newBinlog.getOperation()) {
                    case Binlog.I:
                        // D -> I => U
                        transferOperation = Binlog.DI;
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        if (transferOperation == Binlog.ID) {
            return  null;
        } else if (transferOperation == Binlog.DI) {
            newBinlog.setOperation(Binlog.U);
            return newBinlog;
        } else if (transferOperation == Binlog.D) {
            newBinlog.setOperation(Binlog.D);
            return newBinlog;
        } else {
            oldBinlog.setOperation(transferOperation);
            oldBinlog.setPrimaryValue(String.valueOf(newBinlog.getPrimaryValue()));
            for (Field field: newBinlog.getFields().values()) {
                oldBinlog.addField(field);
            }
            return oldBinlog;
        }
    }

    public BinlogReducer(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public void reduce(String line) {
        Binlog newBinlog = BinlogFactory.createBinlog(line, schema, table);
        Binlog binlog;
        if (newBinlog != null) {
            if (binlogHashMap.containsKey(newBinlog.getPrimaryValue())) {
                // maybe insert record or update fields or delete record
                Binlog oldBinlog = binlogHashMap.get(newBinlog.getPrimaryValue());
                binlog = updateOldBinlog(oldBinlog, newBinlog);
                if (binlog != null) {
                    binlogHashMap.put(binlog.getPrimaryValue(), binlog);
                } else {
                    binlogHashMap.remove(oldBinlog.getPrimaryValue());
                }
            } else if (binlogHashMap.containsKey(newBinlog.getPrimaryOldValue())) {
                // maybe update primary key
                Binlog oldBinlog = binlogHashMap.get(newBinlog.getPrimaryOldValue());
                binlog = updateOldBinlog(oldBinlog, newBinlog);
                binlogHashMap.remove(oldBinlog.getPrimaryValue());
                if (binlog != null) {
                    binlogHashMap.put(binlog.getPrimaryValue(), binlog);
                }
            } else {
                binlogHashMap.put(newBinlog.getPrimaryValue(), newBinlog);
            }
        }
    }

    public boolean isFull() {
        return binlogHashMap.size() >= CAPACITY;
    }

    public HashMap<Long, Binlog> getBinlogHashMap() {
        return binlogHashMap;
    }

    public void clearBinlogHashMap() {
        binlogHashMap.clear();
    }
}
