package com.zbz;

import java.util.HashMap;

/**
 * Created by Victor on 2017/6/10.
 */
public class BinlogReducer {
    private static final int CAPACITY = 10000;

    private HashMap<Long, Binlog> binlogHashMap = new HashMap<>(CAPACITY);
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
            newBinlog.setPrimaryValue(String.valueOf(oldBinlog.getPrimaryOldValue()));
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
        Binlog newBinlog = BinlogFactory.createBinlog(line);
        Binlog binlog;
        if (newBinlog != null) {
            long primaryValue = newBinlog.getPrimaryValue();
            long primaryOldValue = newBinlog.getPrimaryOldValue();
            if (binlogHashMap.containsKey(primaryValue)) {
                // maybe insert record or update fields or delete record
                Binlog oldBinlog = binlogHashMap.get(primaryValue);
                binlog = updateOldBinlog(oldBinlog, newBinlog);
                if (binlog != null) {
                    if (primaryValue != binlog.getPrimaryValue()) {
                        binlogHashMap.remove(primaryValue);
                    }
                    binlogHashMap.put(binlog.getPrimaryValue(), binlog);
                } else {
                    binlogHashMap.remove(primaryValue);
                }
            } else if (binlogHashMap.containsKey(primaryOldValue)) {
                // maybe update primary key
                Binlog oldBinlog = binlogHashMap.get(primaryOldValue);
                binlog = updateOldBinlog(oldBinlog, newBinlog);
                if (binlog != null) {
                    binlogHashMap.remove(primaryOldValue);
                    binlogHashMap.put(primaryValue, binlog);
                } else {
                    binlogHashMap.remove(primaryOldValue);
                }
            } else {
                binlogHashMap.put(primaryValue, newBinlog);
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
