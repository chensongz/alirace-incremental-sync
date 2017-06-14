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

    public static Binlog updateOldBinlog(Binlog oldBinlog, Binlog newBinlog, byte newBinlogOperation) {
        oldBinlog.setOperation(newBinlogOperation);
        oldBinlog.setPrimaryValue(String.valueOf(newBinlog.getPrimaryValue()));
        for (Field field: newBinlog.getFields().values()) {
            oldBinlog.addField(field);
        }
        return oldBinlog;
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
                switch (oldBinlog.getOperation()) {
                    case Binlog.I:
                        // from I to others
                        switch (newBinlog.getOperation()) {
                            case Binlog.U:
                                // update fields I -> U => I
                                binlog = updateOldBinlog(oldBinlog, newBinlog, Binlog.I);
                                binlogHashMap.put(oldBinlog.getPrimaryValue(), binlog);
                                break;
                            case Binlog.D:
                                // delete record I -> D => null
                                binlogHashMap.remove(oldBinlog.getPrimaryValue());
                                break;
                            default:
                                break;
                        }
                        break;
                    case Binlog.U:
                        // from U to others
                        switch (newBinlog.getOperation()) {
                            case Binlog.U:
                                // U -> U => U
                                binlog = updateOldBinlog(oldBinlog, newBinlog, Binlog.U);
                                binlogHashMap.put(oldBinlog.getPrimaryValue(), binlog);
                                break;
                            case Binlog.D:
                                // U -> D => D
                                oldBinlog.setOperation(Binlog.D);
                                binlogHashMap.put(oldBinlog.getPrimaryValue(), newBinlog);
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
                                oldBinlog.setOperation(Binlog.U);
                                binlogHashMap.put(oldBinlog.getPrimaryValue(), newBinlog);
                                break;
                            default:
                                break;
                        }
                        break;
                }
            } else if (binlogHashMap.containsKey(newBinlog.getPrimaryOldValue())) {
                // maybe update primary key
                Binlog oldBinlog = binlogHashMap.get(newBinlog.getPrimaryOldValue());
                switch (oldBinlog.getOperation()) {
                    case Binlog.I:
                        // I -> U => I
                        binlog = updateOldBinlog(oldBinlog, newBinlog, Binlog.I);
                        binlogHashMap.put(oldBinlog.getPrimaryValue(), binlog);
                        break;
                    case Binlog.U:
                        // U -> U => U
                        binlog = updateOldBinlog(oldBinlog, newBinlog, Binlog.U);
                        binlogHashMap.put(oldBinlog.getPrimaryValue(), binlog);
                        break;
                    default:
                        break;
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
