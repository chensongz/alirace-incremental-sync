package com.zbz;

import java.util.HashMap;

/**
 * Created by Victor on 2017/6/10.
 */
public class BinlogReducer {
    private static final int CAPACITY = 500;

    private HashMap<Long, Binlog> binlogHashMap = new HashMap<>();
    private String table;

    public BinlogReducer(String table) {
        this.table = table;
    }

    public void reduce(String line) {
        Binlog newBinlog = BinlogFactory.createBinlog(line, table);
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
                                updateOldBinlog(oldBinlog, newBinlog, Binlog.I);
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
                                updateOldBinlog(oldBinlog, newBinlog, Binlog.U);
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
                        updateOldBinlog(oldBinlog, newBinlog, Binlog.I);
                        break;
                    case Binlog.U:
                        // U -> U => U
                        updateOldBinlog(oldBinlog, newBinlog, Binlog.U);
                        break;
                    default:
                        break;
                }
            } else {
                binlogHashMap.put(newBinlog.getPrimaryValue(), newBinlog);
            }
        }
    }

    public void updateOldBinlog(Binlog oldBinlog, Binlog newBinlog, byte Operation) {
        oldBinlog.setOperation(Operation);
        oldBinlog.setPrimaryValue(String.valueOf(newBinlog.getPrimaryValue()));
        for (Field field: newBinlog.getFields().values()) {
            oldBinlog.addField(field);
        }
        binlogHashMap.put(oldBinlog.getPrimaryValue(), oldBinlog);
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
