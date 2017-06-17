package com.zbz;

import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
public class BinlogReducer {
    private static final int CAPACITY = 8192;

    private TLongObjectHashMap<Binlog> binlogHashMap;
    private String schema;
    private String table;
    private int capacity;

    private long parseBinlog = 0;

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
            newBinlog.setPrimaryValue(oldBinlog.getPrimaryOldValue());
            return newBinlog;
        } else {
            oldBinlog.setOperation(transferOperation);
            oldBinlog.setPrimaryValue(newBinlog.getPrimaryValue());
            Map<String, String> fields =  newBinlog.getFields();
            for (String fieldname: fields.keySet()) {
                oldBinlog.addField(fieldname, fields.get(fieldname));
            }
            return oldBinlog;
        }
    }

    public BinlogReducer(String schema, String table, int capacity) {
        this.table = table;
        this.schema = schema;
        this.capacity = capacity;
        this.binlogHashMap = new TLongObjectHashMap<>();
    }

    public BinlogReducer(String schema, String table) {
        this(schema, table, CAPACITY);
    }

    public void reduce(Binlog newBinlog) {
        Binlog binlog;
        if (newBinlog != null) {
            long primaryValue = newBinlog.getPrimaryValue();
            long primaryOldValue = newBinlog.getPrimaryOldValue();
            Binlog oldBinlog;
            if ((oldBinlog = binlogHashMap.get(primaryValue)) != null) {
                // maybe insert record or update fields or delete record
                binlog = updateOldBinlog(oldBinlog, newBinlog);
                if (binlog != null) {
                    if (primaryValue != binlog.getPrimaryValue()) {
                        binlogHashMap.remove(primaryValue);
                    }
                    binlogHashMap.put(binlog.getPrimaryValue(), binlog);
                } else {
                    binlogHashMap.remove(primaryValue);
                }
            } else if ((oldBinlog = binlogHashMap.get(primaryOldValue)) != null) {
                // maybe update primary key
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

    public void reduce(String line) {
        long t1 = System.currentTimeMillis();
        Binlog newBinlog = BinlogFactory.createBinlog(line);
        long t2 = System.currentTimeMillis();

        parseBinlog += (t2 - t1);

        reduce(newBinlog);
    }

    public boolean isFull() {
        return binlogHashMap.size() >= capacity;
    }

    public TLongObjectHashMap<Binlog> getBinlogHashMap() {
        return binlogHashMap;
    }

    public void clearBinlogHashMap() {
        binlogHashMap.clear();
    }

    public long getParseBinlog() {
        return this.parseBinlog;
    }
}
