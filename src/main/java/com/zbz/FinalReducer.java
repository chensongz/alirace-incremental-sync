package com.zbz;

import java.util.Set;

/**
 * Created by bgk on 6/14/17.
 */
public class FinalReducer {
    private Index baseIndex;
    private Index appendIndex;
    private Persistence basePersistence;
    private Persistence appendPersistence;

    public FinalReducer(Index baseIndex, Index appendIndex, Persistence basePersistence, Persistence appendPersistence) {
        this.baseIndex = baseIndex;
        this.appendIndex = appendIndex;
        this.basePersistence = basePersistence;
        this.appendPersistence = appendPersistence;
    }

    public void compute(long start, long end, Pool<String> sendPool) {
        Set<String> appendKeySet = appendIndex.getIndexHashMap().keySet();

        for (String appendPrimaryValue : appendKeySet) {
            long appendOffset = appendIndex.getOffset(appendPrimaryValue);
            String appendBinlogLine = new String(appendPersistence.read(appendOffset));
            Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
            String appendBinlogPrimaryValueStr = appendBinlog.getPrimaryValue();
            String appendBinlogPrimaryOldValueStr = appendBinlog.getPrimaryOldValue();

            long appendBinlogPrimaryValue = parseLong(appendBinlogPrimaryValueStr);
            long appendBinlogPrimaryOldValue = parseLong(appendBinlogPrimaryOldValueStr);

            if ((appendBinlogPrimaryOldValue <= start || appendBinlogPrimaryOldValue >= end)
                && (appendBinlogPrimaryValue <= start || appendBinlogPrimaryValue >= end)) {
                // both not in,then do nothing
                baseIndex.delete(appendBinlogPrimaryOldValueStr);
                baseIndex.delete(appendBinlogPrimaryValueStr);
            } else {
                long baseOffset;
                if ((baseOffset = baseIndex.getOffset(appendBinlogPrimaryValueStr)) >= 0) {
                    // update other fields
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                    if (newBinlog != null) {
                        if (appendBinlogPrimaryValue != Long.parseLong(newBinlog.getPrimaryValue())) {
//                            System.out.println("delete primaryValue:" + appendBinlogPrimaryValue + "- :" + newBinlog.getPrimaryValue());
                            baseIndex.delete(appendBinlogPrimaryValueStr);
                        }
                        long offset = basePersistence.write(newBinlog.toBytes());
                        baseIndex.insert(newBinlog.getPrimaryValue(), offset);
                    } else {
                        baseIndex.delete(appendBinlogPrimaryValueStr);
                    }
                } else if ((baseOffset = baseIndex.getOffset(appendBinlogPrimaryOldValueStr)) >= 0) {
                    //update key field
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                    if (newBinlog != null) {
                        long offset = basePersistence.write(newBinlog.toBytes());
                        baseIndex.delete(appendBinlogPrimaryOldValueStr);
                        baseIndex.insert(appendBinlogPrimaryValueStr, offset);
                    } else {
                        baseIndex.delete(appendBinlogPrimaryOldValueStr);
                    }
                } else {
                    // if not in baseindex, then insert
                    long offset = basePersistence.write(appendBinlog.toBytes());
                    baseIndex.insert(appendBinlogPrimaryValueStr, offset);
                }
            }

        }

        for (long primaryValue = start + 1; primaryValue < end; primaryValue++) {
            long baseOffset = baseIndex.getOffset(String.valueOf(primaryValue));
            if (baseOffset >= 0) {
                String baseBinlogLine = new String(basePersistence.read(baseOffset));
                Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                sendPool.put(baseBinlog.toSendString());
            }
        }

        sendPool.put("NULL");

        baseIndex = null;
        basePersistence = null;
        appendIndex = null;
        appendPersistence = null;
    }

    private long parseLong(String key) {
        if (key.equals("NULL")) {
            return Long.MIN_VALUE + 1;
        } else {
            return Long.parseLong(key);
        }
    }
}
