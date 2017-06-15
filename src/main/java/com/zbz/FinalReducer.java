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
        Set<Long> appendKeySet = appendIndex.getIndexHashMap().keySet();

        for (Long appendPrimaryValue : appendKeySet) {
            long appendOffset = appendIndex.getOffset(appendPrimaryValue);
            String appendBinlogLine = new String(appendPersistence.read(appendOffset));
            Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
            long appendBinlogPrimaryValue = appendBinlog.getPrimaryValue();
            long appendBinlogPrimaryOldValue = appendBinlog.getPrimaryOldValue();

            if ((appendBinlogPrimaryOldValue <= start || appendBinlogPrimaryOldValue >= end)
                && (appendBinlogPrimaryValue <= start || appendBinlogPrimaryValue >= end)) {
                // both not in,then do nothing
                baseIndex.delete(appendBinlogPrimaryOldValue);
                baseIndex.delete(appendBinlogPrimaryValue);
            } else {
                long baseOffset;
                if ((baseOffset = baseIndex.getOffset(appendBinlogPrimaryValue)) >= 0) {
                    // update other fields
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                    if (newBinlog != null) {
                        if (appendBinlogPrimaryValue != newBinlog.getPrimaryValue()) {
//                            System.out.println("delete primaryValue:" + appendBinlogPrimaryValue + "- :" + newBinlog.getPrimaryValue());
                            baseIndex.delete(appendBinlogPrimaryValue);
                        }
                        long offset = basePersistence.write(newBinlog.toBytes());
                        baseIndex.insert(newBinlog.getPrimaryValue(), offset);
                    } else {
                        baseIndex.delete(appendBinlogPrimaryValue);
                    }
                } else if ((baseOffset = baseIndex.getOffset(appendBinlogPrimaryOldValue)) >= 0) {
                    //update key field
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                    if (newBinlog != null) {
                        long offset = basePersistence.write(newBinlog.toBytes());
                        baseIndex.delete(appendBinlogPrimaryOldValue);
                        baseIndex.insert(appendBinlogPrimaryValue, offset);
                    } else {
                        baseIndex.delete(appendBinlogPrimaryOldValue);
                    }
                } else {
                    // if not in baseindex, then insert
                    long offset = basePersistence.write(appendBinlog.toBytes());
                    baseIndex.insert(appendBinlogPrimaryValue, offset);
                }
            }

        }

        for (long primaryValue = start + 1; primaryValue < end; primaryValue++) {
            long baseOffset = baseIndex.getOffset(primaryValue);
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

}
