package com.zbz;

import java.util.Set;

/**
 * Created by bgk on 6/14/17.
 */
public class InterFileReducer {
    private Index baseIndex, appendIndex;
    private Persistence basePersistence, appendPersistence;
    public InterFileReducer(Index baseIndex, Index appendIndex, Persistence basePersistence, Persistence appendPersistence) {
        this.baseIndex = baseIndex;
        this.appendIndex = appendIndex;
        this.basePersistence = basePersistence;
        this.appendPersistence = appendPersistence;
    }

    public void compute() {
        Set<Long> baseKeySet = baseIndex.getIndexHashMap().keySet();

        Set<Long> appendKeySet = appendIndex.getIndexHashMap().keySet();

        for (Long appendPrimaryValue : appendKeySet) {
            long appendOffset = appendIndex.getOffset(appendPrimaryValue);
            String appendBinlogLine = new String(basePersistence.read(appendOffset));
            Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
            long appendBinlogPrimaryValue = appendBinlog.getPrimaryValue();
            long appendBinlogPrimaryOldValue = appendBinlog.getPrimaryOldValue();
            long indexOffset;
            if ((indexOffset = baseIndex.getOffset(appendBinlogPrimaryValue)) >= 0) {
                // update other fields
                long baseOffset = baseIndex.getOffset(appendBinlogPrimaryValue);
                String baseBinlogLine = new String(basePersistence.read(baseOffset));
                Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                if (newBinlog != null) {
                    long offset = basePersistence.write(newBinlog.toBytes());
                    baseIndex.insert(appendBinlogPrimaryValue, offset);
                } else {
                    baseIndex.delete(appendBinlogPrimaryValue);
                }
                appendBinlogLine = null;
                baseBinlogLine = null;
                baseBinlog = null;
                appendBinlog = null;
                newBinlog = null;
            } else if ((indexOffset = baseIndex.getOffset(appendBinlogPrimaryOldValue)) >= 0) {
                //update key field
                long baseOffset = baseIndex.getOffset(appendBinlogPrimaryOldValue);
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
                appendBinlogLine = null;
                baseBinlogLine = null;
                baseBinlog = null;
                appendBinlog = null;
                newBinlog = null;
            } else {
                // if not in baseindex, then insert
                long offset = basePersistence.write(appendBinlog.toBytes());
                baseIndex.insert(appendBinlogPrimaryValue, offset);
                appendBinlogLine = null;
                appendBinlog = null;
            }
            appendIndex.delete(appendPrimaryValue);
        }

        appendIndex = null;
        appendPersistence = null;

    }

    public Index getBaseIndex() {
        return baseIndex;
    }

    public Persistence getBasePersistence() {
        return basePersistence;
    }
}
