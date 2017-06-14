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
        Set<Long> baseKeySet = baseIndex.getIndexHashMap().keySet();
        Set<Long> appendKeySet = appendIndex.getIndexHashMap().keySet();

        for (long primaryValue = start; primaryValue < end; primaryValue++) {
            if (appendKeySet.contains(primaryValue)) {
                // if second file contains the primary value, then update and send to pool
                long appendOffset = appendIndex.getOffset(primaryValue);
                String appendBinlogLine = new String(appendPersistence.read(appendOffset));
                Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
                long newBaseOffset = baseIndex.getOffset(appendBinlog.getPrimaryValue());
                long oldBaseOffset = baseIndex.getOffset(appendBinlog.getPrimaryOldValue());
                long baseOffset = newBaseOffset >= 0 ? newBaseOffset : oldBaseOffset;
                if (baseOffset < 0) {
                    // if not in baseindex, then send
                    sendPool.put(appendBinlog.toSendString());
                } else {
                    // after update, then send
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                    if (newBinlog != null) {
                        sendPool.put(newBinlog.toSendString());
                    }
                }
            } else {
                // if first file contains the primary value, then send to pool
                if (baseKeySet.contains(primaryValue)) {
                    long baseOffset = baseIndex.getOffset(primaryValue);
                    String baseBinlogLine = new String(basePersistence.read(baseOffset));
                    Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                    sendPool.put(baseBinlog.toSendString());
                }
            }
        }
        sendPool.put("NULL");

        baseIndex = null;
        basePersistence = null;
        appendIndex = null;
        appendPersistence = null;
    }
}
