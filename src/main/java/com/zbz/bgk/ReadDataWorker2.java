package com.zbz.bgk;

import com.zbz.Binlog;
import com.zbz.BinlogFactory;
import com.zbz.BinlogReducer;
import com.zbz.Index;
import com.zbz.zwy.Persistence;

import java.util.Set;

/**
 * Created by bgk on 6/14/17.
 */
public class ReadDataWorker2 {
    private Index baseIndex, appendIndex;
    private Persistence basePersistence, appendPersistence;

    public ReadDataWorker2(Index baseIndex, Index appendIndex, Persistence basePersistence, Persistence appendPersistence) {
        this.baseIndex = baseIndex;
        this.appendIndex = appendIndex;
        this.basePersistence = basePersistence;
        this.appendPersistence = appendPersistence;
    }

    public void compute() {
        Set<Long> baseKeySet = baseIndex.getIndexHashMap().keySet();

        for (Long appendPrimaryValue : appendIndex.getIndexHashMap().keySet()) {
            long appendOffset = appendIndex.getOffset(appendPrimaryValue);
            String appendBinlogLine = new String(basePersistence.read(appendOffset));
            Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
            long indexOffset;
            if ((indexOffset = baseIndex.getOffset(appendBinlog.getPrimaryValue())) >= 0) {
                // update other fields
                long baseOffset = baseIndex.getOffset(appendBinlog.getPrimaryValue());
                String baseBinlogLine = new String(basePersistence.read(baseOffset));
                Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                if (newBinlog != null) {
                    long offset = basePersistence.write(newBinlog.toBytes());
                    baseIndex.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    baseIndex.delete(baseBinlog.getPrimaryValue());
                }
            } else if ((indexOffset = baseIndex.getOffset(appendBinlog.getPrimaryOldValue())) >= 0) {
                //update key field
                long baseOffset = baseIndex.getOffset(appendBinlog.getPrimaryOldValue());
                String baseBinlogLine = new String(basePersistence.read(baseOffset));
                Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog);
                if (newBinlog != null) {
                    long offset = basePersistence.write(newBinlog.toBytes());
                    baseIndex.delete(baseBinlog.getPrimaryValue());
                    baseIndex.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    baseIndex.delete(baseBinlog.getPrimaryValue());
                }
            } else {
                // if not in baseindex, then insert
                long offset = basePersistence.write(appendBinlog.toBytes());
                baseIndex.insert(appendBinlog.getPrimaryValue(), offset);
            }
        }

    }

    public Index getBaseIndex() {
        return baseIndex;
    }

    public Persistence getBasePersistence() {
        return basePersistence;
    }
}
