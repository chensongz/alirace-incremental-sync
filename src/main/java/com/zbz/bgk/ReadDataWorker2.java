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
        baseKeySet.retainAll(appendIndex.getIndexHashMap().keySet());

        for (Long appendPrimaryValue : appendIndex.getIndexHashMap().keySet()) {
            long appendOffset = appendIndex.getOffset(appendPrimaryValue);
            String appendBinlogLine = new String(basePersistence.read(appendOffset));
            Binlog appendBinlog = BinlogFactory.parse(appendBinlogLine);
            if (!baseKeySet.contains(appendPrimaryValue)) {
                // if not in baseindex, then insert
                long offset = basePersistence.write(appendBinlog.toBytes());
                baseIndex.insert(Long.parseLong(appendBinlog.getPrimaryKey()), offset);
            } else {
                long baseOffset = baseIndex.getOffset(appendPrimaryValue);
                String baseBinlogLine = new String(basePersistence.read(baseOffset));
                Binlog baseBinlog = BinlogFactory.parse(baseBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(baseBinlog, appendBinlog, appendBinlog.getOperation());
                long offset = basePersistence.write(newBinlog.toBytes());
                baseIndex.insert(Long.parseLong(appendBinlog.getPrimaryKey()), offset);
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
