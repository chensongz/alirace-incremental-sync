package com.zbz.bgk;

import com.zbz.Binlog;
import com.zbz.BinlogFactory;
import com.zbz.BinlogPool;
import com.zbz.Field;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
        HashMap<Long, Binlog> binlogHashMap = new HashMap<>();
        String filename = "../test-data/canal.txt";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        int i = 0;
        while ((line = reader.readLine()) != null) {
            // *****
            Binlog newBinlog = BinlogFactory.createBinlog(line);
            if (binlogHashMap.containsKey(newBinlog.getPrimaryValue())) {
                // maybe insert record or update fields or delete record
                Binlog oldBinlog = binlogHashMap.get(newBinlog.getPrimaryValue());
                switch (oldBinlog.getOperation()) {
                    case 1:
                        // from I to others
                        switch (newBinlog.getOperation()) {
                            case 2:
                                // update fields I -> U => I
                                updateOldBinlog(oldBinlog, newBinlog, (byte)1, binlogHashMap);
                                break;
                            case 3:
                                // delete record I -> D => null
                                binlogHashMap.remove(oldBinlog.getPrimaryValue());
                                break;
                            default:
                                break;
                        }
                        break;
                    case 2:
                        // from U to others
                        switch (newBinlog.getOperation()) {
                            case 2:
                                // U -> U => U
                                updateOldBinlog(oldBinlog, newBinlog, (byte)2, binlogHashMap);
                                break;
                            case 3:
                                // U -> D => D
                                oldBinlog.setOperation((byte)3);
                                binlogHashMap.put(oldBinlog.getPrimaryValue(), newBinlog);
                                break;
                            default:
                                break;
                        }
                        break;
                    case 3:
                        // from D to others
                        switch (newBinlog.getOperation()) {
                            case 1:
                                // D -> I => U
                                oldBinlog.setOperation((byte)2);
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
                    case 1:
                        // I -> U => I
                        updateOldBinlog(oldBinlog, newBinlog, (byte)1, binlogHashMap);
                        break;
                    case 2:
                        // U -> U => U
                        updateOldBinlog(oldBinlog, newBinlog, (byte)2, binlogHashMap);
                        break;
                    default:
                        break;
                }
            } else {
                binlogHashMap.put(newBinlog.getPrimaryValue(), newBinlog);
            }

            //****
            i++;
            if(i >= 1000000) break;
        }

        reader.close();
        int j = 0;
//        for (Long pk : binlogHashMap.keySet()) {
//            j++;
//            System.out.println("primary key: " + pk);
//            System.out.println("operation:" + binlogHashMap.get(pk).getOperation());
//        }

        for (Binlog binlog : binlogHashMap.values()) {
            System.out.println(binlog.toString());
        }
        System.out.println("total lines: " + j);

    }

    public static void updateOldBinlog(Binlog oldBinlog, Binlog newBinlog, byte Operation, HashMap<Long, Binlog> binlogHashMap) {
        oldBinlog.setOperation(Operation);
        oldBinlog.setPrimaryValue(String.valueOf(newBinlog.getPrimaryValue()));
        for (Field field: newBinlog.getFields().values()) {
            oldBinlog.addField(field);
        }
        binlogHashMap.put(oldBinlog.getPrimaryValue(), oldBinlog);
    }
}
