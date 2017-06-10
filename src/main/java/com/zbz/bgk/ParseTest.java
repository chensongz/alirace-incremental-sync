package com.zbz.bgk;

import com.zbz.*;

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
        String filename = "/home/zwy/work/test/canal.txt";
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
                    case Binlog.I:
                        // from I to others
                        switch (newBinlog.getOperation()) {
                            case Binlog.U:
                                // update fields I -> U => I
                                updateOldBinlog(oldBinlog, newBinlog, Binlog.I, binlogHashMap);
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
                                updateOldBinlog(oldBinlog, newBinlog, Binlog.U, binlogHashMap);
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
                        updateOldBinlog(oldBinlog, newBinlog, Binlog.I, binlogHashMap);
                        break;
                    case Binlog.U:
                        // U -> U => U
                        updateOldBinlog(oldBinlog, newBinlog, Binlog.U, binlogHashMap);
                        break;
                    default:
                        break;
                }
            } else {
                binlogHashMap.put(newBinlog.getPrimaryValue(), newBinlog);
            }

            //****
            i++;
            if(i >= 1000) break;
        }

        reader.close();
        int j = 0;
//        for (Long pk : binlogHashMap.keySet()) {
//            j++;
//            System.out.println("primary key: " + pk);
//            System.out.println("operation:" + binlogHashMap.get(pk).getOperation());
//        }

        //add to database
        Database database = Database.getInstance();
        boolean init = false;
        for (Binlog binlog : binlogHashMap.values()) {
//            System.out.println(binlog.toString());
            byte op = binlog.getOperation();
            switch(op) {
                case Binlog.I:
                    if (!init) {
                        database.init(binlog);
                        init = true;
                    } else {
                        database.insert(binlog);
                    }
                    break;
                case Binlog.U:
                    database.update(binlog);
                    break;
                case Binlog.D:
                    database.delete(binlog);
                    break;
                default:
                    break;

            }
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
