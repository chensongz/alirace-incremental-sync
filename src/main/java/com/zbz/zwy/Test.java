package com.zbz.zwy;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zwy on 17-6-16.
 */
public class Test {

    public static void main(String[] args) {
//        run0();
        run1();
    }

    public static void run0() {
        String[] fileName = {"canal00"};
        for(int j = 0 ;j < fileName.length; j++) {
            String dir = Constants.DATA_HOME + "/" + fileName[j];
            String dirName = Constants.MIDDLE_HOME + "/" + fileName[j] + "_";
            BinlogReducer binlogReducer = new BinlogReducer("", "");

            try {
                BufferedReader reader = new BufferedReader(new FileReader(dir));

                String line;
                long reduce = 0;
                long t1 = System.currentTimeMillis();
                while((line =  reader.readLine()) != null) {

                    int i = 0;
                    int cnt = 0;
                    int m = 0;
                    while(cnt < 5) {
                        i = line.indexOf('|', m);
                        m = i + 1;
                        cnt++;
                    }

                    line = line.substring(i + 1);
                    long t11 = System.currentTimeMillis();
//                    if(binlogReducer.isFull()) {
//                        binlogReducer.clearBinlogHashMap();
//                    }
                    binlogReducer.reduce(line);

                    long t12 = System.currentTimeMillis();
                    reduce += (t12 - t11);
                }
                long t2 = System.currentTimeMillis();
                System.out.println("Total time: " + (t2 - t1));
                System.out.println("Reduce time: " + reduce);
                System.out.println("Parse binlog: " + binlogReducer.getParseBinlogTime());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void run1() {

        String[] fileName = {"canal00"};
        for(int k = 0 ;k < fileName.length; k++) {
            String dir = Constants.DATA_HOME + "/" + fileName[k];
            String dirName = Constants.MIDDLE_HOME + "/" + fileName[k] + "_";
            BinlogReducer binlogReducer = new BinlogReducer("", "");

            FileChannel fc = null;
            long reduce = 0;
            long parse = 0;
            try {
                fc = new RandomAccessFile(dir, "rw").getChannel();
                MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size());

                long t1 = System.currentTimeMillis();
                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                long parseT1 = 0, parseT2 = 0;
                while(buf.remaining() > 0) {
                    bao.reset();
                    byte b = buf.get();
                    boolean endline = false;
                    int fieldCnt = 0;
                    Binlog binlog = new Binlog();
                    parseT1 = System.currentTimeMillis();
                    while(!endline) {
                        b = buf.get();
                        if(b == '\n') {
                            endline = true;
                        } else if(b == '|') {
                            String fieldString = bao.toString();
                            bao.reset();
                            fieldCnt++;

                            if(fieldCnt == 5) {
                                switch (fieldString) {
                                    case "I":
                                        binlog.setOperation(Binlog.I);
                                        break;
                                    case "U":
                                        binlog.setOperation(Binlog.U);
                                        break;
                                    case "D":
                                        binlog.setOperation(Binlog.D);
                                        break;

                                }
                            } else if(fieldCnt > 5) {

                                int j;

                                j = fieldString.indexOf(':');
                                String fieldname = fieldString.substring(0, j);
                                fieldString = fieldString.substring(j + 1);
                                j = fieldString.indexOf(':');
                                String isPrimaryKey = fieldString.substring(j + 1);

                                byte bb;
                                while((bb = buf.get()) != '|') {
                                    bao.write(bb);
                                }
                                String oldValue = bao.toString();
                                bao.reset();
                                while((bb = buf.get()) != '|') {
                                    bao.write(bb);
                                }
                                String newValue = bao.toString();
                                bao.reset();

                                if (isPrimaryKey.equals("1")) {
                                    // if field is primary key
                                    binlog.setPrimaryKey(fieldname);
                                    if (binlog.getOperation() == Binlog.D) {
                                        // if delete operation
                                        binlog.setPrimaryOldValue(newValue);
                                        binlog.setPrimaryValue(oldValue);
                                    } else {
                                        binlog.setPrimaryOldValue(oldValue);
                                        binlog.setPrimaryValue(newValue);
                                    }
                                } else {
                                    // if field is not primary key
                                    binlog.addField(fieldname, newValue);
                                }
                            }
                        } else {
                            bao.write(b);
                        }
                    }
                    parseT2 = System.currentTimeMillis();

                    long t11 = System.currentTimeMillis();
                    binlogReducer.reduce(binlog);
                    long t12 = System.currentTimeMillis();
                    reduce += (t12 - t11);
                    parse += (parseT2 - parseT1);
                }
                long t2 = System.currentTimeMillis();
                System.out.println("Total time: " + (t2 - t1));
                System.out.println("Reduce time: " + reduce);
                System.out.println("Parse and create Binlog time: " + parse);
                System.out.println("HashMap size: " + binlogReducer.getBinlogHashMap().size());
                t1 = System.currentTimeMillis();
                clearBinlogReducer(binlogReducer, dirName);
                t2 = System.currentTimeMillis();
                System.out.println("Persistence time: " + (t2 - t1));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void clearBinlogReducer(BinlogReducer binlogReducer, String name) {

        HashIndex index = new HashIndex();
        Persistence persistence = new Persistence(name);

        for (Object binlog : binlogReducer.getBinlogHashMap().values()) {
            long indexOffset;
            long primaryOldValue = ((Binlog)binlog).getPrimaryOldValue();
            long primaryValue = ((Binlog)binlog).getPrimaryValue();
            if ((indexOffset = index.getOffset(primaryValue)) >= 0) {
                // update other value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, ((Binlog)binlog));
                if (newBinlog != null) {
                    if (primaryValue != newBinlog.getPrimaryValue()) {
                        index.delete(primaryValue);
                    }
                    long offset = persistence.write(newBinlog.toBytes());
                    index.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    index.delete(primaryValue);
                }
            } else if ((indexOffset = index.getOffset(primaryOldValue)) >= 0) {
                // update key value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, ((Binlog)binlog));
                if (newBinlog != null) {
                    long offset = persistence.write(newBinlog.toBytes());
                    index.delete(primaryOldValue);
                    index.insert(primaryValue, offset);
                } else {
                    index.delete(primaryOldValue);
                }
            } else {
                long offset = persistence.write(((Binlog)binlog).toBytes());
                index.insert(primaryValue, offset);
            }

        }
        binlogReducer.clearBinlogHashMap();
    }
}
