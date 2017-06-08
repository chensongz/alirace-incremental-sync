import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by bgk on 6/7/17.
 */
public class BTreeTest {
    public static void main(String[] args) throws IOException {
        String filename = "/home/bgk/middleware/test-data/canal.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
        String line;
        int i = 0;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            if (i++ >= 500) break;
        }
        bufferedReader.close();
    }

    public static void parseBinlog(String line) {

        String[] strings = line.split("|");

        String schema = strings[3];
        String table = strings[4];
        String operation = strings[5];

        Binlog binlog = new Binlog(schema, table, operation);

        String[] field = null;
        String fieldname;
        String beforeUpdateValue;
        String afterUpdateValue;
        boolean isPrimaryKey;
        int i = 6;
        while (i < strings.length) {
            field = strings[i++].split(":");
            beforeUpdateValue = strings[i++];
            afterUpdateValue = strings[i++];
            if (field[2] == "1") {
                isPrimaryKey = true;
            }
//            binlog.addField(new Field(field[0], ));

        }
    }


}
