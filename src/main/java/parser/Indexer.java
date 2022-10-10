package parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class Indexer {
    RandomAccessFile file;
    HashMap<String, Long> index;

    public Indexer(String fileName) {
        try {
            file = new RandomAccessFile(fileName, "r");
            index = new HashMap<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createIndex() {
        String line;
        try {
            long offset = file.getFilePointer();

            while ((line = file.readLine()) != null) {
                String[] cols = line.split(",");
                index.put(cols[1], offset);

                offset = file.getFilePointer();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String findRow(String name) {
        if (!index.containsKey(name)) {
            return null;
        }
        long offset = index.get(name);

        try {
            file.seek(offset);

            return file.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
