package indexer;

import java.io.*;
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

    public void saveIndex(String path) {
        try {
            File file = new File(path + "/hashmap_index");
            FileOutputStream fstream = new FileOutputStream(file);
            ObjectOutputStream fout = new ObjectOutputStream(fstream);

            fout.writeObject(index);
            fout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readIndex(String path) {
        try {
            File file = new File(path);
            FileInputStream fstream = new FileInputStream(file);
            ObjectInputStream fin = new ObjectInputStream(fstream);

            this.index = (HashMap<String, Long>) fin.readObject();
            fin.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
