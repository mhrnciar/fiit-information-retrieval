package indexer;

import java.io.*;
import java.util.HashMap;

public class Indexer {
    RandomAccessFile file;
    HashMap<String, Long> index;

    /**
     * Indexer constructor, which opens the file as RandomAccessFile, so the seek() can be executed in O(1). Index is
     * created as a HashMap, where key is the name of the people, and the value is the offset in the file.
     * @param fileName path to the CSV file with parsed people
     */
    public Indexer(String fileName) {
        try {
            file = new RandomAccessFile(fileName, "r");
            index = new HashMap<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create index from the file with parsed people, by iterating through the CSV file and at the beginning of each
     * line, save the parsed name and offset in the file to index.
     */
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

    /**
     * Search the index, by using the entered name to get the offset from HashMap index. If the name exists, HashMap
     * returns offset, which is used in seek() and the row on that position is returned, otherwise the function
     * returns null.
     * @param name name of the person
     * @return row from CSV file with found person, or null if the person has not been found
     */
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

    /**
     * Save index to file.
     * @param path path to where the index should be saved
     */
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

    /**
     * Read the index from file.
     * @param path path to where the index is saved
     */
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
