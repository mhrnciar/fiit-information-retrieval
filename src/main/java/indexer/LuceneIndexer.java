package indexer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LuceneIndexer {
    private IndexWriter writer;
    private File dirPath;

    /**
     * Constructor for Lucene indexer.
     * @param dirPath path to directory with CSV files
     * @param indexDirectoryPath path to directory where index will be stored
     */
    public LuceneIndexer(String dirPath, String indexDirectoryPath) {
        try {
            Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath).toPath());
            writer = new IndexWriter(indexDirectory, new IndexWriterConfig(new StandardAnalyzer()));

            this.dirPath = new File(dirPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create Lucene index of all the CSV files in dirPath. Iterate through lines and for each entity, create new
     * document and write it to the index.
     */
    public void createIndex() {
        String line;
        RandomAccessFile file;
        File[] paths = dirPath.listFiles();

        try {
            assert paths != null;
            for (File path : paths) {
                file = new RandomAccessFile(path.getAbsolutePath(), "r");
                file.readLine();

                while ((line = file.readLine()) != null) {
                    String[] cols = line.split(",");
                    Document doc;

                    if (cols.length == 4) {
                        doc = getDocument(cols[0], cols[2], cols[3], null);
                    }
                    else {
                        doc = getDocument(cols[0], cols[2], cols[3], cols[4]);
                    }

                    writer.addDocument(doc);
                }

                file.close();
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create new Document object with information about a Person.
     * @param id Person's ID
     * @param name full name
     * @param dateOfBirth date of birth
     * @param dateOfDeath date of death that may be null, in that case the person is set as alive
     * @return Document object of the Person
     */
    private Document getDocument(String id, String name, String dateOfBirth, String dateOfDeath) {
        Document document = new Document();

        TextField idField = new TextField("id", id, TextField.Store.YES);
        TextField nameField = new TextField("name", name, TextField.Store.YES);
        TextField dateOfBirthField = new TextField("date_of_birth", dateOfBirth, TextField.Store.YES);

        TextField isDeceasedField;
        TextField dateOfDeathField;

        /*
         * If the date of death is null, the Person is set as alive and to date of death is
         * saved null; if the date is not empty, the Person is set to deceased
         */
        if (dateOfDeath == null) {
            isDeceasedField = new TextField("is_deceased", "false", TextField.Store.YES);
            dateOfDeathField = new TextField("date_of_death", "null", TextField.Store.YES);
        }
        else {
            isDeceasedField = new TextField("is_deceased", "true", TextField.Store.YES);
            dateOfDeathField = new TextField("date_of_death", dateOfDeath, TextField.Store.YES);
        }

        document.add(idField);
        document.add(nameField);
        document.add(isDeceasedField);
        document.add(dateOfBirthField);
        document.add(dateOfDeathField);

        return document;
    }
}
