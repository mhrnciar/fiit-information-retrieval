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
import java.util.Objects;

public class LuceneIndexer {
    public IndexWriter writer;
    public RandomAccessFile file;

    public LuceneIndexer(String filePath, String indexDirectoryPath) {
        try {
            Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath).toPath());
            writer = new IndexWriter(indexDirectory, new IndexWriterConfig(new StandardAnalyzer()));

            file = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createIndex() {
        String line;
        try {
            while ((line = file.readLine()) != null) {
                String[] cols = line.split(",");
                Document doc = getDocument(cols[0], cols[2], cols[3], cols[4]);

                writer.addDocument(doc);
            }
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Document getDocument(String id, String name, String dateOfBirth, String dateOfDeath) {
        Document document = new Document();

        TextField idField = new TextField("id", id, TextField.Store.YES);
        TextField nameField = new TextField("name", name, TextField.Store.YES);
        TextField dateOfBirthField = new TextField("date_of_birth", dateOfBirth, TextField.Store.YES);

        TextField isDeceasedField;
        TextField dateOfDeathField;
        if (dateOfDeath.equals("\"\"")) {
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

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
