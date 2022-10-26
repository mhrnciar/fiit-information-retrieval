package indexer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

public class LuceneIndexer {
    public IndexWriter writer;

    public LuceneIndexer(String indexDirectoryPath) throws IOException {
        //this directory will contain the indexes
        Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath).toPath());

        //create the indexer
        writer = new IndexWriter(indexDirectory, new IndexWriterConfig(new StandardAnalyzer()));
    }

    public void close() throws IOException {
        writer.close();
    }

    private Document getDocument(String id, String name, String dateOfBirth, String dateOfDeath) throws IOException {
        Document document = new Document();

        //index file contents
        TextField idField = new TextField("id", id, TextField.Store.YES);
        //index file name
        TextField nameField = new TextField("name", name, TextField.Store.YES);
        //index file path
        TextField dateOfBirthField = new TextField("date_of_birth", dateOfBirth, TextField.Store.YES);
        TextField dateOfDeathField = new TextField("date_of_death", dateOfDeath, TextField.Store.YES);

        document.add(idField);
        document.add(nameField);
        document.add(dateOfBirthField);
        document.add(dateOfDeathField);

        return document;
    }

    public void addToIndex(Row row) {
        String id = row.get(0).toString();
        String name = row.get(0).toString();
        String datoOfBirth = row.get(0).toString();
        String dateOfDeath = row.get(0).toString();

        try {
            Document doc = getDocument(id, name, datoOfBirth, dateOfDeath);
            writer.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
