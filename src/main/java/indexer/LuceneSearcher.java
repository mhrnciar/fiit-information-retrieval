package indexer;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LuceneSearcher {

    IndexSearcher indexSearcher;
    QueryParser queryParser;
    Query query;

    public LuceneSearcher(String indexDirectoryPath) {
        try {
            Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath).toPath());
            indexSearcher = new IndexSearcher(DirectoryReader.open(indexDirectory));
            queryParser = new QueryParser("name", new StandardAnalyzer());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TopDocs search( String searchQuery) {
        try {
            query = queryParser.parse(searchQuery);
            return indexSearcher.search(query, 10);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Document getDocument(ScoreDoc scoreDoc) {
        try {
            return indexSearcher.doc(scoreDoc.doc);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}