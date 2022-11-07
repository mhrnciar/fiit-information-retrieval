package indexer;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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

    /**
     * Constructor for Lucene index searcher.
     * @param indexDirectoryPath path to the index directory
     */
    public LuceneSearcher(String indexDirectoryPath) {
        try {
            Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath).toPath());
            indexSearcher = new IndexSearcher(DirectoryReader.open(indexDirectory));
            queryParser = new QueryParser("name", new StandardAnalyzer());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Search the index names for match.
     * @param searchQuery name of searched Person
     * @return list of matched records
     */
    public TopDocs search(String searchQuery) {
        try {
            query = queryParser.parse(searchQuery);
            return indexSearcher.search(query, 10);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Get the Person's data from the index.
     * @param scoreDoc Document of matched Person
     * @return Person's data
     */
    public Document getDocument(ScoreDoc scoreDoc) {
        try {
            return indexSearcher.doc(scoreDoc.doc);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}