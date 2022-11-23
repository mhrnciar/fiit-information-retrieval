import org.apache.lucene.document.Document;
import org.apache.lucene.search.TopDocs;

import indexer.LuceneIndexer;
import indexer.LuceneSearcher;
import parser.Person;
import parser.SparkParser;

import java.util.Scanner;

public class LuceneTester {
    /**
     * Dummy test to parse dump file using Spark.
     */
    public static void sparkParsingTest() {
        SparkParser parser = new SparkParser();

        parser.parse("data/freebase-head-1000000.gz", "output/spark-parsed");
    }

    /**
     * Test to validate creating new Lucene index.
     */
    public static void luceneIndexingTest() {
        String path = "output/spark-parsed";
        LuceneIndexer indexer = new LuceneIndexer(path, "index/luceneindex");
        indexer.createIndex();
    }

    /**
     * Test to validate searching the Lucene index and evaluating whether two people could meet.
     */
    public static void luceneSearchingTest() {
        LuceneSearcher searcher = new LuceneSearcher("index/luceneindex");

        Scanner scanner = new Scanner(System.in);
        String input;
        TopDocs hits;
        Document doc;

        // Cycle until at least one match is found
        do {
            System.out.print("Enter full name of first person: ");
            input = scanner.nextLine();

            hits = searcher.search(input);

            if (hits.scoreDocs.length == 0) {
                System.out.println("Entered name has not been found!");
            }
        } while (hits.scoreDocs.length == 0);

        // Get the data from index and print the parsed person
        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p1 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));
        p1.printPerson();

        // Repeat with second person
        do {
            System.out.print("Enter full name of second person: ");
            input = scanner.nextLine();

            hits = searcher.search(input);

            if (hits.scoreDocs.length == 0) {
                System.out.println("Entered name has not been found!");
            }
        } while (hits.scoreDocs.length == 0);

        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p2 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));
        p2.printPerson();

        if (p1.couldTheyMeet(p2)) {
            System.out.println("Yes, " + p1.getName() + " and " + p2.getName() + " could have met.");
        } else {
            System.out.println("No, " + p1.getName() + " and " + p2.getName() + " could not have met.");
        }
    }

    public static void main(String[] args) {
        // sparkParsingTest();

        // luceneIndexingTest();

        luceneSearchingTest();
    }
}
