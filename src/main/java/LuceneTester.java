import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
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

    private static Document performSearch(LuceneSearcher searcher, String which) {
        Scanner scanner = new Scanner(System.in);
        String input;
        TopDocs hits;

        // Cycle until at least one match is found
        do {
            System.out.print("Enter full name of " + which + " person: ");
            input = scanner.nextLine();

            hits = searcher.search(input);

            if (hits.scoreDocs.length == 0) {
                System.out.println("Entered name has not been found!");
            }
        } while (hits.scoreDocs.length == 0);

        if (hits.scoreDocs.length == 1) {
            return searcher.getDocument(hits.scoreDocs[0]);
        }
        // If there are multiple matches, give option to select from the top 10
        else {
            System.out.println("Found " + hits.scoreDocs.length + " hits:");
            int i = 1;

            for (ScoreDoc hit : hits.scoreDocs) {
                Document doc = searcher.getDocument(hit);
                Person p = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

                System.out.println(i++ + ".");
                p.printPerson();
            }

            int choice;

            do {
                System.out.print("Choose the person from list: ");
                choice = scanner.nextInt();

                if (choice < 1 || choice > hits.scoreDocs.length) {
                    System.out.println("Number out of bounds!");
                }
            } while (choice < 1 || choice > hits.scoreDocs.length);

            return searcher.getDocument(hits.scoreDocs[choice - 1]);
        }
    }

    /**
     * Test to validate searching the Lucene index and evaluating whether two people could meet.
     */
    public static void luceneSearchingTest() {
        LuceneSearcher searcher = new LuceneSearcher("index/luceneindex");

        Document doc = performSearch(searcher, "first");
        Person p1 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        doc = performSearch(searcher, "second");
        Person p2 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        p1.printPerson();
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
