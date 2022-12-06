import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import indexer.LuceneIndexer;
import indexer.LuceneSearcher;
import parser.Person;
import parser.SparkParser;

import java.util.Scanner;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneTester {
    /**
     * Test to validate index searching and evaluation.
     */
    @Test
    public void luceneSearchTest() {
        LuceneSearcher searcher = new LuceneSearcher("index/luceneindex");

        TopDocs hits = searcher.search("Elon Musk");
        Document doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p1 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        hits = searcher.search("Chuck Norris");
        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p2 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        hits = searcher.search("Ulrich Geisser");
        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p3 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        hits = searcher.search("Piotr");
        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p4 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));

        assertTrue(p1.couldTheyMeet(p2));
        assertTrue(p2.couldTheyMeet(p4));
        assertFalse(p3.couldTheyMeet(p1));
        assertFalse(p3.couldTheyMeet(p4));
    }

    /**
     * Parse dump file using Spark.
     */
    public static void parseSpark() {
        SparkParser parser = new SparkParser();

        parser.parse("data/freebase-head-1000000.gz", "output/spark-parsed");
    }

    /**
     * Create new Lucene index.
     */
    public static void createLuceneIndex() {
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
     * Search the Lucene index and evaluate whether two people could meet.
     */
    public static void searchLuceneIndex() {
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
        // parseSpark();

        // createLuceneIndex();

        searchLuceneIndex();
    }
}
