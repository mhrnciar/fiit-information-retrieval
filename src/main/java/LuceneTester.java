import org.apache.lucene.document.Document;
import org.apache.lucene.search.TopDocs;

import indexer.LuceneIndexer;
import indexer.LuceneSearcher;
import parser.Person;
import parser.SparkParser;

import java.util.Scanner;

public class LuceneTester {
    public static void sparkParsingTest() {
        SparkParser parser = new SparkParser();

        parser.parse("data/freebase-head-10000000.gz");
    }

    public static void luceneIndexingTest() {
        // String path = "output/spark_parsed/part-00000-11d80915-9d10-4202-b5c7-6a9e1bedb88f-c000.csv";
        // LuceneIndexer indexer = new LuceneIndexer(path, "index/luceneindex");
        // indexer.createIndex();

        LuceneSearcher searcher = new LuceneSearcher("index/luceneindex");

        Scanner scanner = new Scanner(System.in);
        String input;
        TopDocs hits;
        Document doc;

        do {
            System.out.print("Enter full name of first person: ");
            input = scanner.nextLine();

            hits = searcher.search(input);

            if (hits.scoreDocs.length == 0) {
                System.out.println("Entered name has not been found!");
            }
        } while (hits.scoreDocs.length == 0);

        doc = searcher.getDocument(hits.scoreDocs[0]);
        Person p1 = new Person(doc.get("id"), doc.get("name"), doc.get("date_of_birth"), doc.get("is_deceased"), doc.get("date_of_death"));
        p1.printPerson();

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

        luceneIndexingTest();
    }
}

// David Cohen
// Margaret Waring

// Chris Morgan
// Alan Mansley

// Lars Olsson Smith
// Piotr
