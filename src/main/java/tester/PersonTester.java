package tester;

import parser.Indexer;
import parser.Person;

import java.util.Scanner;

public class PersonTester {
    public static void comparisonTest() {
        Person p1 = new Person("m.aaaaaaa", "Matej Hrnciar", "people.person", "1999-12-10", false, null);
        Person p2 = new Person("m.bbbbbbb", "Stefan Kralovic", "people.deceased_person", "1822-10-17", true, "1893-03-03");
        Person p3 = new Person("m.ccccccc", "Blanka Pretrhnuta", "people.person", "1990-01-15", false, null);
        Person p4 = new Person("m.ddddddd", "Juraj Kovac", "people.deceased_person", "1779-06-27", true, "1834-09-01");

        System.out.println(p1.couldTheyMeet(p2));
        System.out.println(p2.couldTheyMeet(p4));
        System.out.println(p3.couldTheyMeet(p1));
        System.out.println(p3.couldTheyMeet(p4));
    }

    public static void indexingTest() {
        Indexer indexer = new Indexer("output/parsed.csv");
        indexer.createIndex();

        Person p1 = new Person(indexer.findRow("Chris Morgan"), true);
        Person p2 = new Person(indexer.findRow("Ted Ballard"), true);

        p1.printPerson();
        p2.printPerson();

        System.out.println(p1.couldTheyMeet(p2));

        indexer.saveIndex("index");
    }

    public static void consoleTest() {
        Indexer indexer = new Indexer("output/parsed.csv");
        // indexer.createIndex();
        indexer.readIndex("index/hashmap_index");

        Scanner scanner = new Scanner(System.in);
        String input, line;

        do {
            System.out.print("Enter full name of first person: ");
            input = scanner.nextLine();
            line = indexer.findRow(input);

            if (line == null) {
                System.out.println("Entered name has not been found!");
            }
        } while (line == null);

        Person p1 = new Person(line, true);
        p1.printPerson();

        do {
            System.out.print("Enter full name of second person: ");
            input = scanner.nextLine();
            line = indexer.findRow(input);

            if (line == null) {
                System.out.println("Entered name has not been found!");
            }
        } while (line == null);

        Person p2 = new Person(line, true);
        p2.printPerson();

        if (p1.couldTheyMeet(p2)) {
            System.out.println("Yes, " + p1.getName() + " and " + p2.getName() + " could have met.");
        } else {
            System.out.println("No, " + p1.getName() + " and " + p2.getName() + " could not have met.");
        }
    }


    public static void main(String[] args) {
        // comparisonTest();

        // indexingTest();

        consoleTest();
    }
}
