package parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPParser {
    public static void main(String[] args) {
        String gzipFile = "data/freebase-head-10000000.gz";

        decompressGzipFile(gzipFile);
    }

    private static void decompressGzipFile(String gzipFile) {
        System.out.println("Started parsing...");
        Instant startTime = Instant.now();
        HashMap< String, Person > map = new HashMap<>();

        try {
            GZIPInputStream fin = new GZIPInputStream(new FileInputStream(gzipFile));
            FileOutputStream names = new FileOutputStream("output/names.txt");
            FileOutputStream people = new FileOutputStream("output/people.txt");
            FileOutputStream deceased = new FileOutputStream("output/deceased.txt");
            FileOutputStream births = new FileOutputStream("output/births.txt");
            FileOutputStream deaths = new FileOutputStream("output/deaths.txt");
            BufferedReader in = new BufferedReader(new InputStreamReader(fin, StandardCharsets.UTF_8));

            Pattern REGEX_OBJECT_NAME = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/type.object.name>.*");
            Pattern REGEX_OBJECT_PERSON = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/people\\.person>.*");
            Pattern REGEX_OBJECT_DEAD_PERSON = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/people\\.deceased_person>.*");
            Pattern REGEX_DATE_OF_BIRTH = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/people\\.person\\.date_of_birth>.*");
            Pattern REGEX_DATE_OF_DEATH = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/people\\.deceased_person\\.date_of_death>.*");

            String line;

            while ((line = in.readLine()) != null) {
                String[] words = line.split("\t");

                if (!map.containsKey(words[0])) {
                    map.put(words[0], new Person(words[0]));
                }

                if (REGEX_OBJECT_NAME.matcher(line).matches()) {
                    names.write(line.getBytes());
                    names.write('\n');

                    map.get(words[0]).setName(words[2]);
                }
                if (REGEX_OBJECT_PERSON.matcher(line).matches()) {
                    people.write(line.getBytes());
                    people.write('\n');

                    map.get(words[0]).setType(words[2]);
                }
                if (REGEX_OBJECT_DEAD_PERSON.matcher(line).matches()) {
                    deceased.write(line.getBytes());
                    deceased.write('\n');

                    map.get(words[0]).setType(words[2]);
                }
                if (REGEX_DATE_OF_BIRTH.matcher(line).matches()) {
                    births.write(line.getBytes());
                    births.write('\n');

                    map.get(words[0]).setDateOfBirth(words[2]);
                }
                if (REGEX_DATE_OF_DEATH.matcher(line).matches()) {
                    deaths.write(line.getBytes());
                    deaths.write('\n');

                    map.get(words[0]).setDeceased(true);
                    map.get(words[0]).setDateOfDeath(words[2]);
                }
            }
            //close resources
            names.close();
            people.close();
            deceased.close();
            births.close();
            deaths.close();
            fin.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Parsing complete...");

        for (Map.Entry<String, Person> mapEntry : map.entrySet()) {
            Person value = mapEntry.getValue();

            if (value.isValid() && value.size() >= 4) {
                value.printPerson();
            }
        }

        Instant endTime = Instant.now();
        System.out.println("Total execution time: " + Duration.between(startTime, endTime).toSeconds() + " seconds");
    }
}
