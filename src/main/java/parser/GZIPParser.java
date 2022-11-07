package parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class GZIPParser {
    /**
     * Parse the FreeBase dump saved in gzip format, and save the result to output/parsed.csv.
     * @param path path to dump file
     */
    public void parse(String path) {
        System.out.println("Started parsing...");
        Instant startTime = Instant.now();
        HashMap< String, Person > map = new HashMap<>();

        try {
            /*
             * Open the gzip dump and create files for rows with alive and deceased people, names, dates of birth,
             * dates of death. Then create regexes for matching only required rows.
             */
            GZIPInputStream fin = new GZIPInputStream(new FileInputStream(path));
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

            /*
             * Iterate through dump by lines and for each line, try to match it to one of the categories using
             * previously created regexes. If the ID does not exist, create new record in HashMap with the ID and
             * empty Person object. If the ID is present in the HashMap, add the value to its respective variable
             * in the Person object with matching ID
             */
            while ((line = in.readLine()) != null) {
                String[] words = line.split("\t");

                if (!map.containsKey(words[0])) {
                    map.put(words[0], new Person(words[0], false));
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
            //Close files
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

        /*
         * Create new CSV file for parsed people and write header row. Next, iterate through the HashMap and write
         * Person which contains at least 4 out of 5 possible facts (ID, name, is deceased, date of birth, and
         * date of death), and if the Person is valid (if the person is deceased, it contains the date of death,
         * and the name and date of birth is not missing)
         */
        try {
            FileOutputStream fout = new FileOutputStream("output/parsed.csv");
            fout.write("id,name,type,is_deceased,date_of_birth,date_of_death\n".getBytes());

            for (Map.Entry<String, Person> mapEntry : map.entrySet()) {
                Person value = mapEntry.getValue();

                if (value.isValid() && value.size() >= 4) {
                    value.printPerson();
                    fout.write(value.toString().getBytes());
                }
            }

            fout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Instant endTime = Instant.now();
        System.out.println("Total execution time: " + Duration.between(startTime, endTime).toSeconds() + " seconds");
    }
}
