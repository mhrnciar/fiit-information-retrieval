package parser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Person {
    private String id;
    private String rawId;
    private String name;
    private String type;
    private Date dateOfBirth;
    private Date dateOfDeath;
    private boolean deceased;
    private int size;

    // List of formats the date of birth or date of death can be in
    static private final SimpleDateFormat[] knownPatterns = {
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy-MM"),
            new SimpleDateFormat("yyyy")
    };

    // Format to export the dates in
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Constructor of Person object, either from CSV record, or empty with only ID.
     * @param str CSV record if csv is set to true, otherwise ID
     * @param csv true if the passed string is CSV record
     */
    public Person(String str, boolean csv) {
        if (!csv) {
            setId(str);
            setDeceased(false);
            this.size = 2;
        }
        else {
            String[] columns = str.split(",");
            this.id = columns[0];
            this.deceased = columns[3].equals("true");
            this.size = 2;

            this.name = columns[1];
            this.type = columns[2];

            // Iterate through date formats and try to parse the dates
            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfBirth = pattern.parse(columns[4]);
                    break;
                } catch (ParseException ignored) { }
            }

            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfDeath = pattern.parse(columns[5]);
                    break;
                } catch (ParseException ignored) { }
            }
        }
    }

    /**
     * Constructor of Person object with all data.
     * @param id Person's ID
     * @param name full name
     * @param type type of entity, either people.person, or people.deceased_person
     * @param dateOfBirth date of birth
     * @param deceased boolean whether the person is deceased or not
     * @param dateOfDeath date of death, which is assigned only if the deceased argument is set to true
     */
    public Person(String id, String name, String type, String dateOfBirth, boolean deceased, String dateOfDeath) {
        this.id = id;
        this.size = 2;

        this.name = name;
        this.type = type;

        // Iterate through date formats and try to parse the dates
        for (SimpleDateFormat pattern : knownPatterns) {
            try {
                this.dateOfBirth = pattern.parse(dateOfBirth);
                break;
            } catch (ParseException ignored) { }
        }

        this.deceased = deceased;
        if (deceased) {
            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfDeath = pattern.parse(dateOfDeath);
                    break;
                } catch (ParseException ignored) { }
            }
        }
    }

    /**
     * Constructor of Person object for parsing from Lucene index, which cannot store boolean values, so the
     * deceased argument is passed as string.
     * @param id Person's ID
     * @param name full name
     * @param dateOfBirth date of birth
     * @param deceased true if the person is deceased, false if the person is alive
     * @param dateOfDeath date of death, which is assigned only if the deceased argument is set to true
     */
    public Person(String id, String name, String dateOfBirth, String deceased, String dateOfDeath) {
        this.id = id;
        this.size = 2;

        this.name = name;
        this.type = "people.person";

        for (SimpleDateFormat pattern : knownPatterns) {
            try {
                this.dateOfBirth = pattern.parse(dateOfBirth);
                break;
            } catch (ParseException ignored) { }
        }

        this.deceased = deceased.equals("true");
        if (this.deceased) {
            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfDeath = pattern.parse(dateOfDeath);
                    break;
                } catch (ParseException ignored) { }
            }
        }
    }

    /**
     * Return the number of facts the person contains.
     * @return number of facts, up to 5
     */
    public int size() {
        return size;
    }

    /**
     * ID getter.
     * @return Person's ID
     */
    public String getId() {
        return id;
    }

    /**
     * Extract ID from RDF using regex and save it to person.
     * @param id string containing person's ID
     */
    public void setId(String id) {
        Pattern MATCH_ID = Pattern.compile("<\\w+[:/]+[a-zA-Z.]+/\\w+/(.\\.\\w+)>");
        Matcher matcher = MATCH_ID.matcher(id);
        if (matcher.find()) {
            this.id = matcher.group(1);
        }
        this.rawId = id;
    }

    /**
     * Name getter.
     * @return Person's full name
     */
    public String getName() {
        return name;
    }

    /**
     * Extract name from RDF using regex and save it to person.
     * @param name string containing person's full name
     */
    public void setName(String name) {
        Pattern MATCH_NAME = Pattern.compile("\"((\\w+[ ]*)*)\"@(\\w+)");
        Matcher matcher = MATCH_NAME.matcher(name);
        if (matcher.find()) {
            this.name = matcher.group(1);
        }
        size++;
    }

    /**
     * Type getter.
     * @return Person's type
     */
    public String getType() {
        return type;
    }

    /**
     * Extract type from RDF using regex and save it to person.
     * @param type string containing person's type
     */
    public void setType(String type) {
        Pattern MATCH_TYPE = Pattern.compile("<\\w+[:/]+[a-zA-Z.]+/\\w+/([a-zA-Z._]+)>");
        Matcher matcher = MATCH_TYPE.matcher(type);
        if (matcher.find()) {
            this.type = matcher.group(1);
        }
        size++;
    }

    /**
     * Date of birth getter.
     * @return Person's date of birth
     */
    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    /**
     * Extract date of birth from RDF using regex, convert it to Date object, and save it to person.
     * @param dateOfBirth string containing person's date of birth
     */
    public void setDateOfBirth(String dateOfBirth) {
        Pattern MATCH_DOB = Pattern.compile("\"((\\d+[:\\-/]*)+)\".*");
        Matcher matcher = MATCH_DOB.matcher(dateOfBirth);
        if (matcher.find()) {
            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfBirth = pattern.parse(matcher.group(1));
                    break;
                } catch (ParseException ignored) { }
            }
        }
        size++;
    }

    /**
     * Date of death getter.
     * @return Person's date of death
     */
    public Date getDateOfDeath() {
        return dateOfDeath;
    }

    /**
     * Extract date of death from RDF using regex, convert it to Date object, and save it to person.
     * @param dateOfDeath string containing person's date of death
     */
    public void setDateOfDeath(String dateOfDeath) {
        Pattern MATCH_DOD = Pattern.compile("\"((\\d+[:\\-/]*)+)\".*");
        Matcher matcher = MATCH_DOD.matcher(dateOfDeath);
        if (matcher.find()) {
            for (SimpleDateFormat pattern : knownPatterns) {
                try {
                    this.dateOfDeath = pattern.parse(matcher.group(1));
                    break;
                } catch (ParseException ignored) { }
            }
        }
        if (this.dateOfDeath != null) {
            size++;
        }
    }

    /**
     * Deceased indicator getter.
     * @return true if the person is deceased, false if the person is alive
     */
    public boolean isDeceased() {
        return deceased;
    }

    /**
     * Deceased indicator setter.
     * @param deceased indicator whether the person is deceased, or not
     */
    public void setDeceased(boolean deceased) {
        this.deceased = deceased;
    }

    /**
     * Print the person in nice format.
     */
    public void printPerson() {
        StringBuilder str = new StringBuilder(getId() + ": " + getName() +
                ",\n\tDeceased: " + isDeceased() +
                ",\n\tDate of birth: " + df.format(getDateOfBirth()) +
                ",\n\tDate of death: ");

        if (getDateOfDeath() != null) {
            str.append(df.format(getDateOfDeath()));
        }
        else {
            str.append("null");
        }

        System.out.println(str);
        System.out.println();
    }

    /**
     * Convert the information about person to CSV record.
     * @return CSV string containing person's records
     */
    public String toString() {
        StringBuilder str = new StringBuilder(getId() + "," +
                getName() + "," +
                getType() + "," +
                isDeceased() + "," +
                df.format(getDateOfBirth()) + ",");

        if (getDateOfDeath() != null) {
            str.append(df.format(getDateOfDeath()));
        }
        else {
            str.append("null");
        }

        str.append("\n");

        return str.toString();
    }

    /**
     * Check validity of the person.
     * @return true if the person is valid, false if the person is missing name, date of birth, or date of death, in
     * case it was set as deceased
     */
    public boolean isValid() {
        if (isDeceased() && getDateOfDeath() == null) {
            return false;
        }
        return getDateOfBirth() != null && getName() != null;
    }

    /**
     * Check, whether this and another person could have met
     * @param p object of another person
     * @return true if the two people could have met, false if they couldn't
     */
    public boolean couldTheyMeet(Person p) {
        // If they are both alive, they could have, or still can meet
        if (!this.isDeceased() && !p.isDeceased()) {
            return true;
        }
        // If 'this' person is deceased, search for intersection in the 'right' interval
        else if (this.isDeceased() && !p.isDeceased()) {
            return intersect(p, "right");
        }
        // If 'another' person is deceased, search for intersection in the 'left' interval
        else if (!this.isDeceased() && p.isDeceased()) {
            return intersect(p, "left");
        }
        // If they are both deceased, search for intersection in both intervals
        else {
            return intersect(p, "both");
        }
    }

    /**
     * Check intersection of the two peoples' dates of birth and dates of death
     * @param p another person
     * @param how which person is alive
     * @return true if their lifetimes overlap, false if they don't
     */
    private boolean intersect(Person p, String how) {
        if (how.equals("right")) {
            return this.getDateOfDeath().after(p.getDateOfBirth());
        }
        else if (how.equals("left")) {
            return this.getDateOfBirth().before(p.getDateOfDeath());
        }
        else {
            return this.getDateOfBirth().before(p.getDateOfDeath()) && this.getDateOfDeath().after(p.getDateOfBirth());
        }
    }
}
