package parser;

import java.text.DateFormat;
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

    static private final SimpleDateFormat[] knownPatterns = {
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy-MM"),
            new SimpleDateFormat("yyyy")
    };

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

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

    public Person(String id, String name, String type, String dateOfBirth, boolean deceased, String dateOfDeath) {
        this.id = id;
        this.size = 2;

        this.name = name;
        this.type = type;

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

    public int size() {
        return size;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        Pattern MATCH_ID = Pattern.compile("<\\w+[:/]+[a-zA-Z.]+/\\w+/(.\\.\\w+)>");
        Matcher matcher = MATCH_ID.matcher(id);
        if (matcher.find()) {
            this.id = matcher.group(1);
        }
        this.rawId = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        Pattern MATCH_NAME = Pattern.compile("\"((\\w+[ ]*)*)\"@(\\w+)");
        Matcher matcher = MATCH_NAME.matcher(name);
        if (matcher.find()) {
            this.name = matcher.group(1);
        }
        size++;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        Pattern MATCH_TYPE = Pattern.compile("<\\w+[:/]+[a-zA-Z.]+/\\w+/([a-zA-Z._]+)>");
        Matcher matcher = MATCH_TYPE.matcher(type);
        if (matcher.find()) {
            this.type = matcher.group(1);
        }
        size++;
    }

    public Date getDateOfBirth() {
        return dateOfBirth;
    }

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

    public Date getDateOfDeath() {
        return dateOfDeath;
    }

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

    public boolean isDeceased() {
        return deceased;
    }

    public void setDeceased(boolean deceased) {
        this.deceased = deceased;
    }

    public void printPerson() {
        StringBuilder str = new StringBuilder(getId() + ": " + getName() +
                ",\n\tType: " + getType() +
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

    public boolean isValid() {
        if (isDeceased() && getDateOfDeath() == null) {
            return false;
        }
        return getDateOfBirth() != null && getName() != null;
    }

    public boolean couldTheyMeet(Person p) {
        if (!this.isDeceased() && !p.isDeceased()) {
            return true;
        }
        else if (this.isDeceased() && !p.isDeceased()) {
            return intersect(p, "right");
        }
        else if (!this.isDeceased() && p.isDeceased()) {
            return intersect(p, "left");
        }
        else {
            return intersect(p, "both");
        }
    }

    private boolean intersect(Person p, String how) {
        if (how.equals("right")) {
            return this.getDateOfDeath().before(p.getDateOfBirth());
        }
        else if (how.equals("left")) {
            return this.getDateOfBirth().before(p.getDateOfDeath());
        }
        else {
            return this.getDateOfBirth().before(p.getDateOfDeath()) || this.getDateOfDeath().before(p.getDateOfBirth());
        }
    }
}
