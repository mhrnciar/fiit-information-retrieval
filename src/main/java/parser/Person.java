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

    static private final SimpleDateFormat[] knownPatterns = {
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy-MM"),
            new SimpleDateFormat("yyyy")
    };

    public Person(String id) {
        setId(id);
        setDeceased(false);
        this.size = 2;
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
        size++;
    }

    public boolean isDeceased() {
        return deceased;
    }

    public void setDeceased(boolean deceased) {
        this.deceased = deceased;
    }

    public String printPerson() {
        StringBuilder str = new StringBuilder();
        str.append(getId()).append(": ").append(getName()).
                append(",\n\tRaw ID: ").append(this.rawId).
                append(",\n\tType: ").append(getType()).
                append(",\n\tDeceased: ").append(isDeceased()).
                append(",\n\tDate of birth: ").append(getDateOfBirth()).
                append(",\n\tDate of death: ").append(getDateOfDeath());

        System.out.println(str);
        System.out.println();

        return str.toString();
    }

    public boolean isValid() {
        if (isDeceased() && getDateOfDeath() == null) {
            return false;
        }
        return getDateOfBirth() != null && getName() != null;
    }
}

// EXTRACT ID <\w+[:\/]+[a-zA-Z.]+\/\w+\/(.\.\w+)>
// EXTRACT TYPE <\w+[:\/]+[a-zA-Z.]+\/\w+\/([a-zA-Z._]+)>
//  - EXTRACT ONLY CATEGORY <\w+[:\/]+[a-zA-Z.]+\/\w+\/people.person.([a-zA-Z._]+)>

// EXTRACT DATE \"((\d+[:\-\/]*)+)\".*
// EXTRACT NAME \"((\w+[ ]*)*)\"@(\w+)
//  - ALTERNATIVE \"(.*)\"@(\w+)
