package parser;

public class Person {
    private String id;
    private String name;
    private String type;
    private String dateOfBirth;
    private String dateOfDeath;
    private boolean deceased;
    private int size;

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
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        size++;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        size++;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
        size++;
    }

    public String getDateOfDeath() {
        return dateOfDeath;
    }

    public void setDateOfDeath(String dateOfDeath) {
        this.dateOfDeath = dateOfDeath;
        size++;
    }

    public boolean isDeceased() {
        return deceased;
    }

    public void setDeceased(boolean deceased) {
        this.deceased = deceased;
    }

    public void printPerson() {
        StringBuilder str = new StringBuilder();
        str.append(getId()).append(":\t").append(getName()).
                append(",\n\tType: ").append(getType()).
                append(",\n\tDeceased: ").append(isDeceased()).
                append(",\n\tDate of birth: ").append(getDateOfBirth()).
                append(",\n\tDate of death: ").append(getDateOfDeath());

        System.out.println(str);
        System.out.println();
    }

    public boolean isValid() {
        if (isDeceased() && getDateOfDeath() == null) {
            return false;
        }
        return getDateOfBirth() != null && getName() != null;
    }
}
