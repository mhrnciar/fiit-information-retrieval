# Information retrieval

**Parsing of Person entities from Freebase database and creation of simple service “Could they meet?”**


## Objective

The aim of this project is to create a service named “Could they meet?” using data from (now deprecated) Google Freebase. 
The basis of the project is that the user can input two famous people (deceased or still alive), and the service will evaluate and return whether these two people could have met sometime in the past. 
Of course, the search won't be very precise since people may travel a lot, and the chance two people randomly meet is very small. 
However, for the aim of this project, we can assume that they could, as long as the time between their birth and death (in the case of deceased people) overlaps.

This project may be useful in research of known people in history, as well as in the present. 
Sometimes, we want to quickly look up whether two people lived at the same time, especially if their actions had impact on historical occurrences, but it is often a time-consuming task to find the information about two people and compare their dates of birth and death. 
A large database storing this information about all (or at least the majority) important people, such as Freebase, allows for quick comparison. 
However, because the database may contain millions of historical figures, it needs to be indexed in order to be fast and useful.


## Existing solutions

The survey of existing solutions yielded no results. 
There may be some available tools, but they may be obscure and not commonly used. 
The closest application was the usage of Lucene Date Range in Elasticsearch or Kibana (Source). 
The date range can be used to query all records overlapping the entered date range - in our case, the date of birth and death of first person. 
The records can be searched for the name of the second person - if they appear in the result set, they could meet, if they don't appear, they could not have met.

Another related method is by searching Wikipedia.
User can search for two people they want to evaluate, find their dates of birth and death, and compare them.
However, this method is time-consuming and this service may greatly speed up the evaluation process.


## Data

The truncated sample of entity m.0100kt2c in Freebase looks like this:
```bash
$ gzcat freebase-rdf-latest.gz | grep -E ".*<http://rdf\\.freebase\\.com/ns/m.0100kt2c>*"
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage>	<http://en.wikipedia.org/wiki/Mine_G%FCng%F6r>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/common.topic.notable_for>	<http://rdf.freebase.com/ns/g.1q6f_55bl>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.name>	"Mine Güngör"@en	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/people.person.date_of_birth>	"1983-01-04"^^<http://www.w3.org/2001/XMLSchema#date>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.key>	"/wikipedia/en_id/42218260"	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.key>	"/wikipedia/en_title/Mine_G$00FCng$00F6r"	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.key>	"/wikipedia/en/Mine_Gungor"	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.type>	<http://rdf.freebase.com/ns/common.topic>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/common.topic.description>	"Mine Güngör is a Turkish women's ice hockey player. Curremtly, she is a member of Istanbul Buz Korsanları team playing as goaltender. She takes part in the Turkey women's national ice hockey team. The 1.73 m tall woman at 70 kg catches right handed.\nMine Güngör was born in Mamak, Ankara on January 4, 1983. She was distance educated through the American Public University System."@en	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/kg.object_profile.prominent_type>	<http://rdf.freebase.com/ns/people.person>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.type>	<http://rdf.freebase.com/ns/sports.pro_athlete>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/people.person.height_meters>	"1.73"	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/people.person.place_of_birth>	<http://rdf.freebase.com/ns/m.0cpz35>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/key/wikipedia.en>	"Mine_Gungor"	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://rdf.freebase.com/ns/type.object.type>	<http://rdf.freebase.com/ns/people.person>	.
<http://rdf.freebase.com/ns/m.0100kt2c>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>	<http://rdf.freebase.com/ns/people.person>	.
...
```
Each line contains one fact. 
The first URL defines which entity the fact is related to, the second URL defines the category of the fact, and the third 'column' can be another URL denoting a relationship with another entity in the database, or the fact itself (some value, mostly string). 
Each line is ended with the dot character. 
In the mentioned sample, we can find that the name of the entity is Mine Güngör in the category `<…/type.object.name>`, it is a person, as defined in the category `<…/type.object.type>`, and it was born on January 4th, 1983, as shown in the category `<…/people.person.date_of_birth>`.

These three lines have been mentioned because they will be used in the project: first, all lines of type `people.person` will be selected, and using the entity ID, the additional information will be queried from the data, namely `people.person.date_of_birth` and `type.object.name`. 
Deceased people are categorised as `people.person.deceased_person`, so those lines will have to be queried as well, with extra information about the date of death in category `people.deceased_person.date_of_death`.


## Design

The compressed archive has 32 GB, while the whole uncompressed Freebase has about 350 GB, which makes the handling of uncompressed data impractical. 
Therefore, the data should be first handled in GZIP format, from which would be selected only facts that we need, greatly reducing the size of space our data will take up. 
Then, we can remove the majority of URLs (shown in the section about data) to preserve even more space. We can extract the data in two ways:

by using grep operation:
```bash
$ gzcat freebase-rdf-latest.gz | grep -E ".*<http://rdf\\.freebase\\.com/ns/people.person.date_of_birth>*" > BIRTHS.txt
```

by using java.util.zip and java.util.regex packages:
```java
void process() {
    fin = new GZIPInputStream(new FileInputStream("freebase-rdf-latest.gz"));
    // open file for each fact category
    fnames = new FileOutputStream("NAMES.txt");
    fbirths = new FileOutputStream("BIRTHS.txt");
    ...

    // create regex for each fact category
    name_regex = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/type.object.name>.*");
    dob_regex = Pattern.compile(".*<http://rdf\\.freebase\\.com/ns/people\\.person\\.date_of_birth>.*");
    ...
    
    while ((line = fin.readLine()) != EOF) {
        if (name_regex(line).matches()) {
            fnames.write(line);
        }
        if (dob_regex(line).matches()) {
            fbirths.write(line);
        }
    }
    
    fnames.close();
    fbirths.close();
    ...
    fin.close();
}
```

The data can be saved into CSV format for easier manipulation - one line for each person and the information about the person is separated by commas.

## Indexing

To make an index manually, we can save the data in a CSV file and iterate over every line, saving the name and offset from the start of the file into the index. 
The index has been implemented as a HashMap, thanks to which the name is searchable with O(1) complexity. 
The key is the name of the person and the value is the offset, which can be used in `seek()` function, to jump to the start of the line of the person with O(1) complexity. 
To achieve this, it is important to open the file as `RandomAccessFile`, so `seek()` will be able to jump to a specified place in the file.

After the index is created or loaded, the search is performed by passing the names of two people. 
Each person will be queried from the index (in case a person is not found, the person will be prompted to enter the name again) and their dates of birth will be compared: queried people could have met if the birth of one person has been before the other person has died and vice versa. 
If both people are alive, they could have, or still can meet. 
If they both died at very different times and their lives don't overlap, they couldn't have met.


## Distributed computing using Apache Spark

The solution mentioned above is good for a relatively small sample, but it is inefficient for large amounts of data. 
The HashMap used to collect all data about people is kept in memory, which would be quickly exceeded when used on the whole FreeBase database. 
That's why we need to distribute the computation over multiple nodes in a cluster using Apache Spark.


## Indexing using Apache Lucene

CSV file with extracted people can be loaded to Lucene index, which, unlike our simple HashMap index, performs additional operations, such as tokenization, stemming, etc. 
This allows better searching, because the query doesn't have to be fulltext, and there may be some spelling mistakes, and the Lucene still may be able to find the correct result, or return the list of potential results. 
This behaviour was impossible to achieve in HashMap index, because if there were two people with the same name, the second person overwritten the first one.

Searching is done twice, because we need to search for two people. 
After the people are found, they are transformed to Person object, which are then compared as can be seen below:

```text
Enter the full name of the first person: Lars Olsson Smith
m.041dld: Lars Olsson Smith,
    Deceased: true,
    Date of birth: 1836-10-12,
    Date of death: 1913-12-09

Enter the full name of the second person: Piotr
m.027f71j: Piotr,
    Deceased: true,
    Date of birth: 1926-12-03,
    Date of death: 2007-11-19

No, Lars Olsson Smith and Piotr could not have met.
```


## Evaluation

Because we don't perform a search in large text, only search for names, we cannot use _precision/recall_ or other metrics to evaluate the quality of our solution.
We also cannot compare the efficiency and precision of our solution with Freebase API, as it has been shut down as of May 2016. 
We can, however, evaluate the differences between HashMap index and Apache Lucene index:

HashMap:
- Hashing allows searching with complexity O(1)
- Whole HashMap must be loaded in the memory, which is inefficient with big data and may cause the program to fail
- If two people have the same name, they won't appear in the HashMap at once, as one person will overwrite the previous one

Apache Lucene:
- Index doesn't need to be loaded in the memory, it is possible to read it from file
- More people with the same name can appear in the index at once
- Since Lucene performs tokenization, legitimisation, etc. it is possible to search only by the part of the name, or with spelling mistakes

Examples:
- _Aryan Khan_ - two people have the same name, Lucene found both, while HashMap only found one as the second one has been overwritten
- _Mark Mark_ - this name doesn't exist, but Lucene finds similar names, HashMap needs correct fulltext name, so it doesn't name any name
- _Elo Musk_ - the misspelling in the name causes the name Elon Musk to fall to the 4th place, HashMap cannot find name with incorrect spelling, so it doesn't return anything
- _Bruc Willis_ - interestingly, spelling error in this name causes it to not appear in the top 10 results, even though it's missing only one letter; even more interesting is the fact that the top result has been Timbaler del Bruc


## Setup

The project has been developed in Java 8/11 with Apache Spark and Apache Lucene dependencies. 
Maven has been used as a tool for dependency management, and all packages are listed in `pom.xml` in the project root.

There are two classes for starting and testing of the program:
1. GZIPTester for testing of Person class, evaluation whether two people could have met, starting manual parsing process, and manual indexing
2. LuceneTester for testing parsing using Spark and indexing using Lucene

Spark by default is set to local, and to start it on existing server, we first need to replace the `local[*]` in `SparkSession` builder with the URL of the Spark cluster. 
The argument of `parse()` method is the path to the Freebase dump in .gz format and the path to output directory. 
Spark, however, can figure out whether the file has been compressed or not, so the .gz format is used only to save space the dump takes up. 
At the end of parsing, the result is saved to CSV files.

After parsing, first, we need to declare the `LuceneIndexer` object with the path to the generated CSV file, as well as path to where the index should be saved. 
Indexing itself is performed in `createIndex()` method, which iterates through the whole file and writes the contents to the index. 
Next, we create the `LuceneSearcher` object with path to existing index, and using `search()` method, we can search for people in the index.

In case of running the parsing on Mesos cluster, we first need to create .jar file - `SparkSubmit-1.0.jar`. 
This .jar doesn't have Spark executor in package, so it has to be added during runtime. 
For easier starting of the job has been prepared `submit-job.sh` shell script, which specifies the main class, sets the URL of the Mesos cluster, adds the dump file and the Spark executor to the job, and specifies the runtime arguments. 
The first argument is the name of the input file, which should match the name in the -–files option and the second argument specifies the output folder. 
Note that the -–master, –-files, and –-conf options contain placeholders which should be replaced with the correct URL and paths prior to submitting the job.

Indexing can be started from `GZIPTester` or `LuceneTester` classes, in which it is needed to uncomment the indexing task and enter the correct path to the CSV files. 
After the indexing is complete, it is possible to perform searching by uncommenting the searching tasks and entering the correct path to saved index. 
With HashMap index, the user will be repeatedly prompted until a correct name has been entered. 
Lucene index returns a list of potential hits, from which the user can select the person they want to compare. 
By default, the searcher returns the top 10 results, and it can be changed in the `LuceneSearcher` class. 
The program is console-based, so all hits and the result of evaluation is printed in the console.


