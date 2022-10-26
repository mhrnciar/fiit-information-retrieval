package parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class SparkParser {
    public void parse(String path) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Could They Meet").getOrCreate();

        System.out.println("Started parsing...");
        Instant startTime = Instant.now();

        Dataset<Row> df = spark.read().option("delimiter", " ").text(path);
        df = df.select(
                split(col("value"), "\t").getItem(0).as("subject"),
                split(col("value"), "\t").getItem(1).as("operator"),
                split(col("value"), "\t").getItem(2).as("value")
        ).select(
                regexp_extract(col("subject"), "<\\w+[:/]+[a-zA-Z.]+/\\w+/(.\\.\\w+)>", 1).as("subject"),
                col("operator"),
                col("value")
        );

        // TODO: Divide fields into multiple dataframes by filters, apply regex extracting, and use join to collect data with the same IDs
        Dataset<Row> df_people = df.filter(col("value").rlike(".*<http://rdf\\.freebase\\.com/ns/people\\.person>.*|.*<http://rdf\\.freebase\\.com/ns/people\\.deceased_person>.*"));
        df_people = df_people.select(
                col("subject").as("id"),
                regexp_extract(col("value"), "<\\w+[:/]+[a-zA-Z.]+/\\w+/([a-zA-Z._]+)>", 1).as("type")
        );

        Dataset<Row> df_joined = df_people.join(df, df_people.col("id").equalTo(df.col("subject")));
        df_joined = df_joined.select(
                col("id"),
                col("type"),
                col("operator"),
                regexp_extract(col("value"), "\"((\\w+[ ]*)*)\"@(\\w+)", 1).as("name"),
                regexp_extract(col("value"), "\"((\\d+[:\\-/]*)+)\".*", 1).as("date")
        ).filter("name != '' OR date != ''");
        System.out.println(df_joined.showString(1000, 0, false));

        Instant endTime = Instant.now();
        System.out.println("Total execution time: " + Duration.between(startTime, endTime).toSeconds() + " seconds");
    }
}
