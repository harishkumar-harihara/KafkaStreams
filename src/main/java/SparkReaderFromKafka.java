import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkReaderFromKafka {
    public void readStreamingDataFromKafka(String topicName){
       SparkSession spark = SparkSession.builder().appName("KafkaSparkIntegrationExample").
               master("local[1]").getOrCreate();

       Dataset<Row> df =  spark.readStream().
                                format("kafka").
                                option("kafka.bootstrap.servers", "localhost:9092").
                                option("subscribe", topicName).
                                option("includeHeaders","true").
                                load();

       df.createOrReplaceGlobalTempView("flightsTable");

       df.selectExpr("SELECT * FROM flightsTable WHERE count = 251").show();
    }
}
