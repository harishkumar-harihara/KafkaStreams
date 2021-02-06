import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, sum, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//object MyDeserializerWrapper {
//  val deser = new CustomDeserializer
//}

class SparkReaderFromKafka {

  def streamReader(topicName:String) {
    val spark: SparkSession = SparkSession.builder().
      master("local[1]").
      appName("KafkaStreamIntegration").getOrCreate();

    import spark.implicits._

    val df = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers","localhost:9092").
      option("subscribe",topicName).
      load();

    val schema = StructType(Array(
      StructField("origin",StringType,true),
      StructField("destination",StringType,true),
      StructField("count",IntegerType,true)))

    val flightsDF = df.select(from_json(col("value").cast("string"),schema).as("data"))

    val noOfFlightsFrom = flightsDF.groupBy(col("data.origin")).agg(sum(col("data.count")))

//    val deserialize = (topicName: String, bytes: Array[Byte]) => MyDeserializerWrapper.deser.deserialize(topicName, bytes);

//    val deserializeUDF = udf(deserialize);

//    val flightsDF = df.select(deserializeUDF(col("value"))).toDF("origin","dest","count");

//    spark.udf.register("deserialize", (topic: String, bytes: Array[Byte]) =>
//      MyDeserializerWrapper.deser.deserialize(topic, bytes)
//    )


//    val flightEncoder = Encoders.bean(classOf[FlightData]);

//    val flightDF = df.select(col("value").as(flightEncoder));
//      .map(x => (x.origin_country_name,x.dest_country_name,
//    x.count)).toDF("origin","destination","count");
//
//    val withinCountry = flightDF.filter(col("origin") === col("destination"))
//
    noOfFlightsFrom.writeStream.outputMode("complete").format("console").start();
  }
}
