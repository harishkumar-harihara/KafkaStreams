import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FlightProducer {
    public void produce(String topicName){

        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG,"FlightProducer");
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomFlightSerializer.class.getName());
        KafkaProducer<Integer,FlightData> producer = new KafkaProducer<>(kafkaProps);

//        Path path = Paths.get("src/main/resources/2010-summary.json");

        BufferedReader br = null;
        JSONParser parser = new JSONParser();
        int index = 0;

        try{
            String sCurrentLine;
            br = new BufferedReader(new FileReader("src/main/resources/2010-summary.json"));

            while ((sCurrentLine = br.readLine()) != null) {
//                System.out.println("Record:\t" + sCurrentLine);

                Object obj;
                try {
                    obj = parser.parse(sCurrentLine);
                    JSONObject jsonObject = (JSONObject) obj;
                    String origin_country_name = (String) jsonObject.get("ORIGIN_COUNTRY_NAME");
                    String dest_country_name = (String) jsonObject.get("DEST_COUNTRY_NAME");
                    int count = Integer.parseInt(jsonObject.get("count").toString());

                    FlightData data = new FlightData(origin_country_name, dest_country_name, count);
//                    data.setOrigin_country_name(origin_country_name);
//                    data.setDest_country_name(dest_country_name);
//                    data.setCount(count);

                    producer.send(new ProducerRecord<>(topicName,index,data)).get();

//                    System.out.printf("origin=%s, destination=%s, count=%d \n",
//                            origin_country_name,dest_country_name,count);

                    index++;
                } catch (ParseException | ExecutionException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            producer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FlightProducer flightProducer = new FlightProducer();
        flightProducer.produce(args[0]);

        SparkReaderFromKafka reader = new SparkReaderFromKafka();
        reader.streamReader(args[0]);
    }
}
