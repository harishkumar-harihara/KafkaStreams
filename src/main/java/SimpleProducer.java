import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;

public class SimpleProducer {

    private static final Logger logger = LogManager.getLogger(SimpleProducer.class);

    public KafkaProducer<Integer,String> createProducer(){

        logger.info("Creating Kafka Producer...");

        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG,"HelloProducer");
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer,String> producer = new KafkaProducer<>(kafkaProps);
        return producer;
    }

    public void sendMessage(String topicName, int numEvents){

        try{
            KafkaProducer<Integer,String> producer = createProducer();
            logger.trace("Start sending messages...");
            for(int i=0; i < numEvents; i++){
                producer.send(new ProducerRecord<>(topicName,i,"SampleMessage" + i));
            }
            producer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendSyncMessage(String topicName, int numEvents){
        try{
            KafkaProducer<Integer,String> producer = createProducer();
            logger.trace("Start sending messages...");
            for(int i=0; i < numEvents; i++){
                producer.send(new ProducerRecord<>(topicName,i,"SampleMessage" + i)).get();
            }
            producer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendAsyncMessage(String topicName, int numEvents){
        class DemoProductCallback implements Callback{
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    e.printStackTrace();
                }
            }
        }

        try{
            KafkaProducer<Integer,String> producer = createProducer();
            logger.trace("Start sending messages...");
            for(int i=0; i < numEvents; i++){
                producer.send(new ProducerRecord<>(topicName,i,"SampleMessage" + i),new DemoProductCallback());
            }
            producer.close();
        }catch (Exception e){
            logger.error("Exception occurred – Check log for more details.\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SimpleProducer producer = new SimpleProducer();
        producer.sendSyncMessage(args[0],Integer.valueOf(args[1]));

        SimpleConsumer consumer = new SimpleConsumer();
        consumer.consume(args[0]);
    }
}


