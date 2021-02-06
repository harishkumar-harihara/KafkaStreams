import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    public KafkaConsumer<Integer,String> createConsumer(){
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,"ConsumeSampleMessages");

        KafkaConsumer<Integer,String> kafkaConsumer = new KafkaConsumer<Integer, String>(consumerProps);
        return kafkaConsumer;
    }

    public void consume(String topicName){
        KafkaConsumer<Integer,String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        try{
            ConsumerRecords<Integer,String> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<Integer,String> record:records){
                System.out.printf("topic = %s , partition = %s, offset = %d,key = %d, value = %s \n",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }finally {
            kafkaConsumer.close();
        }
    }



}
