import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomDeserializer implements Deserializer<FlightData> {


    @Override
    public void open(InputStream inputStream) throws IOException {

    }

    @Override
    public FlightData deserialize(FlightData flightData) throws IOException {
        return null;
    }

    public FlightData deserialize(String topicName, byte[] data){
        int originSize;
        int destinationSize;
        String origin;
        String dest;
        int count;

        try{
            if(data == null)
                return null;
            if(data.length < 12){
                throw new SerializationException("Data received is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            // get origin_country_name
            originSize = buffer.getInt();
            byte[] originBytes = new byte[originSize];
            buffer.get(originBytes);
            origin = new String(originBytes, StandardCharsets.UTF_8);

            // get destination_country_name
            destinationSize = buffer.getInt();
            byte[] destinationBytes = new byte[destinationSize];
            buffer.get(destinationBytes);
            dest = new String(destinationBytes, StandardCharsets.UTF_8);

            // get count
            count = buffer.getInt();

            return new FlightData(origin,dest,count);
        } catch (SerializationException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
