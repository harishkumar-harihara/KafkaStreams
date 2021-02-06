import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomFlightSerializer implements Serializer<FlightData> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, FlightData flightData) {
        byte[] destinationCountryName;
        byte[] originCountryName;
        int destCountrySize;
        int originCountrySize;
        try{
            if(flightData.getDest_country_name() != null){
                destinationCountryName = flightData.getDest_country_name().getBytes(StandardCharsets.UTF_8);
                destCountrySize = destinationCountryName.length;
            }else{
                destinationCountryName = new byte[0];
                destCountrySize = 0;
            }
            if(flightData.getOrigin_country_name() != null){
                originCountryName = flightData.getOrigin_country_name().getBytes(StandardCharsets.UTF_8);
                originCountrySize = originCountryName.length;
            }else{
                originCountryName = new byte[0];
                originCountrySize = 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(destCountrySize + originCountrySize + 4);
            buffer.put(originCountryName);
            buffer.put(destinationCountryName);
            buffer.putInt(flightData.getCount());

            return buffer.array();
        }catch (Exception e){
            throw new SerializationException("Error during custom serialization to FlightData type" + e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, FlightData data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
