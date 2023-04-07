package de.kafka.protocol.serde;

import de.kafka.protocol.constant.Topics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonSerializer implements Serializer<Object> {
    private ObjectMapper objectMapper= new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            if (data == null) {
                System.out.println("Null recevied at serializing. topic : " + topic);
                return null;

            }
            System.out.println(
                    "Serializing..."
            );
            switch (topic) {
                case Topics.TOPIC_IMP:
                case Topics.TOPIC_CLICK:
                case Topics.TOPIC_JOINED_CLICK:
                    return objectMapper.writeValueAsBytes(data);
                default:
                    System.out.println("unknown topic : " + topic);
            }

            return null;
        }catch(Exception e){
            throw new SerializationException("Error with Serializing");

        }


    }
}
