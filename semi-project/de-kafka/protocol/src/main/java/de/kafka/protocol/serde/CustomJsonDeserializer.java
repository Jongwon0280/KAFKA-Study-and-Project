package de.kafka.protocol.serde;

import de.kafka.protocol.constant.Topics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.Serializers;
import de.kafka.protocol.event.ClickEvent;
import de.kafka.protocol.event.ImpressionEvent;
import de.kafka.protocol.event.JoinedClickEvent;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonDeserializer implements Deserializer<Object> {
    private ObjectMapper objectMapper= new ObjectMapper();

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                System.out.println("Null recevied at deserializing ");
                return null;

            }
            System.out.println(
                    "Deserializing...topic : "+ topic
            );
            switch (topic) {
                case Topics.TOPIC_IMP:
                    return objectMapper.readValue(new String(data,"UTF-8"), ImpressionEvent.class);

                case Topics.TOPIC_CLICK:
                    return objectMapper.readValue(new String(data,"UTF-8"), ClickEvent.class);
                case Topics.TOPIC_JOINED_CLICK:
                    return objectMapper.readValue(new String(data,"UTF-8"), JoinedClickEvent.class);
                default:
                    System.out.println("unknown topic : " + topic);
            }

            return null;
        }catch(Exception e){
            if(data!=null){
                System.err.println("error occured by data "+ new String(data));

            }
            throw new SerializationException("Error when deserializing byte[] to Class");

        }


    }
}
