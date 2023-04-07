package de.kafka.ad.processor;

import de.kafka.protocol.Utils;
import de.kafka.protocol.constant.Topics;
import de.kafka.protocol.event.ClickEvent;
import de.kafka.protocol.event.ImpressionEvent;
import de.kafka.protocol.event.JoinedClickEvent;
import de.kafka.protocol.generator.RandomGenrator;
import de.kafka.protocol.serde.CustomJsonDeserializer;
import de.kafka.protocol.serde.CustomJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static de.kafka.ad.processor.Caches.*;

@Slf4j
public class ImpClickJoinApp {
    public static void main(String[] args) {
        if (args.length !=1 ){
            log.error("pass --bootstrapservers of Kafka Cluster as first argument");
            System.exit(1);

        }
        String bootstrapServers = args[0];
        if (StringUtils.isBlank(bootstrapServers)){
            log.error("pass --bootstrapservers of Kafka Cluster as first argument");
            System.exit(1);

        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomJsonDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"de-kafka-cousumer-3");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,CustomJsonSerializer.class.getName());


        try(Producer<String, Object> producer = new KafkaProducer<>(producerProps);
            Consumer<String, Object> consumer = new KafkaConsumer<>(props);){
            consumer.subscribe(Arrays.asList(Topics.TOPIC_IMP,Topics.TOPIC_CLICK,Topics.TOPIC_JOINED_CLICK));

            while(true) {
                ConsumerRecords<String , Object> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Object> record : records) {
                    Utils.printConsumerRecord(record);
                    if(record.value() instanceof ImpressionEvent){
                        ImpressionEvent impressionEvent = (ImpressionEvent) record.value();
                        CACHE_IMP.put(impressionEvent.getImpId(),impressionEvent);
                        log.info("successed to put imp {}",impressionEvent);


                    }else if (record.value() instanceof ClickEvent){
                        ClickEvent clickEvent = (ClickEvent) record.value();
                        ImpressionEvent impressionEvent = CACHE_IMP.getIfPresent(clickEvent.getImpId());
                        if (impressionEvent !=null) {
                            JoinedClickEvent joinedClickEvent = JoinedClickEvent.from(impressionEvent, clickEvent);
                            JoinedClickEvent duplicatedJoinedClick = CACHE_CLICK.getIfPresent(joinedClickEvent.getImpId());
                            log.info("successed to put imp {}", clickEvent);
                            if (duplicatedJoinedClick == null) {
                                ProducerRecord<String, Object> joinedClickData = new ProducerRecord<>(Topics.TOPIC_JOINED_CLICK,
                                        joinedClickEvent.getImpId(), joinedClickEvent);
                                CACHE_CLICK.put(joinedClickEvent.getImpId(),joinedClickEvent);
                                producer.send(joinedClickData, (metadata, exception) -> {
                                    if (exception != null) {
                                        System.err.println("Fail to produce Record" + joinedClickData);

                                    } else {
                                        Utils.printRecordMetadata(metadata);
                                    }
                                });
                                log.info("successed to send new joined click - {}", joinedClickEvent);
                            } else {
                                log.info("duplicated to joined click - {}", duplicatedJoinedClick);

                            }
                        } else {
                            log.info("cannot join click with imp = {}",clickEvent);
                            }

                        } else if (record.value() instanceof JoinedClickEvent) {
                        JoinedClickEvent joinedClickEvent = (JoinedClickEvent) record.value();
                        log.info("joined Click");
                        String adId = joinedClickEvent.getAdId();
                        AtomicLong count =CACHE_CLICK_COUNT_BY_AD_ID.getIfPresent(adId);
                        if(count ==null){
                            count = new AtomicLong();

                        }
                        log.info("click count of ad {} : {}",adId,count.incrementAndGet());
                        CACHE_CLICK_COUNT_BY_AD_ID.put(joinedClickEvent.getAdId(),count);





                    } else {
                        log.info("Unknown Event");
                    }

                }

                }



        }catch(Exception e){
            log.error(e.getMessage());
            e.printStackTrace();
        }

    }


}
