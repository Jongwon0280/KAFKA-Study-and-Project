package de.kafka.protocol.generator;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import de.kafka.protocol.event.ClickEvent;
import de.kafka.protocol.event.ImpressionEvent;



public class RandomGenrator {
    private static List<String> AD_IDS = Arrays.asList(
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10)
    );

    // impressionEvent Class field info
//     private String impId;
//    private String requestId;
//    private String adId;
//    private String userId;
//    private String deviceId;
//    private String inventoryId;
//    private long timestamp;
//
    public static ImpressionEvent genratorImpressionEvent(long timestamp) {
        return new ImpressionEvent(
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                AD_IDS.get(RandomUtils.nextInt(0,5)),
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                RandomStringUtils.randomAlphabetic(10),
                timestamp
                );
    }
// ClickEvent Class Field info
//    private String impId;
//    private String clickUrl;
//    private long timestamp;

    public static ClickEvent generatorClickEvent(long timestamp, String impId){
        return new ClickEvent(
                impId,
                RandomStringUtils.randomAlphabetic(10),
                timestamp
        );
    }
}
