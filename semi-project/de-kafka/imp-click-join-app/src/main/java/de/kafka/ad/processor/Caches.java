package de.kafka.ad.processor;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import de.kafka.protocol.event.ImpressionEvent;
import de.kafka.protocol.event.JoinedClickEvent;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class Caches {


    private static final Duration impValidWindow = Duration.ofMinutes(1L);

    private static final Duration clickDedupWindow = Duration.ofMillis(1L);

    public static Cache<String, ImpressionEvent> CACHE_IMP =
            Caffeine.newBuilder()
                    .maximumSize(10_000)
                    .expireAfter(new Expiry<String, ImpressionEvent>() {
                        @Override
                        public long expireAfterCreate(@NonNull String key, @NonNull ImpressionEvent value, long currentTime) {
                            return impValidWindow.toNanos();
                        }

                        @Override
                        public long expireAfterUpdate(@NonNull String key, @NonNull ImpressionEvent value, long currentTime, @NonNegative long currentDuration) {
                            return currentDuration;
                        }

                        @Override
                        public long expireAfterRead(@NonNull String key, @NonNull ImpressionEvent value, long currentTime, @NonNegative long currentDuration) {
                            return currentDuration;
                        }


                    }).build();

    public static Cache<String, JoinedClickEvent> CACHE_CLICK =
            Caffeine.newBuilder()
                    .maximumSize(10_000)
                    .expireAfter(new Expiry<String, JoinedClickEvent>() {
                        @Override
                        public long expireAfterCreate(@NonNull String key, @NonNull JoinedClickEvent value, long currentTime) {
                            return clickDedupWindow.toNanos();
                        }

                        @Override
                        public long expireAfterUpdate(@NonNull String key, @NonNull JoinedClickEvent value, long currentTime, @NonNegative long currentDuration) {
                            return currentDuration;
                        }

                        @Override
                        public long expireAfterRead(@NonNull String key, @NonNull JoinedClickEvent value, long currentTime, @NonNegative long currentDuration) {
                            return currentDuration;
                        }


                    }).build();
    public static Cache<String , AtomicLong>CACHE_CLICK_COUNT_BY_AD_ID=
            Caffeine.newBuilder()
                    .maximumSize(10_000)
                    .build();

}
