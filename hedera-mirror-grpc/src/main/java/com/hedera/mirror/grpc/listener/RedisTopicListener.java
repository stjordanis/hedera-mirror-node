package com.hedera.mirror.grpc.listener;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import java.util.Arrays;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.publisher.Flux;

import com.hedera.mirror.grpc.GrpcProperties;
import com.hedera.mirror.grpc.domain.TopicMessage;
import com.hedera.mirror.grpc.domain.TopicMessageFilter;

@Log4j2
@Named
@RequiredArgsConstructor
public class RedisTopicListener implements TopicListener {

    private static final SerializationPair<String> CHANNEL_SERIALIZER = SerializationPair
            .fromSerializer(RedisSerializer.string());

    private static final SerializationPair<TopicMessage> MESSAGE_SERIALIZER = SerializationPair
            .fromSerializer(new Jackson2JsonRedisSerializer(TopicMessage.class));

    private final GrpcProperties grpcProperties;
    private final ReactiveRedisMessageListenerContainer container;

    @Override
    public Flux<TopicMessage> listen(TopicMessageFilter filter) {
        ChannelTopic channel = getChannel(filter);
        return container.receive(Arrays.asList(channel), CHANNEL_SERIALIZER, MESSAGE_SERIALIZER)
                .map(m -> m.getMessage())
                .doOnSubscribe(s -> log.info("Listening for messages on {}: {}", channel, filter))
                .doOnComplete(() -> log.info("complete: {}", filter))
                .doOnError(e -> log.error("error: {}", filter, e))
                .doOnCancel(() -> log.info("Stop listening for messages on {}: {}", channel, filter));
    }

    private ChannelTopic getChannel(TopicMessageFilter filter) {
        return ChannelTopic.of(String
                .format("topic.%d.%d.%d", grpcProperties.getShard(), filter.getRealmNum(), filter.getTopicNum()));
    }
}
