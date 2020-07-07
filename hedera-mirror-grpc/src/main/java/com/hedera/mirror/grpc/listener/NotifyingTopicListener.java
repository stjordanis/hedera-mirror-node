package com.hedera.mirror.grpc.listener;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 Hedera Hashgraph, LLC
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.pubsub.PgSubscriber;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import com.hedera.mirror.grpc.DbProperties;
import com.hedera.mirror.grpc.domain.TopicMessage;
import com.hedera.mirror.grpc.domain.TopicMessageFilter;

@Named
@Log4j2
public class NotifyingTopicListener implements TopicListener {

    private final ObjectMapper objectMapper;
    private final Flux<TopicMessage> topicMessages;
    private final PgSubscriber subscriber;

    public NotifyingTopicListener(ObjectMapper objectMapper, DbProperties dbProperties) {
        this.objectMapper = objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        subscriber = PgSubscriber.subscriber(Vertx.vertx(), new PgConnectOptions()
                .setDatabase(dbProperties.getName())
                .setHost(dbProperties.getHost())
                .setPassword(dbProperties.getPassword())
                .setPort(dbProperties.getPort())
                .setUser(dbProperties.getUsername())
        ).reconnectPolicy(retries -> 500L);

        topicMessages = Flux.defer(() -> listen())
                .map(this::toTopicMessage)
                .share()
                .name("notify")
                .metrics();
    }

    private Flux<String> listen() {
        EmitterProcessor<String> emitterProcessor = EmitterProcessor.create();

        subscriber.connect(connectResult -> {
            if (!connectResult.succeeded()) {
                throw new RuntimeException(connectResult.cause());
            }

            subscriber.actualConnection().notificationHandler(notificationResult -> {
                emitterProcessor.onNext(notificationResult.getPayload());
            });

            subscriber.actualConnection()
                    .query("LISTEN topic_message")
                    .execute(result -> log.info("Listening for messages"));
        });

        return emitterProcessor.doFinally(s ->
                subscriber.actualConnection()
                        .query("UNLISTEN topic_message")
                        .execute(result -> log.info("Cancelling listen")))
                .doFinally(s -> subscriber.close());
    }

    @Override
    public Flux<TopicMessage> listen(TopicMessageFilter filter) {
        return topicMessages.doOnSubscribe(s -> log.info("Listening for messages: {}", filter))
                .doOnComplete(() -> log.info("complete: {}", filter))
                .doOnError(e -> log.error("error: {}", filter, e))
                .doOnCancel(() -> log.info("cancel: {}", filter));
    }

    private TopicMessage toTopicMessage(String payload) {
        try {
            return objectMapper.readValue(payload, TopicMessage.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
