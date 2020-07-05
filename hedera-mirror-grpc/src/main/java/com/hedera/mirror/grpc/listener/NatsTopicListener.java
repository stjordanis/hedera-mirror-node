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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import com.hedera.mirror.grpc.domain.TopicMessage;
import com.hedera.mirror.grpc.domain.TopicMessageFilter;

@Named
@Log4j2
@RequiredArgsConstructor
public class NatsTopicListener implements TopicListener {

    private final ObjectMapper objectMapper;
    private final ConnectionWrapper connection;

    @Override
    public Flux<TopicMessage> listen(TopicMessageFilter filter) {
        AtomicReference<Subscription> subscription = new AtomicReference<>();

        Flux<TopicMessage> flux = Flux.create(sink -> {
            String subject = "topic.0." + filter.getRealmNum() + "." + filter.getTopicNum();
            log.info("Subscribing to subject {}: {}", subject, filter);
            subscription.set(connection.getDispatcher().subscribe(subject, m -> {
                try {
                    TopicMessage topicMessage = objectMapper.readValue(m.getData(), TopicMessage.class);
                    sink.next(topicMessage);
                } catch (Exception e) {
                    sink.error(e);
                }
            }));
        });

        return flux.filter(t -> filterMessage(t, filter))
                .doOnCancel(() -> log.info("Unsubscribing"))
                .doOnCancel(() -> subscription.get().unsubscribe())
                .doOnComplete(() -> subscription.get().unsubscribe());
    }

    private boolean filterMessage(TopicMessage message, TopicMessageFilter filter) {
        return message.getRealmNum() == filter.getRealmNum() &&
                message.getTopicNum() == filter.getTopicNum();// &&
        //message.getConsensusTimestamp() >= filter.getStartTimeLong();
    }
}
