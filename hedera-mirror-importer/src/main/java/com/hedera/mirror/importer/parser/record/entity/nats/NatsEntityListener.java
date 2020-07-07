package com.hedera.mirror.importer.parser.record.entity.nats;

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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import javax.annotation.PostConstruct;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import com.hedera.mirror.importer.MirrorProperties;
import com.hedera.mirror.importer.domain.TopicMessage;
import com.hedera.mirror.importer.exception.ImporterException;

@Log4j2
@Named
@RequiredArgsConstructor
public class NatsEntityListener {

    private final MirrorProperties mirrorProperties;
    private final ObjectMapper objectMapper;
    private final ConnectionWrapper connection;
    private final MeterRegistry meterRegistry;

    private Timer timer;
    private long shard;
    private boolean enabled;

    @PostConstruct
    void init() {
        timer = Timer.builder("hedera.mirror.importer.publish.duration")
                .description("The amount of time it took to publish the topic message to the message broker")
                .tag("type", "topicmessage")
                .register(meterRegistry);
        shard = mirrorProperties.getShard(); // Cache to avoid reflection penalty
        enabled = mirrorProperties.isNats();
    }

    public void onTopicMessage(TopicMessage topicMessage) throws ImporterException {
        if (!enabled) {
            return;
        }
        String subject = String.format("topic.%d.%d.%d", shard, topicMessage.getRealmNum(), topicMessage.getTopicNum());

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(topicMessage);
            timer.record(() -> connection.get().publish(subject, bytes));
        } catch (Exception e) {
            log.error("Unable to publish to subject {} via {}", subject, connection.get().getConnectedUrl(), e);
        }
    }
}
