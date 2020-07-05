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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Resource;
import org.junit.jupiter.api.Test;

import com.hedera.mirror.grpc.domain.TopicMessage;

public class NatsTopicListenerTest extends AbstractTopicListenerTest {

    @Resource
    private NatsTopicListener topicListener;

    @Override
    protected TopicListener getTopicListener() {
        return topicListener;
    }

    @Test
    void test() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = "{\"consensusTimestamp\":1593449558057407397," +
                "\"message\":\"AAABcwD" +
                "+rQljOTQ4ZDNiMi00Yjc4LTRmM2ItODFhYS0yZmMwMTVlYWViYTkwZTk0MTAyYS1lMDk4LTRlYmEtYTEwNy1hM2E4YmUzYWI1NDVmZjc3ZGJhMC03ZDJlLTRiMjgtOTRmYy1kY2ZmZjgxODQzOTYzZjRmOWI4OS05OTU2LTQ3ZTctOGY3Zi0xNTczNDBlNzkxZWU5ZWU2YTZjOS0zNmM0LTQyNjItYTU0NC1mM2FjZDRiNThmMzljNDAxOGY5My0xMDQyLTQ1MDctYWYzNC1lY2QyOTRkZTJmZTMyNTJhMDYxYy01YTI5LTQ3Y2UtODgxZi1hYmEwYjYzAA==\",\"realmNum\":0,\"runningHash\":\"LRWrPQPqsGxMJMluBMj1HNxXToV0w3dHZIIRnmbR4+t/MTjI9U4KlWZO7Jxrp4y4\",\"sequenceNumber\":60698808,\"topicNum\":1022,\"runningHashVersion\":2}";
        TopicMessage topicMessage = objectMapper.readValue(json, TopicMessage.class);
        assertThat(topicMessage).isNotNull();
    }
}
