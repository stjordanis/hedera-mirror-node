package com.hedera.mirror.grpc.domain;

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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.UnsafeByteOperations;
import com.hederahashgraph.api.proto.java.Timestamp;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.springframework.data.domain.Persistable;

import com.hedera.mirror.api.proto.ConsensusTopicResponse;
import com.hedera.mirror.grpc.converter.InstantToLongConverter;
import com.hedera.mirror.grpc.converter.LongToInstantConverter;

@AllArgsConstructor
@Builder
@Entity
@JsonIgnoreProperties(ignoreUnknown = true, value = {"consensusTimestampInstant", "response"})
@NoArgsConstructor(force = true)
@Value
public class TopicMessage implements Comparable<TopicMessage>, Persistable<Long> {

    private static final Comparator<TopicMessage> COMPARATOR = Comparator
            .nullsFirst(Comparator.comparingLong(TopicMessage::getSequenceNumber));

    @Id
    @ToString.Exclude
    private Long consensusTimestamp;

    @Getter(lazy = true)
    @Transient
    private Instant consensusTimestampInstant = LongToInstantConverter.INSTANCE.convert(consensusTimestamp);

    @ToString.Exclude
    private byte[] message;

    private int realmNum;

    @ToString.Exclude
    private byte[] runningHash;

    private int runningHashVersion;

    private long sequenceNumber;

    private int topicNum;

    @NonFinal
    @ToString.Exclude
    @Transient
    private final AtomicReference<ConsensusTopicResponse> response = new AtomicReference<>();

    // Cache this to avoid paying the conversion penalty for multiple subscribers to the same topic
    public ConsensusTopicResponse toResponse() {
        if (response.get() == null) {
            response.set(ConsensusTopicResponse.newBuilder()
                    .setConsensusTimestamp(Timestamp.newBuilder()
                            .setSeconds(getConsensusTimestampInstant().getEpochSecond())
                            .setNanos(getConsensusTimestampInstant().getNano())
                            .build())
                    .setMessage(UnsafeByteOperations.unsafeWrap(message))
                    .setRunningHash(UnsafeByteOperations.unsafeWrap(runningHash))
                    .setRunningHashVersion(runningHashVersion)
                    .setSequenceNumber(sequenceNumber)
                    .build());
        }
        return response.get();
    }

    @Override
    public int compareTo(TopicMessage other) {
        return COMPARATOR.compare(this, other);
    }

    @Override
    public Long getId() {
        return consensusTimestamp;
    }

    @Override
    public boolean isNew() {
        return true;
    }

    public static class TopicMessageBuilder {
        public TopicMessageBuilder consensusTimestamp(Instant consensusTimestamp) {
            this.consensusTimestamp = InstantToLongConverter.INSTANCE.convert(consensusTimestamp);
            return this;
        }
    }
}
