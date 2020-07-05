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

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.Dispatcher;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

@Log4j2
@Named
public class ConnectionWrapper implements ConnectionListener, ErrorListener {

    private final Options options;
    private volatile Dispatcher dispatcher;

    public ConnectionWrapper(ListenerProperties listenerProperties) {
        ListenerProperties.NatsProperties natsProperties = listenerProperties.getNats();
        char[] password = natsProperties.getPassword() != null ? natsProperties.getPassword().toCharArray() : null;
        char[] username = natsProperties.getUsername() != null ? natsProperties.getUsername().toCharArray() : null;
        options = new Options.Builder()
                .connectionListener(this)
                .errorListener(this)
                .maxMessagesInOutgoingQueue(natsProperties.getQueueSize())
                .server(natsProperties.getUri())
                .maxReconnects(-1)
                .userInfo(username, password)
                .build();
    }

    public Dispatcher getDispatcher() {
        if (dispatcher == null) {
            synchronized (this) {
                if (dispatcher == null) {
                    try {
                        Connection connection = Nats.connect(options);
                        dispatcher = connection.createDispatcher(m -> {
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return dispatcher;
    }

    @Override
    public void connectionEvent(Connection connection, Events type) {
        String uri = StringUtils.defaultIfBlank(connection.getConnectedUrl(), "unknown");
        log.info("Connection to {} changed state {}", uri, type);
    }

    @Override
    public void errorOccurred(Connection connection, String error) {
        log.error("Error connecting to {}", connection.getConnectedUrl(), error);
    }

    @Override
    public void exceptionOccurred(Connection connection, Exception e) {
        log.error("Exception connecting to {}", connection.getConnectedUrl(), e);
    }

    @Override
    public void slowConsumerDetected(Connection connection, Consumer consumer) {
        log.warn("Slow consumer detected with {} dropped messages", consumer.getDroppedCount());
    }
}
