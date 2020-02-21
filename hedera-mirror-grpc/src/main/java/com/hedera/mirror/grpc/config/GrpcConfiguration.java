package com.hedera.mirror.grpc.config;

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

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.services.HealthStatusManager;
import java.util.LinkedHashMap;
import java.util.Map;
import net.devh.boot.grpc.server.serverfactory.GrpcServerConfigurer;
import net.devh.boot.grpc.server.service.GrpcServiceDefinition;
import net.devh.boot.grpc.server.service.GrpcServiceDiscoverer;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hedera.mirror.grpc.GrpcProperties;

@Configuration
public class GrpcConfiguration {

    @Bean
    CompositeHealthContributor grpcServices(GrpcServiceDiscoverer grpcServiceDiscoverer,
                                            HealthStatusManager healthStatusManager) {

        Map<String, HealthIndicator> healthIndicators = new LinkedHashMap<>();

        for (GrpcServiceDefinition grpcService : grpcServiceDiscoverer.findGrpcServices()) {
            String serviceName = grpcService.getDefinition().getServiceDescriptor().getName();
            healthIndicators.put(serviceName, new GrpcHealthIndicator(healthStatusManager, serviceName));
        }

        return CompositeHealthContributor.fromMap(healthIndicators);
    }

    @Bean
    public GrpcServerConfigurer grpcServerConfigurer(GrpcProperties grpcProperties) {
        NettyProperties nettyProperties = grpcProperties.getNetty();

        Class channelType;
        EventLoopGroup bossEventLoopGroup;
        EventLoopGroup workerEventLoopGroup;

        // specify channel type and eventloop group properties
        if (nettyProperties.isNioChannelType()) {
            channelType = NioServerSocketChannel.class;
            bossEventLoopGroup = new NioEventLoopGroup(nettyProperties.getBossEventLoopGroupPoolCount());
            workerEventLoopGroup = new NioEventLoopGroup(nettyProperties.getWorkerEventLoopGroupPoolCount());
        } else {
            channelType = EpollServerSocketChannel.class;
            bossEventLoopGroup = new EpollEventLoopGroup(nettyProperties.getBossEventLoopGroupPoolCount());
            workerEventLoopGroup = new EpollEventLoopGroup(nettyProperties.getWorkerEventLoopGroupPoolCount());
        }

        return serverBuilder -> ((NettyServerBuilder) serverBuilder)
                .flowControlWindow(nettyProperties.getFlowControlWindow())
                .maxConcurrentCallsPerConnection(nettyProperties.getMaxConcurrentCallsPerConnection())
                .maxInboundMessageSize(nettyProperties.getMaxMessageSize())
                .maxInboundMetadataSize(nettyProperties.getMaxMetadataSize())
                .channelType(channelType)
                .bossEventLoopGroup(bossEventLoopGroup)
                .workerEventLoopGroup(workerEventLoopGroup);
    }
}
