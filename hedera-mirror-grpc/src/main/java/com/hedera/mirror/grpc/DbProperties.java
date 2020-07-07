package com.hedera.mirror.grpc;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("hedera.mirror.grpc.db")
public class DbProperties {
    @NotBlank
    private String host = "";

    @NotBlank
    private String name = "";

    @NotBlank
    private String password = "";

    @Min(0)
    private int port = 5432;

    @NotBlank
    private String username = "";
}
