package com.ftc.miniotest.config;

import io.minio.MinioClient;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-10-13 14:45:22
 * @describe: minio配置
 */
@Data
@Component
@ConfigurationProperties("minio")
public class MinioConfig {

    /**
     * 服务器地址
     */
    private String endpoint;

    /**
     * 服务器端口
     */
    private int port;

    /**
     * 用户名
     */
    private String accessKey;

    /**
     * 密码
     */
    private String secretKey;

    /**
     * 是否使用https访问,默认为true
     */
    private Boolean secure;

    /**
     * 默认桶名称
     */
    private String bucket;

    @Bean
    public MinioClient getMinioClient() {
        return MinioClient.builder()
                .endpoint(endpoint, port, secure)
                .credentials(accessKey, secretKey)
                .build();
    }
}
