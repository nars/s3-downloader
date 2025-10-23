package com.nxber.tools.s3downloader.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(S3ClientProperties.class)
public class S3ClientConfiguration {
}
