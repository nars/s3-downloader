package com.nxber.tools.s3downloader.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.stereotype.Component;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@Component
public class S3SourceManager {
	private static final String PROPERTY_PREFIX = "storage.s3.";
	private static final Set<String> RESERVED_KEYS = Set.of(
		"region",
		"endpoint",
		"path-style-access",
		"access-key",
		"secret-key",
		"default-bucket",
		"display-name",
		"page-size",
		"search-page-limit",
		"default-source"
	);

	private final Map<String, S3Source> sources;
	private final String defaultSourceName;

	public S3SourceManager(Environment environment, S3ClientProperties properties) {
		if (!(environment instanceof ConfigurableEnvironment configurableEnvironment)) {
			throw new IllegalStateException("S3SourceManager requires a ConfigurableEnvironment");
		}

		Binder binder = Binder.get(environment);
		List<String> discoveredNames = discoverSourceNames(configurableEnvironment);

		Map<String, S3Source> resolvedSources = new LinkedHashMap<>();
		for (String name : discoveredNames) {
			S3SourceProperties sourceProperties = binder.bind(PROPERTY_PREFIX + name, Bindable.of(S3SourceProperties.class))
				.orElseThrow(() -> new IllegalStateException("Missing configuration for storage source '%s'".formatted(name)));
			S3Client client = buildClient(sourceProperties);
			String displayName = Optional.ofNullable(sourceProperties.getDisplayName())
				.orElseGet(() -> name.replace('-', ' '));
			resolvedSources.put(name, new S3Source(name, displayName, sourceProperties, client));
		}

		if (resolvedSources.isEmpty()) {
			throw new IllegalStateException("No S3 sources configured. Define at least one under 'storage.s3.<name>.*'");
		}

		this.sources = Collections.unmodifiableMap(resolvedSources);
		this.defaultSourceName = determineDefaultSource(properties.getDefaultSource(), resolvedSources.keySet());
	}

	private List<String> discoverSourceNames(ConfigurableEnvironment environment) {
		MutablePropertySources sources = environment.getPropertySources();
		List<String> names = new ArrayList<>();

		sources.forEach(propertySource -> {
			if (propertySource instanceof EnumerablePropertySource<?> enumerable) {
				for (String propertyName : enumerable.getPropertyNames()) {
					if (!propertyName.startsWith(PROPERTY_PREFIX)) {
						continue;
					}
					String remainder = propertyName.substring(PROPERTY_PREFIX.length());
					int dotIndex = remainder.indexOf('.');
					if (dotIndex < 0) {
						continue;
					}
					String candidate = remainder.substring(0, dotIndex);
					if (!RESERVED_KEYS.contains(candidate)) {
						if (!names.contains(candidate)) {
							names.add(candidate);
						}
					}
				}
			}
		});

		return names;
	}

	private String determineDefaultSource(String configuredDefault, Set<String> available) {
		if (configuredDefault != null && available.contains(configuredDefault)) {
			return configuredDefault;
		}
		return available.iterator().next();
	}

	private S3Client buildClient(S3SourceProperties properties) {
		S3Configuration.Builder serviceConfig = S3Configuration.builder();

		if (properties.isPathStyleAccess()) {
			serviceConfig.pathStyleAccessEnabled(true);
		}

		S3ClientBuilder builder = S3Client.builder()
			.region(Region.of(properties.getRegion()))
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
				properties.getAccessKey(),
				properties.getSecretKey()
			)))
			.httpClientBuilder(ApacheHttpClient.builder())
			.serviceConfiguration(serviceConfig.build());

		if (properties.getEndpoint() != null) {
			builder = builder.endpointOverride(properties.getEndpoint());
		}

		return builder.build();
	}

	public List<S3Source> getSources() {
		return List.copyOf(sources.values());
	}

	public S3Source getDefaultSource() {
		return sources.get(defaultSourceName);
	}

	public S3Source resolve(String requestedName) {
		if (requestedName == null || requestedName.isBlank()) {
			return getDefaultSource();
		}
		return sources.getOrDefault(requestedName, getDefaultSource());
	}

	public boolean exists(String name) {
		return sources.containsKey(name);
	}

	public record S3Source(String name, String displayName, S3SourceProperties properties, S3Client client) {
		public String defaultBucket() {
			return properties.getDefaultBucket();
		}
	}
}
