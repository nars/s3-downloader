package com.nxber.tools.s3downloader.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.nxber.tools.s3downloader.config.S3ClientProperties;
import com.nxber.tools.s3downloader.config.S3SourceManager;
import com.nxber.tools.s3downloader.config.S3SourceManager.S3Source;
import com.nxber.tools.s3downloader.storage.BucketSummary;
import com.nxber.tools.s3downloader.storage.StorageFolder;
import com.nxber.tools.s3downloader.storage.StorageListing;
import com.nxber.tools.s3downloader.storage.StorageObject;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@Service
public class StorageBrowserService {
	private static final String TOKEN_DELIMITER = "::";
	private static final Set<String> PREVIEWABLE_IMAGE_EXTENSIONS = Set.of("jpg", "jpeg", "png", "gif", "webp", "bmp", "tif", "tiff", "avif", "svg");

	private final S3SourceManager sourceManager;
	private final S3ClientProperties properties;

	public StorageBrowserService(S3SourceManager sourceManager, S3ClientProperties properties) {
		this.sourceManager = sourceManager;
		this.properties = properties;
	}

	public List<S3Source> listSources() {
		return sourceManager.getSources();
	}

	public S3Source resolveSource(String sourceName) {
		return sourceManager.resolve(sourceName);
	}

	public List<BucketSummary> listBuckets(String sourceName) {
		S3Source source = sourceManager.resolve(sourceName);
		S3Client s3Client = source.client();
		try {
			ListBucketsResponse response = s3Client.listBuckets();
			return response.buckets().stream()
				.map(bucket -> new BucketSummary(bucket.name()))
				.toList();
		} catch (S3Exception exception) {
			if (exception.statusCode() == 403 || exception.statusCode() == 401) {
				return List.of(new BucketSummary(source.defaultBucket()));
			}
			throw exception;
		}
	}

	public StorageListing listObjects(String sourceName, String bucket, String prefix, String tokenStack, String query, boolean includeFolderDetails) {
		S3Source source = sourceManager.resolve(sourceName);
		S3Client s3Client = source.client();
		String effectiveBucket = StringUtils.hasText(bucket) ? bucket : source.defaultBucket();

		String normalizedPrefix = normalizePrefix(prefix);
		Deque<String> decodedTokenStack = decodeTokenStack(tokenStack);
		String continuationToken = decodedTokenStack.peekLast();

		int searchIterations = 0;

		List<StorageObject> objects = new ArrayList<>();
		List<StorageFolder> folders = new ArrayList<>();
		Map<String, FolderStats> folderStatsCache = includeFolderDetails ? new HashMap<>() : Map.of();

		boolean truncated = false;

		String currentToken = continuationToken;
		String nextTokenForStack = null;

		Predicate<String> matchesQuery = buildMatcher(query);

		do {
			ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
				.bucket(effectiveBucket)
				.prefix(normalizedPrefix)
				.delimiter("/")
				.maxKeys(properties.getPageSize());

			if (StringUtils.hasText(currentToken)) {
				requestBuilder = requestBuilder.continuationToken(currentToken);
			}

			ListObjectsV2Response response;
			try {
				response = s3Client.listObjectsV2(requestBuilder.build());
			} catch (S3Exception exception) {
				throw translateException(exception, effectiveBucket, source);
			}
			searchIterations++;

			List<StorageFolder> pageFolders = response.commonPrefixes().stream()
				.map(CommonPrefix::prefix)
				.map(prefixValue -> new StorageFolder(folderName(prefixValue), prefixValue, 0L, null))
				.filter(folder -> matchesQuery.test(folder.name()))
				.toList();

			List<StorageObject> pageObjects = response.contents().stream()
				.filter(object -> !object.key().endsWith("/"))
				.map(object -> {
					String keyValue = object.key();
					String objectName = fileName(keyValue);
					return new StorageObject(
						keyValue,
						objectName,
						object.size(),
						object.lastModified(),
						object.eTag(),
						isPreviewableImageKey(keyValue)
					);
				})
				.filter(object -> matchesQuery.test(object.name()))
				.toList();

			folders.addAll(pageFolders);
			objects.addAll(pageObjects);

			truncated = response.isTruncated();
			nextTokenForStack = response.nextContinuationToken();

			boolean reachedDisplayCapacity = objects.size() >= properties.getPageSize();
			boolean reachedSearchLimit = searchIterations >= properties.getSearchPageLimit();

			if (!StringUtils.hasText(query)) {
				break;
			}

			if (!truncated || reachedDisplayCapacity || reachedSearchLimit) {
				break;
			}

			currentToken = response.nextContinuationToken();
		} while (true);

		List<StorageObject> trimmedObjects = limitList(objects, properties.getPageSize());
		List<StorageFolder> trimmedFolders = limitList(folders, properties.getPageSize());
		List<StorageFolder> sizedFolders;
		if (includeFolderDetails) {
			sizedFolders = trimmedFolders.stream()
				.map(folder -> {
					// cache folder metrics per prefix to avoid repeat listings during this request
					FolderStats stats = folderStatsCache.computeIfAbsent(folder.prefix(), key -> calculateFolderStats(source, effectiveBucket, key));
					return new StorageFolder(
						folder.name(),
						folder.prefix(),
						stats.size(),
						stats.lastModified()
					);
				})
				.toList();
		} else {
			sizedFolders = trimmedFolders.stream()
				.map(folder -> new StorageFolder(folder.name(), folder.prefix(), 0L, null))
				.toList();
		}

		String encodedCurrentStack = encodeTokenStack(decodedTokenStack);
		String encodedNextStack = StringUtils.hasText(nextTokenForStack)
			? appendToStack(encodedCurrentStack, nextTokenForStack)
			: "";
		String encodedPreviousStack = decodedTokenStack.isEmpty() ? "" : dropLastFromStack(encodedCurrentStack);

		return new StorageListing(
			effectiveBucket,
			normalizedPrefix,
			sizedFolders,
			trimmedObjects,
			truncated && StringUtils.hasText(nextTokenForStack),
			encodedCurrentStack,
			encodedNextStack,
			encodedPreviousStack
		);
	}

	public ResponseInputStream<GetObjectResponse> openObjectStream(String sourceName, String bucket, String key) {
		S3Source source = sourceManager.resolve(sourceName);
		String effectiveBucket = StringUtils.hasText(bucket) ? bucket : source.defaultBucket();
		return source.client().getObject(GetObjectRequest.builder()
			.bucket(effectiveBucket)
			.key(key)
			.build());
	}

	public Map<String, Long> streamObjectsAsZip(String sourceName, String bucket, Collection<String> keys, ZipOutputStream zipOutputStream) {
		S3Source source = sourceManager.resolve(sourceName);
		S3Client s3Client = source.client();
		String effectiveBucket = StringUtils.hasText(bucket) ? bucket : source.defaultBucket();
		Map<String, Long> transferredBytes = new HashMap<>();
		keys.stream()
			.filter(StringUtils::hasText)
			.forEach(key -> writeObjectToZip(s3Client, effectiveBucket, key, zipOutputStream, transferredBytes, source));
		return transferredBytes;
	}

	public Map<String, Long> streamPrefixAsZip(String sourceName, String bucket, String prefix, ZipOutputStream zipOutputStream) {
		S3Source source = sourceManager.resolve(sourceName);
		S3Client s3Client = source.client();
		String effectiveBucket = StringUtils.hasText(bucket) ? bucket : source.defaultBucket();
		Map<String, Long> transferredBytes = new HashMap<>();
		String normalizedPrefix = normalizePrefix(prefix);

		ListObjectsV2Request request = ListObjectsV2Request.builder()
			.bucket(effectiveBucket)
			.prefix(normalizedPrefix)
			.build();

		ListObjectsV2Iterable iterable;
		try {
			iterable = s3Client.listObjectsV2Paginator(request);
		} catch (S3Exception exception) {
			throw translateException(exception, effectiveBucket, source);
		}
		for (ListObjectsV2Response response : iterable) {
			for (S3Object object : response.contents()) {
				if (object.key().endsWith("/")) {
					continue;
				}
				writeObjectToZip(s3Client, effectiveBucket, object.key(), zipOutputStream, transferredBytes, normalizedPrefix, source);
			}
		}

		return transferredBytes;
	}

	private void writeObjectToZip(S3Client s3Client, String bucket, String key, ZipOutputStream zipOutputStream, Map<String, Long> transferredBytes, S3Source source) {
		writeObjectToZip(s3Client, bucket, key, zipOutputStream, transferredBytes, "", source);
	}

	private void writeObjectToZip(S3Client s3Client, String bucket, String key, ZipOutputStream zipOutputStream, Map<String, Long> transferredBytes, String prefixToTrim, S3Source source) {
		String entryName = sanitizeEntryName(key, prefixToTrim);

		try (ResponseInputStream<GetObjectResponse> objectStream = s3Client.getObject(GetObjectRequest.builder()
			.bucket(bucket)
			.key(key)
			.build())) {
			ZipEntry entry = new ZipEntry(entryName);
			Long contentLength = objectStream.response().contentLength();
			if (contentLength != null && contentLength >= 0) {
				entry.setSize(contentLength);
			}
			zipOutputStream.putNextEntry(entry);
			transfer(objectStream, zipOutputStream);
			zipOutputStream.closeEntry();
			transferredBytes.put(key, contentLength != null ? contentLength : -1L);
		} catch (IOException exception) {
			throw new UncheckedIOException("Failed to add object '%s' to archive".formatted(key), exception);
		} catch (S3Exception exception) {
			throw translateException(exception, bucket, source);
		}
	}

	private void transfer(InputStream inputStream, ZipOutputStream outputStream) throws IOException {
		inputStream.transferTo(outputStream);
	}

	private Predicate<String> buildMatcher(String query) {
		if (!StringUtils.hasText(query)) {
			return value -> true;
		}

		String normalizedQuery = query.toLowerCase();
		return value -> value != null && value.toLowerCase().contains(normalizedQuery);
	}

	private String normalizePrefix(String prefix) {
		if (!StringUtils.hasText(prefix)) {
			return "";
		}

		String normalized = prefix.trim();
		if (!normalized.endsWith("/")) {
			normalized = normalized + "/";
		}
		return normalized;
	}

	private String folderName(String prefix) {
		String trimmed = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
		int separatorIndex = trimmed.lastIndexOf('/');
		return separatorIndex >= 0 ? trimmed.substring(separatorIndex + 1) : trimmed;
	}

	private String fileName(String key) {
		int separatorIndex = key.lastIndexOf('/');
		return separatorIndex >= 0 ? key.substring(separatorIndex + 1) : key;
	}

	private String sanitizeEntryName(String key, String prefixToTrim) {
		String sanitized = key;
		if (StringUtils.hasText(prefixToTrim) && sanitized.startsWith(prefixToTrim)) {
			sanitized = sanitized.substring(prefixToTrim.length());
		}
		if (sanitized.startsWith("/")) {
			sanitized = sanitized.substring(1);
		}
		sanitized = sanitized.replace("..", "");
		return sanitized;
	}

	private Deque<String> decodeTokenStack(String encodedStack) {
		Deque<String> tokens = new ArrayDeque<>();
		if (!StringUtils.hasText(encodedStack)) {
			return tokens;
		}

		for (String encoded : encodedStack.split(TOKEN_DELIMITER)) {
			if (!StringUtils.hasText(encoded)) {
				continue;
			}
			tokens.addLast(new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8));
		}

		return tokens;
	}

	private String encodeTokenStack(Deque<String> tokens) {
		return tokens.stream()
			.filter(StringUtils::hasText)
			.map(token -> Base64.getUrlEncoder().withoutPadding().encodeToString(token.getBytes(StandardCharsets.UTF_8)))
			.reduce((left, right) -> left + TOKEN_DELIMITER + right)
			.orElse("");
	}

	private String appendToStack(String encodedStack, String tokenToAppend) {
		if (!StringUtils.hasText(tokenToAppend)) {
			return encodedStack;
		}

		String encodedToken = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenToAppend.getBytes(StandardCharsets.UTF_8));
		if (!StringUtils.hasText(encodedStack)) {
			return encodedToken;
		}

		return encodedStack + TOKEN_DELIMITER + encodedToken;
	}

	private String dropLastFromStack(String encodedStack) {
		if (!StringUtils.hasText(encodedStack)) {
			return "";
		}

		int lastDelimiterIndex = encodedStack.lastIndexOf(TOKEN_DELIMITER);
		if (lastDelimiterIndex < 0) {
			return "";
		}

		return encodedStack.substring(0, lastDelimiterIndex);
	}

	private <T> List<T> limitList(List<T> source, int maxSize) {
		return source.size() <= maxSize ? source : source.subList(0, maxSize);
	}

	public boolean supportsInlinePreview(String key) {
		return isPreviewableImageKey(key);
	}

	private FolderStats calculateFolderStats(S3Source source, String bucket, String prefix) {
		if (!StringUtils.hasText(prefix)) {
			return new FolderStats(0L, null);
		}

		String normalizedPrefix = normalizePrefix(prefix);
		String continuationToken = null;
		long totalSize = 0L;
		Instant mostRecent = null;
		int pageSize = Math.max(1, properties.getPageSize());
		S3Client s3Client = source.client();

		while (true) {
			ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
				.bucket(bucket)
				.prefix(normalizedPrefix)
				.maxKeys(pageSize);

			if (StringUtils.hasText(continuationToken)) {
				requestBuilder = requestBuilder.continuationToken(continuationToken);
			}

			ListObjectsV2Response response;
			try {
				response = s3Client.listObjectsV2(requestBuilder.build());
			} catch (S3Exception exception) {
				throw translateException(exception, bucket, source);
			}

			for (S3Object object : response.contents()) {
				if (!object.key().endsWith("/")) {
					totalSize += object.size();
					Instant objectLastModified = object.lastModified();
					if (objectLastModified != null && (mostRecent == null || objectLastModified.isAfter(mostRecent))) {
						mostRecent = objectLastModified;
					}
				}
			}

			if (!response.isTruncated()) {
				break;
			}

			String nextContinuationToken = response.nextContinuationToken();
			if (!StringUtils.hasText(nextContinuationToken) || nextContinuationToken.equals(continuationToken)) {
				break;
			}
			continuationToken = nextContinuationToken;
		}

		return new FolderStats(totalSize, mostRecent);
	}

	private record FolderStats(long size, Instant lastModified) {
	}

	private boolean isPreviewableImageKey(String key) {
			if (!StringUtils.hasText(key)) {
				return false;
			}

			String lowercaseKey = key.toLowerCase(Locale.ROOT);
			int lastDotIndex = lowercaseKey.lastIndexOf('.');
			if (lastDotIndex < 0 || lastDotIndex == lowercaseKey.length() - 1) {
				return false;
			}

			String extension = lowercaseKey.substring(lastDotIndex + 1);
			return PREVIEWABLE_IMAGE_EXTENSIONS.contains(extension);
		}

	private RuntimeException translateException(S3Exception exception, String bucket, S3Source source) {
		int statusCode = exception.statusCode();
		if (statusCode == 403) {
			return new StorageAccessException("Access denied while accessing bucket '%s' in source '%s'. Ensure the credentials include list and read permissions.".formatted(bucket, source.name()), exception);
		}
		if (statusCode == 404) {
			return new StorageAccessException("Bucket '%s' in source '%s' was not found. Confirm the bucket name and region.".formatted(bucket, source.name()), exception);
		}
		return exception;
	}
}
