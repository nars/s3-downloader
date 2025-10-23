package com.nxber.tools.s3downloader.web;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.nxber.tools.s3downloader.config.S3SourceManager.S3Source;
import com.nxber.tools.s3downloader.service.StorageAccessException;
import com.nxber.tools.s3downloader.service.StorageBrowserService;
import com.nxber.tools.s3downloader.storage.BucketSummary;
import com.nxber.tools.s3downloader.storage.StorageListing;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Controller
public class BucketBrowserController {
	private static final DateTimeFormatter ZIP_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.US)
		.withZone(ZoneId.systemDefault());

	private final StorageBrowserService browserService;

	public BucketBrowserController(StorageBrowserService browserService) {
		this.browserService = browserService;
	}

	@GetMapping({"/", "/browser"})
	public String browse(
		@RequestParam(name = "source", required = false) String sourceName,
		@RequestParam(name = "bucket", required = false) String bucket,
		@RequestParam(name = "prefix", required = false, defaultValue = "") String prefix,
		@RequestParam(name = "tokenStack", required = false, defaultValue = "") String tokenStack,
		@RequestParam(name = "query", required = false, defaultValue = "") String query,
		@RequestParam(name = "showDetails", required = false, defaultValue = "false") boolean showDetails,
		Model model
	) {
		S3Source activeSource = browserService.resolveSource(sourceName);
		String activeSourceName = activeSource.name();
		String activeBucket = resolveBucket(activeSource, bucket);

		List<S3Source> sources = browserService.listSources();
		List<BucketSummary> buckets = browserService.listBuckets(activeSourceName);
		StorageListing listing;
		String errorMessage = null;
		try {
			listing = browserService.listObjects(activeSourceName, activeBucket, prefix, tokenStack, query, showDetails);
		} catch (StorageAccessException exception) {
			errorMessage = exception.getMessage();
			listing = new StorageListing(activeBucket, "", List.of(), List.of(), false, "", "", "");
		}
		List<Breadcrumb> breadcrumbs = buildBreadcrumbs(listing.prefix());
		String parentPrefix = computeParentPrefix(listing.prefix());

		model.addAttribute("sources", sources);
		model.addAttribute("activeSource", activeSourceName);
		model.addAttribute("activeSourceDisplayName", activeSource.displayName());
		model.addAttribute("buckets", buckets);
		model.addAttribute("activeBucket", activeBucket);
		model.addAttribute("listing", listing);
		model.addAttribute("breadcrumbs", breadcrumbs);
		model.addAttribute("parentPrefix", parentPrefix);
		model.addAttribute("query", query);
		model.addAttribute("errorMessage", errorMessage);
		model.addAttribute("showDetails", showDetails);

		return "index";
	}

	@GetMapping(path = "/download")
	public ResponseEntity<StreamingResponseBody> downloadFile(
		@RequestParam(name = "source", required = false) String sourceName,
		@RequestParam("bucket") String bucket,
		@RequestParam("key") String key
	) {
		String filename = extractFileName(key);

		ResponseInputStream<GetObjectResponse> objectStream;
		try {
			objectStream = browserService.openObjectStream(sourceName, bucket, key);
		} catch (S3Exception exception) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, exception.awsErrorDetails().errorMessage(), exception);
		}

		long contentLength = Optional.ofNullable(objectStream.response().contentLength()).orElse(-1L);

		StreamingResponseBody body = outputStream -> {
			try (objectStream) {
				objectStream.transferTo(outputStream);
			}
		};

		ResponseEntity.BodyBuilder builder = ResponseEntity.ok()
			.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
			.contentType(MediaType.APPLICATION_OCTET_STREAM);

		if (contentLength >= 0) {
			builder = builder.contentLength(contentLength);
		}

		return builder.body(body);
	}

	@GetMapping(path = "/preview")
	public ResponseEntity<StreamingResponseBody> previewImage(
		@RequestParam(name = "source", required = false) String sourceName,
		@RequestParam("bucket") String bucket,
		@RequestParam("key") String key
	) {
		if (!browserService.supportsInlinePreview(key)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Preview not supported for this object");
		}

		ResponseInputStream<GetObjectResponse> objectStream;
		try {
			objectStream = browserService.openObjectStream(sourceName, bucket, key);
		} catch (S3Exception exception) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, exception.awsErrorDetails().errorMessage(), exception);
		}

		StreamingResponseBody body = outputStream -> {
			try (objectStream) {
				objectStream.transferTo(outputStream);
			}
		};

		String contentTypeValue = objectStream.response().contentType();
		MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM;
		if (StringUtils.hasText(contentTypeValue)) {
			try {
				mediaType = MediaType.parseMediaType(contentTypeValue);
			} catch (IllegalArgumentException ignored) {
				mediaType = MediaType.APPLICATION_OCTET_STREAM;
			}
		} else {
			mediaType = MediaTypeFactory.getMediaType(key).orElse(MediaType.APPLICATION_OCTET_STREAM);
		}

		long contentLength = Optional.ofNullable(objectStream.response().contentLength()).orElse(-1L);

		ResponseEntity.BodyBuilder builder = ResponseEntity.ok()
			.contentType(mediaType)
			.header(HttpHeaders.CACHE_CONTROL, "public, max-age=300");

		if (contentLength >= 0) {
			builder = builder.contentLength(contentLength);
		}

		return builder.body(body);
	}

	@PostMapping(path = "/download/batch")
	public ResponseEntity<StreamingResponseBody> downloadSelection(
		@RequestParam(name = "source", required = false) String sourceName,
		@RequestParam("bucket") String bucket,
		@RequestParam("keys") List<String> keys
	) {
		if (CollectionUtils.isEmpty(keys)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No objects selected for download");
		}

		List<String> sanitizedKeys = keys.stream()
			.filter(StringUtils::hasText)
			.distinct()
			.toList();

		if (sanitizedKeys.isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No objects selected for download");
		}

		String zipName = "download-" + ZIP_TIMESTAMP.format(java.time.Instant.now()) + ".zip";

		StreamingResponseBody body = outputStream -> {
			try (java.util.zip.ZipOutputStream zipOutputStream = new java.util.zip.ZipOutputStream(outputStream)) {
				browserService.streamObjectsAsZip(sourceName, bucket, sanitizedKeys, zipOutputStream);
			}
		};

		return ResponseEntity.ok()
			.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + zipName + "\"")
			.contentType(MediaType.APPLICATION_OCTET_STREAM)
			.body(body);
	}

	@GetMapping(path = "/download/folder")
	public ResponseEntity<StreamingResponseBody> downloadFolder(
		@RequestParam(name = "source", required = false) String sourceName,
		@RequestParam("bucket") String bucket,
		@RequestParam("prefix") String prefix
	) {
		if (!StringUtils.hasText(prefix)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Folder prefix is required");
		}

		String folderName = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
		folderName = folderName.contains("/") ? folderName.substring(folderName.lastIndexOf('/') + 1) : folderName;
		String zipName = folderName + "-" + ZIP_TIMESTAMP.format(java.time.Instant.now()) + ".zip";

		StreamingResponseBody body = outputStream -> {
			try (java.util.zip.ZipOutputStream zipOutputStream = new java.util.zip.ZipOutputStream(outputStream)) {
				browserService.streamPrefixAsZip(sourceName, bucket, prefix, zipOutputStream);
			}
		};

		return ResponseEntity.ok()
			.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + zipName + "\"")
			.contentType(MediaType.APPLICATION_OCTET_STREAM)
			.body(body);
	}

	private String resolveBucket(S3Source source, String bucket) {
		if (StringUtils.hasText(bucket)) {
			return bucket;
		}
		return source.defaultBucket();
	}

	private String extractFileName(String key) {
		if (!StringUtils.hasText(key)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Object key is required");
		}
		int separatorIndex = key.lastIndexOf('/');
		return separatorIndex >= 0 ? key.substring(separatorIndex + 1) : key;
	}

	private List<Breadcrumb> buildBreadcrumbs(String prefix) {
		List<Breadcrumb> breadcrumbs = new ArrayList<>();
		if (!StringUtils.hasText(prefix)) {
			return breadcrumbs;
		}

		String[] segments = prefix.split("/");
		StringBuilder cumulative = new StringBuilder();
		for (String segment : segments) {
			if (!StringUtils.hasText(segment)) {
				continue;
			}
			cumulative.append(segment).append('/');
			breadcrumbs.add(new Breadcrumb(segment, cumulative.toString()));
		}

		return breadcrumbs;
	}

	private String computeParentPrefix(String prefix) {
		if (!StringUtils.hasText(prefix)) {
			return "";
		}

		String trimmed = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
		int separatorIndex = trimmed.lastIndexOf('/');
		return separatorIndex < 0 ? "" : trimmed.substring(0, separatorIndex + 1);
	}

	public record Breadcrumb(String label, String prefix) {
	}
}
