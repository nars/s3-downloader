package com.nxber.tools.s3downloader.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mockito;

import com.nxber.tools.s3downloader.config.S3ClientProperties;
import com.nxber.tools.s3downloader.config.S3SourceManager;
import com.nxber.tools.s3downloader.config.S3SourceManager.S3Source;
import com.nxber.tools.s3downloader.config.S3SourceProperties;
import com.nxber.tools.s3downloader.storage.StorageListing;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

@ExtendWith(MockitoExtension.class)
class StorageBrowserServiceTest {
	@Mock
	private S3Client s3Client;

	@Mock
	private S3SourceManager sourceManager;

	private S3ClientProperties properties;
	private StorageBrowserService service;
	private S3Source source;

	@BeforeEach
	void setUp() {
		properties = new S3ClientProperties();
		properties.setPageSize(50);
		properties.setSearchPageLimit(3);
		properties.setDefaultSource("primary");

		S3SourceProperties sourceProperties = new S3SourceProperties();
		sourceProperties.setRegion("us-east-1");
		sourceProperties.setAccessKey("access");
		sourceProperties.setSecretKey("secret");
		sourceProperties.setDefaultBucket("default-bucket");

		source = new S3Source("primary", "Primary", sourceProperties, s3Client);

		when(sourceManager.resolve(Mockito.any())).thenReturn(source);

		service = new StorageBrowserService(sourceManager, properties);
	}

	@Test
	void shouldListFoldersAndFiles() {
		ListObjectsV2Response response = ListObjectsV2Response.builder()
			.commonPrefixes(CommonPrefix.builder().prefix("docs/reports/").build())
			.contents(
				S3Object.builder()
					.key("docs/readme.txt")
					.size(1024L)
					.lastModified(Instant.parse("2025-01-01T00:00:00Z"))
					.eTag("etag")
					.build()
			)
			.isTruncated(false)
			.build();

		ListObjectsV2Response folderResponse = ListObjectsV2Response.builder()
			.contents(
				S3Object.builder()
					.key("docs/reports/summary.txt")
					.size(2048L)
					.lastModified(Instant.parse("2025-01-01T00:00:01Z"))
					.eTag("etag-2")
					.build()
			)
			.isTruncated(false)
			.build();

		when(s3Client.listObjectsV2(Mockito.<ListObjectsV2Request>argThat(request -> request != null && "/".equals(request.delimiter())))).thenReturn(response);
		when(s3Client.listObjectsV2(Mockito.<ListObjectsV2Request>argThat(request -> request != null && request.delimiter() == null && "docs/reports/".equals(request.prefix()))))
			.thenReturn(folderResponse);

		StorageListing listing = service.listObjects("primary", "default-bucket", "docs", "", "", true);

		assertThat(listing.folders()).hasSize(1);
		assertThat(listing.folders().getFirst().name()).isEqualTo("reports");
		assertThat(listing.folders().getFirst().size()).isEqualTo(2048L);
		assertThat(listing.folders().getFirst().lastModified()).isEqualTo(Instant.parse("2025-01-01T00:00:01Z"));
		assertThat(listing.objects()).hasSize(1);
		assertThat(listing.objects().getFirst().name()).isEqualTo("readme.txt");
		assertThat(listing.objects().getFirst().previewable()).isFalse();
		assertThat(listing.hasNext()).isFalse();
	}

	@Test
	void shouldFilterResultsByQuery() {
		ListObjectsV2Response response = ListObjectsV2Response.builder()
			.contents(
				S3Object.builder().key("docs/alpha.txt").size(1L).lastModified(Instant.now()).eTag("1").build(),
				S3Object.builder().key("docs/beta.txt").size(1L).lastModified(Instant.now()).eTag("2").build()
			)
			.isTruncated(false)
			.build();

		when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

		StorageListing listing = service.listObjects("primary", "default-bucket", "docs", "", "beta", true);

		assertThat(listing.objects()).hasSize(1);
		assertThat(listing.objects().getFirst().name()).isEqualTo("beta.txt");
	}

	@Test
	void shouldSkipFolderDetailsWhenDisabled() {
		ListObjectsV2Response response = ListObjectsV2Response.builder()
			.commonPrefixes(CommonPrefix.builder().prefix("docs/reports/").build())
			.contents(
				S3Object.builder()
					.key("docs/reports/summary.txt")
					.size(2048L)
					.lastModified(Instant.parse("2025-01-01T00:00:01Z"))
					.eTag("etag-2")
					.build()
			)
			.isTruncated(false)
			.build();

		when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

		StorageListing listing = service.listObjects("primary", "default-bucket", "docs", "", "", false);

		assertThat(listing.folders()).hasSize(1);
		assertThat(listing.folders().getFirst().size()).isEqualTo(0L);
		assertThat(listing.folders().getFirst().lastModified()).isNull();
		verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
	}

	@Test
	void shouldFallbackToDefaultBucketWhenListBucketsDenied() {
		when(s3Client.listBuckets()).thenThrow((S3Exception) S3Exception.builder()
			.statusCode(403)
			.awsErrorDetails(AwsErrorDetails.builder()
				.errorCode("AccessDenied")
				.errorMessage("Access denied")
				.build())
			.build());

		List<String> bucketNames = service.listBuckets("primary").stream().map(bucket -> bucket.name()).toList();

		assertThat(bucketNames).containsExactly("default-bucket");
	}
}
