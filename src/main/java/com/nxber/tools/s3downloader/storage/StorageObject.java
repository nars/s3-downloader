package com.nxber.tools.s3downloader.storage;

import java.time.Instant;

public record StorageObject(
	String key,
	String name,
	long size,
	Instant lastModified,
	String eTag,
	boolean previewable
) {
}
