package com.nxber.tools.s3downloader.storage;

import java.time.Instant;

public record StorageFolder(
	String name,
	String prefix,
	long size,
	Instant lastModified
) {
}
