package com.nxber.tools.s3downloader.storage;

import java.util.List;

public record StorageListing(
	String bucket,
	String prefix,
	List<StorageFolder> folders,
	List<StorageObject> objects,
	boolean hasNext,
	String continuationToken,
	String nextContinuationToken,
	String previousContinuationToken
) {
}
