package com.nxber.tools.s3downloader.service;

public class StorageAccessException extends RuntimeException {
	public StorageAccessException(String message, Throwable cause) {
		super(message, cause);
	}
}
