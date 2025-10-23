package com.nxber.tools.s3downloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

@Validated
@ConfigurationProperties(prefix = "storage.s3")
public class S3ClientProperties {
	@Min(1)
	@Max(1000)
	private int pageSize = 200;

	@Min(1)
	private int searchPageLimit = 10;

	private String defaultSource;

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getSearchPageLimit() {
		return searchPageLimit;
	}

	public void setSearchPageLimit(int searchPageLimit) {
		this.searchPageLimit = searchPageLimit;
	}

	public String getDefaultSource() {
		return defaultSource;
	}

	public void setDefaultSource(String defaultSource) {
		this.defaultSource = defaultSource;
	}
}
