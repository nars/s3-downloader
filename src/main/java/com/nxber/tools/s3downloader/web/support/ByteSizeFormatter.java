package com.nxber.tools.s3downloader.web.support;

import java.util.Locale;

import org.springframework.stereotype.Component;

@Component("byteFormatter")
public class ByteSizeFormatter {
	private static final long UNIT = 1024L;
	private static final String[] UNITS = {"B", "KB", "MB", "GB", "TB", "PB"};

	public String format(long bytes) {
		if (bytes < UNIT) {
			return bytes + " B";
		}

		double value = bytes;
		int unitIndex = 0;

		while (value >= UNIT && unitIndex < UNITS.length - 1) {
			value /= UNIT;
			unitIndex++;
		}

		return String.format(Locale.US, "%.2f %s", value, UNITS[unitIndex]);
	}
}
