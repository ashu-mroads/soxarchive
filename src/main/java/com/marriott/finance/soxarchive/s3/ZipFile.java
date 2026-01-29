package com.marriott.finance.soxarchive.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.marriott.finance.soxarchive.model.Integration;

public class ZipFile {
	
	public static File createZipFile(
	        Integration integration,
	        Instant windowStart,
	        List<String> jsonLines
	) throws IOException {

	    File zipFile = File.createTempFile(
	            "bizevents-" + integration.getId() + "-",
	            ".zip"
	    );

	    try (ZipOutputStream zos =
	                 new ZipOutputStream(
	                         new FileOutputStream(zipFile))) {

	        ZipEntry entry =
	                new ZipEntry("events.jsonl");

	        zos.putNextEntry(entry);

	        for (String line : jsonLines) {
	            zos.write(line.getBytes());
	            zos.write('\n');
	        }

	        zos.closeEntry();
	    }

	    return zipFile;
	}

}
