package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.utils.Credentials;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


import com.fasterxml.jackson.core.JsonProcessingException;

class FTPTests {
	
	@Test@Tag("slow")
	void testListing() throws JsonProcessingException, IOException {
		Credentials creds = Credentials.read(new File(System.getenv("HOME") + "/.credentials.json"));
		try (FTPProcessing ftpProc = new FTPProcessing(creds, "pubmedcommons")) {
			File processingStatus = File.createTempFile("processingStatus", ".json");
			Files.write(processingStatus.toPath(), "{}".getBytes());
			ftpProc.processRemoteDirectory(
					creds.get("pubmedcommons", "remotepath"), 
					processingStatus, FTPProcessing.skipExtensions("md5"), 
					file -> {
						System.out.printf("File %s\n", file.getName());
						return FTPProcessing.Status.Seen;
					}, Integer.MAX_VALUE);
		}
	}
}
