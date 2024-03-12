package org.curieo.retrieve.ftp;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.io.IOUtils;
import org.curieo.utils.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import lombok.Generated;
import lombok.Value;

public class FTPProcessing implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(FTPProcessing.class);

	private static final ObjectReader OBJECT_READER;
	private static final ObjectWriter OBJECT_WRITER;
	Credentials creds;
	String key;
	FTPClient ftp;

	static {
		TypeReference<HashMap<String, Status>> typeRef 
		  	= new TypeReference<HashMap<String, Status>>() {};
	  	OBJECT_READER = new ObjectMapper().readerFor(typeRef);
	  	OBJECT_WRITER = new ObjectMapper().writerFor(typeRef).withDefaultPrettyPrinter();
	}
	
	public FTPProcessing(Credentials credentials, String key) throws IOException {
		this.key = key;
		this.creds = credentials;
		connect();
	}
	
	public String getServer() {
		return creds.get(key, "server");
	}

	public String getUser() {
		return creds.get(key, "user");
	}

	public String getPassword() {
		return creds.get(key, "password");
	}

	public static enum Status {
		Success, // submitted, fully completed, processed, no need to resubmit
		Error,   // needs redo
		Open,    // as yet undone
		Seen     // submitted, not processed, but no need to submit to processor
	}

	/**
	 * If the path does not exist, returns empty map.
	 * @param path
	 * @return instantiated status map
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	public static Map<String, Status> readProcessingStatus(File path) throws JsonProcessingException, IOException {
		if (!path.exists()) {
			LOGGER.warn("New status map created.");
			return new HashMap<>();
		}
		return OBJECT_READER.readValue(path);
	}

	/**
	 * write the status map
	 * @param path
	 * @return instantiated status map
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	public static void writeProcessingStatus(Map<String, Status> status, File path) throws JsonProcessingException, IOException {
		OBJECT_WRITER.writeValue(path, status);
	}
	
	public void reopenIfClosed() throws SocketException, IOException {
		if (!ftp.isConnected()) {
			ftp.disconnect();
			connect();
		}
	}
	
	@Override
	public void close() {
		if (ftp.isConnected()) {
	        try {
	        	ftp.disconnect();
	        	LOGGER.info("Closed");
	        } catch (IOException ioe) {
	        	LOGGER.warn("Could not close FTP client.");
	          // do nothing
	        }
		}
	}
	
	/**
	 * Synchronize a remote directory and a local directory.
	 * @param ftp
	 * @param remoteDirectory
	 * @param localDirectory
	 */
	public static void synchronize(String remoteDirectory, File localDirectory) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Synchronize a remote directory and a local directory.
	 * @param ftp
	 * @param remoteDirectory
	 * @param processingStatus
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	public void processRemoteDirectory(String remoteDirectory, 
									  File processingStatus,
									  Function<FTPFile, Status> unseenFilter,
									  Function<File, Status> processor) throws JsonProcessingException, IOException {
		Map<String, Status> statusMap = readProcessingStatus(processingStatus);
		// update the status map
		int newFiles = 0;
		reopenIfClosed();
		for (FTPFile file : ftp.listFiles(remoteDirectory)) {
			Status status = statusMap.get(file.getName());
			if (status == null) {
				statusMap.put(file.getName(), unseenFilter.apply(file));
				newFiles ++;
			}
		}
		if (newFiles != 0) {
			writeProcessingStatus(statusMap, processingStatus);
		}
		for (Map.Entry<String, Status> status : statusMap.entrySet()) {
			if (status.getValue() == Status.Open || status.getValue() == Status.Error) {
				// retrieve the remote file, and submit.
				reopenIfClosed();
				File tmp = File.createTempFile(prefix(status.getKey()), suffix(status.getKey()));
				String remoteFile;
				if (remoteDirectory.endsWith("/")) {
					remoteFile = remoteDirectory + status.getKey();
				}
				else {
					remoteFile = remoteDirectory + "/" + status.getKey();
				}
				if (!retrieveFile(remoteFile, tmp)) {
					LOGGER.error("Cannot retrieve file {}", remoteFile);
					status.setValue(Status.Error);
				}
				else {
					status.setValue(processor.apply(tmp));
					LOGGER.info("Processed {}: status = {}", status.getKey(), status.getValue());
				}
				
				if (!tmp.delete()) {
					LOGGER.error("Could not delete temp file {}", tmp.getAbsolutePath());
				}
				writeProcessingStatus(statusMap, processingStatus);
			}
		}
	}
	
	private boolean retrieveFile(String remoteFile, File localFile) throws IOException {
		FileOutputStream fos = new FileOutputStream(localFile);
		InputStream inputStream = ftp.retrieveFileStream(remoteFile);
		if (inputStream == null) {
			LOGGER.error(String.format("Retrieving remote file %s ended in null", remoteFile));
			System.exit(1);
		}
		IOUtils.copy(inputStream, fos);
		fos.flush();
		fos.close();
		inputStream.close();
		ftp.disconnect();
		return true;
	}
	
	
	private static String prefix(String name) {
		int dot = name.lastIndexOf('.');
		if (dot == -1) {
			return "tmp";
		}
		return name.substring(0, dot);
	}

	private static String suffix(String name) {
		int dot = name.lastIndexOf('.');
		if (dot == -1) {
			return "tmp";
		}
		if (name.toLowerCase().substring(dot).equals(".gz") && dot > 4) {
			int d = dot;
			while (d > 0 && name.toLowerCase().charAt(d-1) != '.') {
				d--;
			}
			if (d > 0 && (dot - d) < 4) {
				return name.substring(d-1);
			}
		}
		return name.substring(dot);
	}

	public static Function<FTPFile, Status> skipExtensions(String... extensions) {
		Set<String> ext = new HashSet<>();
		for (String e : extensions) {
			ext.add(e);
		}
		return new SkipExtension(ext);
	}
	
	@Generated @Value
	private static class SkipExtension implements Function<FTPFile, Status> {
		Set<String> extensions;
		
		@Override
		public Status apply(FTPFile t) {
			if (extensions.contains(suffix(t.getName()))) {
				return Status.Seen; // skip
			}
			else {
				return Status.Open;
			}
		}
		
	}
	
	private void connect() throws SocketException, IOException {
		ftp = new FTPClient();
		ftp.connect(getServer());
		// extremely important
		ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
		ftp.setBufferSize(1024*1024);
		//ftp.enterLocalPassiveMode();
		
		LOGGER.info("Connected to {}.", getServer());
		LOGGER.info(ftp.getReplyString());

		// After connection attempt, you should check the reply code to verify
		// success.
		int reply = ftp.getReplyCode();

		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			LOGGER.error("FTP server refused connection.");
		}
		
		ftp.login(getUser(), getPassword());
	}
}