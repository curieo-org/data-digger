package org.curieo.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URIHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(URIHandler.class);
  private static final Map<String, Function<String, InputStream>> HANDLERS;
  private static final Map<String, Function<String, File>> RETRIEVERS;

  static {
    HANDLERS = new HashMap<>();
    HANDLERS.put("resource://", URIHandler::resourceHandler);
    HANDLERS.put("file://", URIHandler::fileHandler);
    RETRIEVERS = new HashMap<>();
    RETRIEVERS.put("resource://", URIHandler::resourceRetriever);
    RETRIEVERS.put("file://", URIHandler::fileRetriever);
    RETRIEVERS.put("ftp://", URIHandler::retrieveOverFTP);
    RETRIEVERS.put("http://", URIHandler::retrieveOverHTTP);
  }

  public static InputStream getResource(String uri) {
    for (Map.Entry<String, Function<String, InputStream>> handler : HANDLERS.entrySet()) {
      if (uri.startsWith(handler.getKey())) {
        return handler.getValue().apply(uri.substring(handler.getKey().length()));
      }
    }

    LOGGER.warn("No handler found for URL: {}", uri);
    return null;
  }

  public static boolean writeHTTPURL(String uri, File file) throws IOException {
    URL url = URI.create(uri).toURL(); // + "?id=" + pmcId);
    URLConnection urlcon = url.openConnection();
    HttpURLConnection con = (HttpURLConnection) urlcon;
    con.setRequestMethod("GET");
    int status = con.getResponseCode();
    if (status != 200) {
      LOGGER.warn("No response for URL {}", uri);
      return false;
    }
    try (FileOutputStream fos = new FileOutputStream(file)) {
      try (InputStream s = con.getInputStream()) {
        IOUtils.copy(s, fos);
        fos.flush();
      }
      con.disconnect();
    }

    return true;
  }

  private static File retrieveOverFTP(String path) {
    String ftpUri = "ftp://" + path;
    try {
      File file = File.createTempFile("temp", "tmp");
      if (!FTPProcessing.retrieve(ftpUri, file)) {
        LOGGER.warn("Could not download {}; not available.", ftpUri);
        return null;
      }
      return file;
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Cannot retrieve " + path, e);
    }
  }

  private static File retrieveOverHTTP(String path) {
    String httpUri = "http://" + path;
    try {
      File file = File.createTempFile("temp", "tmp");
      if (!writeHTTPURL(httpUri, file)) {
        LOGGER.warn("Could not download {}; not available.", httpUri);
        return null;
      }
      return file;
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve " + path, e);
    }
  }

  private static InputStream resourceHandler(String path) {
    return URIHandler.class.getResourceAsStream(path);
  }

  private static File resourceRetriever(String path) {
    try {
      File file = File.createTempFile("temp", "tmp");
      try (InputStream is = URIHandler.class.getResourceAsStream(path);
          FileOutputStream fos = new FileOutputStream(file)) {
        IOUtils.copy(is, fos);
      }
      return file;
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve " + path, e);
    }
  }

  private static File fileRetriever(String path) {
    return new File(path);
  }

  private static InputStream fileHandler(String path) {
    try {
      return new FileInputStream(path);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
