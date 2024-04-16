package org.curieo.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import lombok.Data;
import lombok.Generated;

@Generated
@Data
public class Credentials {
  private static final ObjectReader OBJECT_READER;
  private static final ObjectWriter OBJECT_WRITER;
  HashMap<String, HashMap<String, String>> data;

  public Credentials() {}

  static {
    TypeReference<HashMap<String, HashMap<String, String>>> typeRef =
        new TypeReference<HashMap<String, HashMap<String, String>>>() {};
    OBJECT_READER = new ObjectMapper().readerFor(typeRef);
    OBJECT_WRITER = new ObjectMapper().writerFor(typeRef).withDefaultPrettyPrinter();
  }

  public void add(String application, String key, String value) {
    if (data == null) {
      data = new HashMap<>();
    }
    data.computeIfAbsent(application, a -> new HashMap<>()).put(key, value);
  }

  public String get(String application, String key) {
    return data.getOrDefault(application, new HashMap<>()).get(key);
  }

  public String need(String application, String key) {
    HashMap<String, String> config = data.get(application);
    if (config == null) {
      throw new IllegalArgumentException(
          String.format("entry %s not found in config", application));
    }

    String retval = config.get(key);
    if (retval == null) {
      throw new IllegalArgumentException(
          String.format("Key %s not found in application %s in config", key, application));
    }
    return retval;
  }

  public boolean hasApplication(String application) {
    return data.containsKey(application);
  }

  /**
   * If the path does not exist, this fails silently!
   *
   * @param path
   * @return instantiated creds
   * @throws JsonProcessingException
   * @throws IOException
   */
  public static Credentials read(File path) throws JsonProcessingException, IOException {
    Credentials creds = new Credentials();
    if (!path.exists()) {
      creds.data = new HashMap<>();
      return creds;
    }
    creds.setData(OBJECT_READER.readValue(path));
    return creds;
  }

  public void write(File path) throws JsonProcessingException, IOException {
    OBJECT_WRITER.writeValue(path, data);
  }
}
