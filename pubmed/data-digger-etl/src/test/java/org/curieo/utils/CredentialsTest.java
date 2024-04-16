package org.curieo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class CredentialsTest {
  @Test
  void testReadAndWrite() throws JsonProcessingException, IOException {
    File defaultLocation = new File(System.getenv("HOME") + "/.credentials.json");
    Credentials creds = Credentials.read(defaultLocation);
    creds.add("test", "key", "value");
    creds.write(defaultLocation);
    assertEquals("value", Credentials.read(defaultLocation).get("test", "key"));
  }
}
