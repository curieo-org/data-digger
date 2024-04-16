package org.curieo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class CredentialsTest {
  @Test
  void testReadAndWrite() throws IOException {
    File defaultLocation = new File(Config.CREDENTIALS_PATH);
    Credentials creds = Credentials.read(defaultLocation);
    creds.add("test", "key", "value");
    creds.write(defaultLocation);
    assertEquals("value", Credentials.read(defaultLocation).get("test", "key"));
  }
}
