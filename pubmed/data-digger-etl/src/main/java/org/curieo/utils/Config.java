package org.curieo.utils;

public record Config() {
  public static final String CONFIG_FOLDER = Config.getEnv("CONFIG_FOLDER", "../../config/");
  public static final String CREDENTIALS_PATH =
      Config.getEnv("CREDENTIALS_PATH", CONFIG_FOLDER + "credentials.json");
  public static final String CORPORA_FOLDER = Config.getEnv("CORPORA_FOLDER", "../corpora/");

  static String getEnv(String key, String defaultValue) {
    String value = System.getenv(key);
    return value != null ? value : defaultValue;
  }
}
