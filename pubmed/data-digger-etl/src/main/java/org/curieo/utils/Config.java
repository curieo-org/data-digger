package org.curieo.utils;

public record Config() {
  public static final String CONFIG_FOLDER = "../../config/";
  public static final String CREDENTIALS_PATH = CONFIG_FOLDER + "credentials.json";
  public static final String CORPORA_FOLDER = "../corpora/";
}
