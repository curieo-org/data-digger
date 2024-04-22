package org.curieo.utils;

import io.github.cdimascio.dotenv.Dotenv;

public class Config {
  public String pubmed_ftp_server;
  public String pubmed_ftp_user;
  public String pubmed_ftp_password;

  public String baseline_remote_path;
  public String updates_remote_path;
  public String commons_remote_path;
  public String corpora_folder_path;

  public String postgres_database;
  public String postgres_user;
  public String postgres_password;

  public String openai_organization;
  public String openai_key;

  public String elastic_server_url;
  public String elastic_server_port;
  public String elastic_server_fingerprint;
  public String elastic_server_user;
  public String elastic_server_password;
  public String elastic_apikey;
  private Dotenv dotenv;

  public Config() {
    dotenv = Dotenv.load();

    pubmed_ftp_server = getEnv("PUBMED_FTP_SERVER", false, "ftp.ncbi.nlm.nih.gov");
    pubmed_ftp_user = getEnv("PUBMED_FTP_USER", false, "anonymous");
    pubmed_ftp_password = getEnv("PUBMED_FTP_PASSWORD", false, "anonymous");

    baseline_remote_path = getEnv("BASELINE_REMOTE_PATH", false, "/pubmed/baseline/");
    updates_remote_path = getEnv("UPDATES_REMOTE_PATH", false, "/pubmed/updatefiles/");
    commons_remote_path = getEnv("COMMONS_REMOTE_PATH", false, "/pubmed/pubmedcommons/");
    corpora_folder_path = getEnv("CORPORA_FOLDER_PATH", false, "../corpora/");

    postgres_database = getEnv("POSTGRES_DATABASE", true, null);
    postgres_user = getEnv("POSTGRES_USER", true, null);
    postgres_password = getEnv("POSTGRES_PASSWORD", true, null);

    openai_organization = getEnv("OPENAI_ORGANIZATION", false, "");
    openai_key = getEnv("OPENAI_KEY", false, "");

    elastic_server_url = getEnv("ELASTIC_SERVER_URL", false, "127.0.0.1");
    elastic_server_port = getEnv("ELASTIC_SERVER_PORT", false, "9200");
    elastic_server_fingerprint = getEnv("ELASTIC_SERVER_FINGERPRINT", false, "anonymous");
    elastic_server_user = getEnv("ELASTIC_SERVER_USER", false, "anonymous");
    elastic_server_password = getEnv("ELASTIC_SERVER_PASSWORD", false, "anonymous");
    elastic_apikey = getEnv("ELASTIC_APIKEY", false, "anonymous");
  }

  private String getEnv(String key, boolean required, String defaultValue) {
    String value = this.dotenv.get(key);

    if (value == null && required) {
      System.err.println("Environment variable %s is not set" + key);
      System.exit(1);
    }

    return value != null ? value : defaultValue;
  }
}
