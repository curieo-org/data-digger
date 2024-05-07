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
  public String pmc_comm_folder_path;
  public String pmc_noncomm_folder_path;
  public String pmc_other_folder_path;

  public String postgres_database;
  public String postgres_user;
  public String postgres_password;

  public String aws_storage_bucket;
  public String aws_region;
  public int thread_pool_size;

  private String environment = System.getenv("ENVIRONMENT");
  private Dotenv dotenv;

  public Config() {
    if (environment == null || !environment.equals("production")) {
      dotenv = Dotenv.load();
    }

    pubmed_ftp_server = getEnv("PUBMED_FTP_SERVER", false, "ftp.ncbi.nlm.nih.gov");
    pubmed_ftp_user = getEnv("PUBMED_FTP_USER", false, "anonymous");
    pubmed_ftp_password = getEnv("PUBMED_FTP_PASSWORD", false, "anonymous");

    baseline_remote_path = getEnv("BASELINE_REMOTE_PATH", false, "/pubmed/baseline/");
    updates_remote_path = getEnv("UPDATES_REMOTE_PATH", false, "/pubmed/updatefiles/");
    commons_remote_path = getEnv("COMMONS_REMOTE_PATH", false, "/pubmed/pubmedcommons/");
    corpora_folder_path = getEnv("CORPORA_FOLDER_PATH", false, "../corpora/");
    pmc_comm_folder_path = getEnv("PMC_COMM_FOLDER_PATH", false, "/pub/pmc/oa_bulk/oa_comm/xml/");
    pmc_noncomm_folder_path =
        getEnv("PMC_NONCOMM_FOLDER_PATH", false, "/pub/pmc/oa_bulk/oa_noncomm/xml/");
    pmc_other_folder_path =
        getEnv("PMC_OTHER_FOLDER_PATH", false, "/pub/pmc/oa_bulk/oa_other/xml/");

    thread_pool_size = Integer.parseInt(getEnv("THREAD_POOL_SIZE", false, "10"));

    postgres_database = getEnv("POSTGRES_DATABASE", true, null);
    postgres_user = getEnv("POSTGRES_USER", true, null);
    postgres_password = getEnv("POSTGRES_PASSWORD", true, null);

    aws_storage_bucket = getEnv("AWS_STORAGE_BUCKET", true, null);
    aws_region = getEnv("AWS_REGION", true, null);
  }

  public String getEnv(String key, boolean required, String defaultValue) {
    String value;

    if (environment == null || !environment.equals("production")) {
      value = dotenv.get(key);
    } else {
      value = System.getenv(key);
    }

    if (value == null && required) {
      System.err.println("Environment variable %s is not set: " + key);
      System.exit(1);
    }

    return value != null ? value : defaultValue;
  }
}
