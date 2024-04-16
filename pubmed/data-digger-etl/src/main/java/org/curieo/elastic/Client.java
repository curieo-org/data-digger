package org.curieo.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.io.IOException;
import javax.net.ssl.SSLContext;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

/**
 * Elastic client as documented
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/getting-started-java.html
 */
public class Client {
  // The API client
  private ElasticsearchClient esClient;

  /**
   * This instantiates a simple (non-secure) client, but note: "In self-managed installations,
   * Elasticsearch will start with security features like authentication and TLS enabled. To connect
   * to the Elasticsearch cluster you’ll need to configure the Java API Client to use HTTPS with the
   * generated CA certificate in order to make requests successfully."
   * (https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/connecting.html)
   * So, use the HTTPS client instead.
   *
   * @param serverUrl server URL -- get these from the dashboard at http://localhost:5601
   * @param apiKey api key -- get these from the dashboard at http://localhost:5601
   */
  public Client(String serverUrl, String apiKey) {
    // Create the low-level client
    RestClient restClient =
        RestClient.builder(HttpHost.create(serverUrl))
            .setDefaultHeaders(new Header[] {new BasicHeader("Authorization", "ApiKey " + apiKey)})
            .build();

    // Create the transport with a Jackson mapper
    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());

    // And create the API client
    esClient = new ElasticsearchClient(transport);
  }

  /**
   * Instantiate HTTPS client.
   *
   * @param serverUrl
   * @param serverPort
   * @param fingerprint
   * @param user
   * @param password
   */
  public Client(
      String serverUrl, int serverPort, String fingerprint, String user, String password) {

    // Connecting to Elasticsearch
    // SHA256
    // Fingerprint=18:1E:C8:E8:7C:E8:7B:2C:3E:12:8D:C6:0E:15:2B:5D:FE:D9:49:9F:A2:66:36:15:0C:A9:7B:17:90:0C:36:BD

    SSLContext sslContext = TransportUtils.sslContextFromCaFingerprint(fingerprint);

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(user, password));

    RestClient restClient =
        RestClient.builder(new HttpHost(serverUrl, serverPort, "https"))
            .setHttpClientConfigCallback(
                hc ->
                    hc.setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider))
            .build();

    // Create the transport and the API client
    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());
    esClient = new ElasticsearchClient(transport);
  }

  public ElasticsearchClient getClient() {
    return esClient;
  }

  public DeleteResponse deleteDocument(String index, String identifier)
      throws ElasticsearchException, IOException {
    DeleteRequest request = DeleteRequest.of(d -> d.index(index).id(identifier));
    return esClient.delete(request);
  }
}
