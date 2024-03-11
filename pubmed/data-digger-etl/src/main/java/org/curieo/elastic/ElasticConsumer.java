package org.curieo.elastic;

import java.io.IOException;
import java.io.StringReader;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.JsonData;
import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Value;

@Generated @Value @AllArgsConstructor
public class ElasticConsumer<T> implements Function<T, Result>{
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	ElasticsearchClient client;
	String index;
	Function<T, String> identifier;
	
	@Override
	public Result apply(T t) {
		
		try {
			String json = OBJECT_MAPPER.writeValueAsString(t);

			StringReader input = new StringReader(json);
			
			IndexRequest<JsonData> request = IndexRequest.of(i -> i
			    .index(index)
			    .id(identifier.apply(t))
			    .withJson(input)
			);
			IndexResponse response = client.index(request);
			return response.result();
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}
}
