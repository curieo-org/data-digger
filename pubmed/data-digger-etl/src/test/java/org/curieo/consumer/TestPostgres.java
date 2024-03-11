package org.curieo.consumer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.curieo.utils.Credentials;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import static org.curieo.consumer.PostgreSQLClient.CreateFlags.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestPostgres {
	
	@Test @Tag("slow")
	void testCreateDb() throws JsonProcessingException, IOException, SQLException {
		File defaultLocation = new File(System.getenv("HOME") + "/.credentials.json");
		Credentials creds = Credentials.read(defaultLocation);
		
		String user = creds.get("postgres-datadigger", "user");
		String password = creds.get("postgres-datadigger", "password");
		try (PostgreSQLClient client = new PostgreSQLClient(null, user, password)) {
			client.createDatabase("test", OnExistSilentNoOp);
			assertThrows(RuntimeException.class, 
					() -> client.createDatabase("test", OnExistFail));
			client.dropDatabase("test");
			client.createDatabase("test", OnExistSilentOverride);
		}
	}
}
