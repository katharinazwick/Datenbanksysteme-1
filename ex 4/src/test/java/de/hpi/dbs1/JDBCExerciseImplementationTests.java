package de.hpi.dbs1;

import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.junit.jupiter.api.*;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import static de.hpi.dbs1.fixtures.Actors.*;
import static de.hpi.dbs1.fixtures.Movies.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JDBCExerciseImplementationTests {

	static JDBCExercise implementation;
	static ConnectionConfig config;

	@BeforeAll
	static void setUp() {
		implementation = ChosenImplementationKt.getChosenImplementation();
		config = new PropertiesConnectionConfig(new File("database.properties"));
		Assertions.assertFalse(config.getUsername().isBlank(), "please provide your database username in the \"database.properties\" file");
		Assertions.assertFalse(config.getPassword().isBlank(), "please provide your database password in the \"database.properties\" file");
	}

	@Test
	@Order(1)
	@DisplayName("connectDatabase(config) should return a valid connection to the Postgres server")
	void testConnectDatabase() throws SQLException {
		try(var connection = implementation.createConnection(config)) {
			Assertions.assertTrue(connection.isValid(0), "connection is not valid");
		}
	}

	@Test
	@Order(2)
	@DisplayName("Movie query 1: Wolf of Wall Street")
	void testQueryMovies1() throws SQLException {
		try(var connection = implementation.createConnection(config)) {
			List<Movie> results = implementation.queryMovies(connection, "Wolf of Wall Street");
			Assertions.assertIterableEquals(List.of(WWS_1929, WWS_2013), results);
		}
	}

	@Test
	@Order(2)
	@DisplayName("Movie query 2: Ghost in the Shell")
	void testQueryMovies2() throws SQLException {
		try(var connection = implementation.createConnection(config)) {
			List<Movie> results = implementation.queryMovies(connection, "Ghost in the Shell");

			Assertions.assertEquals(GITS_1995, results.get(0), "The first result should be \"Ghost in the Shell\" from 1995");
			Assertions.assertEquals(GITS_1995.actorNames, results.get(0).actorNames, "\"Ghost in the Shell (1995)\" did not contain the correct actors");
			Assertions.assertTrue(results.contains(GITS_TNM_2015), "The result should contain \"Ghost in the Shell: The New Movie\" from 2015");
		}
	}

	@Test
	@Order(3)
	@DisplayName("Actor/Actress query 1: Anne Hathaway")
	void testQueryActors1() throws SQLException {
		try(var connection = implementation.createConnection(config)) {
			List<Actor> results = implementation.queryActors(connection, "Anne Hathaway");
			Assertions.assertEquals(1, results.size());

			Assertions.assertEquals(ANNE_HATHAWAY, results.get(0));
			Assertions.assertEquals(ANNE_HATHAWAY.playedIn, results.get(0).playedIn);
			Assertions.assertEquals(ANNE_HATHAWAY.costarNameToCount, results.get(0).costarNameToCount);
		}
	}

	@Test
	@Order(3)
	@DisplayName("Actor/Actress query 2: Freeman")
	void testQueryActors2() throws SQLException {
		try(var connection = implementation.createConnection(config)) {
			List<Actor> results = implementation.queryActors(connection, "Freeman");
			Assertions.assertEquals(List.of(MORGAN_FREEMAN, FREEMAN_WOOD, KATHLEEN_FREEMAN, HOWARD_FREEMAN, MONA_FREEMAN), results);

			Assertions.assertEquals(MORGAN_FREEMAN, results.get(0));
			Assertions.assertEquals(MORGAN_FREEMAN.playedIn, results.get(0).playedIn);
			Assertions.assertEquals(MORGAN_FREEMAN.costarNameToCount, results.get(0).costarNameToCount);
		}
	}
}
