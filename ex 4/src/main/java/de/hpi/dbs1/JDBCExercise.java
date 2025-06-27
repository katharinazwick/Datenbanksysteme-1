package de.hpi.dbs1;

import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface JDBCExercise {

	/**
	 * Aufgabe a)
	 *
	 * @param config all relevant connection details (see {@link ConnectionConfig})
	 * @return an open {@link Connection} to the database
	 * @throws SQLException if the connection could not be established
	 */
	Connection createConnection(@NotNull ConnectionConfig config) throws SQLException;

	/**
	 * Aufgabe b)
	 *
	 * @param connection an open database connection
	 * @param keywords the keywords for which to search
	 * @return a list of the correct {@link Movie}
	 * @throws SQLException if the query fails
	 */
	List<Movie> queryMovies(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException;

	/**
	 * Aufgabe c)
	 *
	 * @param connection an open database connection
	 * @param keywords the keywords for which to search
	 * @return a list of the correct {@link Actor}
	 * @throws SQLException if the query fails
	 */
	List<Actor> queryActors(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException;
}
