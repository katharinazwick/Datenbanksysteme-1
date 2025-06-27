import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

@ChosenImplementation(false)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

    Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Override
    public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
        String host = config.getHost();
        int port = config.getPort();
        String database = config.getDatabase();
        String user = config.getUsername();
        String password = config.getPassword();

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        logger.info("Connecting to database at " + url);
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public List<Movie> queryMovies(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info("Suche Filme mit Keyword: " + keywords);
        List<Movie> movies = new ArrayList<>();

        // 1. alle Filme mit Titel, der das Keyword enthält finden
        String movieQuery = """
                    SELECT tb.tconst, tb.originalTitle, tb.startYear, tb.genres
                    FROM title_basics tb
                    WHERE LOWER(tb.originalTitle) LIKE ?
                      AND tb.originalTitle IS NOT NULL
                    ORDER BY tb.originalTitle ASC, tb.startYear ASC
                """;

        try (var stmt = connection.prepareStatement(movieQuery)) {
            stmt.setString(1, "%" + keywords.toLowerCase() + "%");

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tconst = rs.getString("tconst");
                    String title = rs.getString("originalTitle");
                    Integer year = rs.getObject("startYear") != null ? rs.getInt("startYear") : null;

                    String genresString = rs.getString("genres");
                    Set<String> genres = new HashSet<>();
                    if (genresString != null && !genresString.equals("\\N")) {
                        genres = Set.of(genresString.split(","));
                    }

                    Movie movie = new Movie(tconst, title, year, genres);

                    // 2. sortierte Liste von Schauspielern hinzufügen
                    String actorsQuery = """
                                SELECT nb.primaryName
                                FROM tprincipals tp
                                JOIN name_basics nb ON tp.person_id = nb.nconst
                                WHERE tp.movie_id = ?
                                  AND (tp.category = 'actor' OR tp.category = 'actress')
                                ORDER BY nb.primaryName ASC
                            """;

                    try (var actorStmt = connection.prepareStatement(actorsQuery)) {
                        actorStmt.setString(1, tconst);
                        try (var actorRs = actorStmt.executeQuery()) {
                            while (actorRs.next()) {
                                String actorName = actorRs.getString("primaryName");
                                movie.actorNames.add(actorName);
                            }
                        }
                    }

                    movies.add(movie);
                }
            }
        }

        return movies;
    }


    @Override
    public List<Actor> queryActors(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info("Schauspieler-Suche mit Keyword: " + keywords);
        List<Actor> actors = new ArrayList<>();

        // 1. Top 5 Schauspieler*innen mit Keyword und Filmanzahl
        String topActorsSQL = """
                    SELECT nb.nconst, nb.primaryName, COUNT(*) AS movie_count
                    FROM name_basics nb
                    JOIN tprincipals tp ON nb.nconst = tp.person_id
                    WHERE (tp.category = 'actor' OR tp.category = 'actress')
                      AND LOWER(nb.primaryName) LIKE ?
                    GROUP BY nb.nconst, nb.primaryName
                    ORDER BY movie_count DESC, nb.primaryName ASC
                    LIMIT 5
                """;

        try (var stmt = connection.prepareStatement(topActorsSQL)) {
            stmt.setString(1, "%" + keywords.toLowerCase() + "%");

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String nConst = rs.getString("nconst");
                    String name = rs.getString("primaryName");

                    Actor actor = new Actor(nConst, name);

                    // 2. 5 neueste Filme abrufen
                    String moviesSQL = """
                                SELECT tb.originalTitle, tb.startYear
                                FROM tprincipals tp
                                JOIN title_basics tb ON tp.movie_id = tb.tconst
                                WHERE tp.person_id = ?
                                  AND (tp.category = 'actor' OR tp.category = 'actress')
                                  AND tb.startYear IS NOT NULL
                                ORDER BY tb.startYear DESC, tb.originalTitle ASC
                                LIMIT 5
                            """;

                    try (var movieStmt = connection.prepareStatement(moviesSQL)) {
                        movieStmt.setString(1, nConst);
                        try (var movieRs = movieStmt.executeQuery()) {
                            while (movieRs.next()) {
                                String title = movieRs.getString("originalTitle");
                                actor.playedIn.add(title);
                            }
                        }
                    }

                    // 3. Top 5 Co-Stars ermitteln
                    String costarsSQL = """
                                SELECT nb2.primaryName, COUNT(*) AS shared_count
                                FROM tprincipals tp1
                                JOIN tprincipals tp2 ON tp1.movie_id = tp2.movie_id
                                JOIN name_basics nb2 ON tp2.person_id = nb2.nconst
                                WHERE tp1.person_id = ?
                                  AND tp2.person_id != tp1.person_id
                                  AND (tp2.category = 'actor' OR tp2.category = 'actress')
                                GROUP BY nb2.primaryName
                                ORDER BY shared_count DESC, nb2.primaryName ASC
                                LIMIT 5
                            """;

                    try (var costarStmt = connection.prepareStatement(costarsSQL)) {
                        costarStmt.setString(1, nConst);
                        try (var costarRs = costarStmt.executeQuery()) {
                            while (costarRs.next()) {
                                String coStarName = costarRs.getString("primaryName");
                                int sharedCount = costarRs.getInt("shared_count");
                                actor.costarNameToCount.put(coStarName, sharedCount);
                            }
                        }
                    }

                    actors.add(actor);
                }
            }
        }
        return actors;
    }
}
