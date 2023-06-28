package com.movies.controller;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.movies.model.Movie;

@RestController
@RequestMapping("/api/v1")
public class MovieController {
	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public MovieController(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@GetMapping("/longest-duration-movies")
	public ResponseEntity<List<Movie>> getLongestDurationMovies() {
		String query = "SELECT tconst, primaryTitle, runtimeMinutes, genres FROM movies "
				+ "ORDER BY runtimeMinutes DESC LIMIT 10";
		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);

		List<Movie> movies = rows.stream().map(row -> {
			Movie movie = new Movie();
			movie.setTconst((String) row.get("tconst"));
			movie.setPrimaryTitle((String) row.get("primaryTitle"));
			movie.setRuntimeMinutes((int) row.get("runtimeMinutes"));
			movie.setGenres((String) row.get("genres"));
			return movie;
		}).collect(Collectors.toList());

		return ResponseEntity.ok(movies);
	}

	@PostMapping("/new-movie")
	public ResponseEntity<String> saveNewMovie(@RequestBody Movie movie) {
		String insertQuery = "INSERT INTO movies (tconst, primaryTitle, runtimeMinutes, genres) "
				+ "VALUES (?, ?, ?, ?)";
		jdbcTemplate.update(insertQuery, movie.getTconst(), movie.getPrimaryTitle(), movie.getRuntimeMinutes(),
				movie.getGenres());

		return ResponseEntity.ok("success");
	}

	@GetMapping("/top-rated-movies")
	public ResponseEntity<List<Movie>> getTopRatedMovies() {
		String query = "SELECT m.tconst, m.primaryTitle, m.genres, r.averageRating "
				+ "FROM movies m JOIN ratings r ON m.tconst = r.tconst " + "WHERE r.averageRating > 6.0 "
				+ "ORDER BY r.averageRating DESC";
		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);

		List<Movie> movies = rows.stream().map(row -> {
			Movie movie = new Movie();
			movie.setTconst((String) row.get("tconst"));
			movie.setPrimaryTitle((String) row.get("primaryTitle"));
			movie.setGenres((String) row.get("genres"));
			movie.setAverageRating((BigDecimal) row.get("averageRating"));
			return movie;
		}).collect(Collectors.toList());

		return ResponseEntity.ok(movies);
	}


	@GetMapping("/genre-movies-with-subtotals")
	public ResponseEntity<String> getGenreMoviesWithSubtotals() {
		String query = "SELECT IFNULL(m.genres, 'TOTAL') AS Genres, IFNULL(m.primaryTitle, 'TOTAL') AS primaryTitle, "
				+ "SUM(IFNULL(r.numVotes, 0)) AS numVotes " + "FROM movies m "
				+ "LEFT JOIN ratings r ON m.tconst = r.tconst " + "GROUP BY Genres, primaryTitle WITH ROLLUP";

		List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);

		StringBuilder output = new StringBuilder();
		String currentGenre = "";

		for (Map<String, Object> row : rows) {
			String genre = (String) row.get("Genres");
			String primaryTitle = (String) row.get("primaryTitle");
			long numVotes = ((Number) row.get("numVotes")).longValue();

			if (!genre.equals(currentGenre)) {
				currentGenre = genre;
				output.append(genre).append("\n");
			}

			output.append("\t").append(primaryTitle).append("\n");
			output.append("\t").append(numVotes).append("\n");
		}

		return ResponseEntity.ok(output.toString());
	}

	@PostMapping("/update-runtime-minutes")
	public ResponseEntity<String> updateRuntimeMinutes() {
		String updateQuery = "UPDATE movies " + "SET runtimeMinutes = CASE "
				+ "WHEN genres = 'Documentary' THEN runtimeMinutes + 15 "
				+ "WHEN genres = 'Animation' THEN runtimeMinutes + 30 " + "ELSE runtimeMinutes + 45 " + "END";
		jdbcTemplate.update(updateQuery);

		return ResponseEntity.ok("success");
	}

}
