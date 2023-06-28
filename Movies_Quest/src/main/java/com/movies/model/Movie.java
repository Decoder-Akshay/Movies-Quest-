package com.movies.model;

import java.math.BigDecimal;

public class Movie {

	private String tconst;
	private String primaryTitle;
	private int runtimeMinutes;
	private String genres;
	private BigDecimal averageRating;

	public String getTconst() {
		return tconst;
	}

	public void setTconst(String tconst) {
		this.tconst = tconst;
	}

	public String getPrimaryTitle() {
		return primaryTitle;
	}

	public void setPrimaryTitle(String primaryTitle) {
		this.primaryTitle = primaryTitle;
	}

	public int getRuntimeMinutes() {
		return runtimeMinutes;
	}

	public void setRuntimeMinutes(int runtimeMinutes) {
		this.runtimeMinutes = runtimeMinutes;
	}

	public String getGenres() {
		return genres;
	}

	public void setGenres(String genres) {
		this.genres = genres;
	}

	public Movie(String tconst, String primaryTitle, int runtimeMinutes, String genres, BigDecimal averageRating) {
		super();
		this.tconst = tconst;
		this.primaryTitle = primaryTitle;
		this.runtimeMinutes = runtimeMinutes;
		this.genres = genres;
		this.averageRating = averageRating;
	}

	public BigDecimal getAverageRating() {
		return averageRating;
	}

	public void setAverageRating(BigDecimal averageRating) {
		this.averageRating = averageRating;
	}

	public Movie() {
		super();
		// TODO Auto-generated constructor stub
	}

}
