import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

class ReadFromFileTest {
	// it seems to be just TV entries before these lines
	private static final int MOVIES_LINES_TO_SKIP = 2128910;
	private static final int RATINGS_LINES_TO_SKIP = 262652;
	private static final int GENRES_LINES_TO_SKIP = 154269;
	private static final int RELEASE_DATES_LINES_TO_SKIP = 2209820;
	private static final int RUNNING_TIMES_LINES_TO_SKIP = 275285;
	
	private static final int LINES_TO_READ = 200;
	
	private class RunningTimes {
		private LinkedList<String> titles, years, countries, runningTimes, notes;
		private RunningTimes() {
			titles = new LinkedList<String>();
			years = new LinkedList<String>();
			countries = new LinkedList<String>();
			runningTimes = new LinkedList<String>();
			notes = new LinkedList<String>();
		}
		
		private void readRunningTimesFromFile() {
			File file = new File(System.getProperty("user.dir") + "/running-times.list");
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				System.out.println("Total file size to read (in bytes) : " + fis.available());
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				// skipping the TV entries before the movie entries
				for(int i = 0; i < RUNNING_TIMES_LINES_TO_SKIP; i++) {
				  br.readLine();
				}
				String content, title, year, country, runningTime, note;
				String beforeRunningTime, runTimeToken;
				for (int i = 0; i < LINES_TO_READ; i++) {
					content = br.readLine();
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)"))) {
						note  = "[not specified]"; country  = "[not specified]";
						String [] tokens = content.split("\\s");
						System.out.println("TOKENS: " + Arrays.toString(tokens));
						
						if(tokens[tokens.length-1].contains(")")) {
							// the entry contains a note at the end
							note = content.substring(content.lastIndexOf('(') + 1, content.lastIndexOf(')'));
							// finding the running time token
							int j = 1;
							while(!tokens[tokens.length-j].contains("(")) {
								j++;
							}
							runTimeToken = tokens[tokens.length-j-1];
							System.out.println("RUN TIME TOKEN (a): " + runTimeToken);
							if(runTimeToken.contains(":")) {
								// country is also present
								runningTime = runTimeToken.substring(runTimeToken.indexOf(':') + 1);
								country = runTimeToken.substring(0, runTimeToken.indexOf(':'));
							}
							else 
								runningTime = runTimeToken;
						}
						else {
							runTimeToken = tokens[tokens.length-1];
							System.out.println("RUN TIME TOKEN (b): " + runTimeToken);
							if(runTimeToken.contains(":")) {
								// country is also present
								runningTime = runTimeToken.substring(runTimeToken.indexOf(':') + 1);
								country = runTimeToken.substring(0, runTimeToken.indexOf(':'));
							}
							else 
								runningTime = runTimeToken;
						}
						System.out.println("RUNNING TIME: " + runningTime);
						beforeRunningTime = content.substring(0, content.lastIndexOf(runTimeToken));
						System.out.println("BEFORE RUNNING TIME: " + beforeRunningTime);
						year = beforeRunningTime.substring(beforeRunningTime.lastIndexOf('(') + 1, 
								beforeRunningTime.lastIndexOf(')'));
						title = beforeRunningTime.substring(0, beforeRunningTime.lastIndexOf('('));
						
						System.out.println("TITLE: " + title + " YEAR: " + year + " COUNTRY: " + country + 
								" RUNNING TIME: " + runningTime + " NOTE: " + note);
						titles.add(title); years.add(year); countries.add(country); runningTimes.add(runningTime);
						notes.add(note);
					}
					else {
						// not counting the line
						i--;
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	private class ReleaseDates {
		private LinkedList<String> titles, years, countries, releaseDates, releaseTypes;
		
		private ReleaseDates() {
			titles = new LinkedList<String>();
			years = new LinkedList<String>();
			countries = new LinkedList<String>();
			releaseDates = new LinkedList<String>();
			releaseTypes = new LinkedList<String>();
		}
		
		private void readReleaseDatesFromFile() {
			File file = new File(System.getProperty("user.dir") + "/release-dates.list");
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				System.out.println("Total file size to read (in bytes) : " + fis.available());
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				// skipping the TV entries before the movie entries
				for(int i = 0; i < RELEASE_DATES_LINES_TO_SKIP; i++) {
				  br.readLine();
				}

				String content, title, year, country, releaseDate, releaseType;
				int bracketIndex;
				String firstHalf, secondHalf; // splitting the content on the index of the final colon
				for (int i = 0; i < LINES_TO_READ; i++) {
					content = br.readLine();
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)"))) {
						firstHalf = content.substring(0, content.lastIndexOf(':'));
						secondHalf = content.substring(content.lastIndexOf(':'));
						
						country = firstHalf.substring(firstHalf.lastIndexOf(')') + 1).trim();
						try {
							year = firstHalf.substring(firstHalf.lastIndexOf('(') + 1, firstHalf.lastIndexOf(')'));
						} 
						catch (StringIndexOutOfBoundsException e) {
							System.out.println("***EXCEPTION CONTENT***: " + content);
							e.printStackTrace();
							String [] tokens = content.split("\\s");
							// handling the exception for the #Y entry
							// *** may need to add handling for other entries ***
							year = "";
							if(tokens[0].equals("#Y")) {
								firstHalf = content.substring(0, content.indexOf(':'));
								secondHalf = content.substring(content.indexOf(':'));
								country = firstHalf.substring(firstHalf.lastIndexOf(')') + 1).trim();
								year = firstHalf.substring(firstHalf.lastIndexOf('(') + 1, firstHalf.lastIndexOf(')'));
							}
						}
						title = firstHalf.substring(0, firstHalf.lastIndexOf('('));
						
						bracketIndex = secondHalf.indexOf('(');
						if(bracketIndex != -1) {
							releaseDate = secondHalf.substring(1, secondHalf.indexOf('('));
							releaseType = secondHalf.substring(secondHalf.lastIndexOf('(') + 1, secondHalf.lastIndexOf(')'));
						}
						else {
							// for some entries there is no release type after the date
							releaseDate = secondHalf.substring(1);
							releaseType = "[not specified]";
						}
						
						System.out.println("CONTENT: " + content);
						System.out.println("TITLE: " + title + " YEAR: " + year + " COUNTRY: " + country + 
								" RELEASE DATE: " + releaseDate + " RELEASE TYPE: " + releaseType);
						titles.add(title); years.add(year); countries.add(country); releaseDates.add(releaseDate);
						releaseTypes.add(releaseType);
					}
					else {
						// not counting the line
						i--;
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	private class Genres {
		private List<Set<String>> listOfGenreSets;
		private LinkedList<String> yearsList, titlesList;
		
		private Genres() {
			listOfGenreSets = new ArrayList<Set<String>>();
			yearsList = new LinkedList<String>();
			titlesList = new LinkedList<String>();
		}
		
		private void readGenresFromFile() {
			File file = new File(System.getProperty("user.dir") + "/genres.list");
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				System.out.println("Total file size to read (in bytes) : " + fis.available());
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				// skipping the TV entries before the movie entries
				for(int i = 0; i < GENRES_LINES_TO_SKIP; i++) {
				  br.readLine();
				}
				
				String content;
				int skippedLines = 0;
				
				// reading first entry
				do {
					content = br.readLine();
					skippedLines++;
					// if the first entry contains “(TV)” or “(V)” or "(VG)", read another line
				} while ((content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)")));
				Set <String> firstGenresSet = new HashSet<String>();
				String [] firstTokens = content.split("\\s+");
				String firstGenre = firstTokens[firstTokens.length-1]; // the last token is the genre
				String firstYearIncludingVersion = content.substring((content.lastIndexOf('(') + 1), 
						(content.lastIndexOf(')')));
				String firstTitle = content.substring(0, content.lastIndexOf('('));
				
				firstGenresSet.add(firstGenre);
				
				/****** POSSIBLY NOT NEEDED **********/
				yearsList.add(firstYearIncludingVersion);
				titlesList.add(firstTitle);
				/*************************************/
				
				System.out.println("FIRST GENRE: " + firstGenre + " FIRST YEAR INCL. VERSION: " + 
						firstYearIncludingVersion + " FIRST TITLE: " + firstTitle);
				
				for (int i = 1; i <= LINES_TO_READ - skippedLines; i++) {
					content = br.readLine();
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)"))) {
						Set <String> genresSet = new HashSet<String>();
						String [] tokens = content.split("\\s+");
						String genre = tokens[tokens.length-1]; // the last token is the genre
						String yearIncludingVersion = content.substring((content.lastIndexOf('(') + 1), 
								(content.lastIndexOf(')')));
						String title = content.substring(0, content.lastIndexOf('('));
						
						/* if the title + the year of release + any Roman numeral version number after it
						 * all match those of the previous entry, then add the genre to the previous entry's set
						 */
						if((title + yearIncludingVersion).equals(firstTitle + firstYearIncludingVersion)) {
							firstGenresSet.add(genre);
						}
						else {
							System.out.println("GENRES SET: " + firstGenresSet + " YEAR INCL. VERSION: " + 
									firstYearIncludingVersion + " TITLE: " + firstTitle);
							
							// save the previous entry's data
							listOfGenreSets.add(firstGenresSet);
							yearsList.add(firstYearIncludingVersion);
							titlesList.add(firstTitle);
							
							// change the previous entry's data with the current entry's
							firstGenre = genre; firstYearIncludingVersion = yearIncludingVersion; firstTitle = title;	
							firstGenresSet = genresSet;
							
							firstGenresSet.add(firstGenre);
						}
					}
					else {
						// not counting the line
						System.out.println("i--");
						i--;
					}
					
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	private class Ratings {
		private LinkedList<String> distributions, votesList, ratings, titles, years;
		
		private Ratings() {
			distributions = new LinkedList<String>();
			votesList = new LinkedList<String>();
			ratings = new LinkedList<String>();
			titles = new LinkedList<String>();
			years = new LinkedList<String>();
		}
		
		private void readRatingsFromFile() {
			File file = new File(System.getProperty("user.dir") + "/ratings.list");
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				System.out.println("Total file size to read (in bytes) : " + fis.available());
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				// skipping the TV entries before the movie entries
				for(int i = 0; i < RATINGS_LINES_TO_SKIP; i++) {
				  br.readLine();
				}
				
				String content, distribution = "", votes = "", rating = "", title = "", year = "";
				for (int i = 0; i < LINES_TO_READ; i++) {
					content = br.readLine();
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)"))) {
						System.out.println("CONTENT: " + content);
						String [] tokens = content.split("\\s+");
						distribution = tokens[1];
						votes = tokens[2];
						rating = tokens[3];
						title = content.substring( (content.indexOf( (String) tokens[4]) ) , (content.lastIndexOf('(')));
						year = content.substring((content.lastIndexOf('(') + 1), (content.lastIndexOf(')')));
						System.out.println("DISTRIBUTION: " + distribution + " VOTES: " + votes + " RATING: " + rating + 
								" TITLE: " + title + " YEAR: " + year + "\n");
						distributions.add(distribution); votesList.add(votes); ratings.add(rating); titles.add(title);
						years.add(year);
					}
					else {
						// not counting the line
						i--;
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
			
		}
	}
	
	private class Movies {
		private LinkedList<String> movie_titles, years;
		private Movies() {
			movie_titles = new LinkedList<String>();
			years = new LinkedList<String>();
		}
		
		private void readMoviesFromFile() {
			File file = new File(System.getProperty("user.dir") + "/movies.list");
			FileInputStream fis = null;
			try {
				movie_titles = new LinkedList<String>();
				years = new LinkedList<String>();
				fis = new FileInputStream(file);
	 
				System.out.println("Total file size to read (in bytes) : " + fis.available());

				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				
				// skipping everything before the actual movie titles
				for(int i = 0; i < MOVIES_LINES_TO_SKIP; i++) {
				  br.readLine();
				}
				
				String content, title, year;
				int lastOpenBracketIndex;
				for (int i = 0; i < LINES_TO_READ; i++) {
					content = br.readLine();
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)")|| content.contains("(VG)"))) {
						
						// skipping any occurrences of parentheses in the movie title
						// assumes that all years are bookended by parentheses
						lastOpenBracketIndex = content.lastIndexOf('(');
						
						// taking the title to be everything before the last open bracket
						title = content.substring(0, lastOpenBracketIndex);
						// taking the year to be everything between the last two brackets
						year = content.substring(lastOpenBracketIndex + 1, content.lastIndexOf((')')));
						System.out.print("CONTENT: " + content + " TITLE: " + title + "YEAR: " + year + "\n");
						movie_titles.add(title);
						years.add(year);
					}
					else {
						// not counting the line
						i--;
					}
				}
				br.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	private class Database {
		private Connection conn;
		private final int BATCH_SIZE = 40;
		
		private Database(Connection conn) {
			this.conn = conn;
		}
		
		private void insertIntoDB(Ratings r) {
			
		}
		
		private void insertIntoDB(RunningTimes rt) {
			
		}
		
		private void insertIntoDB(ReleaseDates rd) {
			
		}
		
		private void insertIntoDB(Genres g) {
			try {
				// inserting titles & years first, then adding the sets of genres				
		      	String query = "insert into genres (title, year) values (?, ?)";
		      	// multiple batches are required as otherwise it's stopping after 80 entries
		      	System.out.println("LINES_TO_READ: " + LINES_TO_READ + " BATCH_SIZE: " + 
		      			BATCH_SIZE + " LINES_TO_READ/BATCH_SIZE: " + LINES_TO_READ/BATCH_SIZE);
		      	for(int j = 0; j < 	LINES_TO_READ/BATCH_SIZE; j++) {
			      	System.out.println("TITLES (" + g.titlesList.size() + 
			      			"): " + g.titlesList.toString());
		      		PreparedStatement preparedStmt = conn.prepareStatement(query);
		      		//**** need to change this!: titlesList decreasing as i is increasing ****
			      	for(int i = 0; i < g.titlesList.size(); i++) {
				        preparedStmt.setString (1, g.titlesList.remove());
				        preparedStmt.setString (2, g.yearsList.remove());
				      	preparedStmt.addBatch();
			      	}
			        preparedStmt.executeBatch();
			        preparedStmt.close();
		      	}
		      	
		      	// handling the remainder in cases where LINES_TO_READ isn't a multiple of BATCH_SIZE
		      	int remainder = LINES_TO_READ % 50;
		      	System.out.println("REMAINDER: " + remainder);
		      	if(remainder != 0) {
		      		PreparedStatement preparedStmt = conn.prepareStatement(query);
			      	for(int i = 0; i < remainder; i++) {
				        preparedStmt.setString (1, g.titlesList.remove());
				        preparedStmt.setString (2, g.yearsList.remove());
				      	preparedStmt.addBatch();
			      	}
			        preparedStmt.executeBatch();
			        preparedStmt.close();
		      	}

			        System.out.println("G.LISTOFGENRESETS SIZE: " + g.listOfGenreSets.size());
		      	Iterator<Set<String>> it = g.listOfGenreSets.iterator();
		      	Set<String> nextSet = new HashSet<String>();
		      	// adding genres
		      	int rowId = 2;
		      	String updateQuery, setText;
		      	while(it.hasNext()) {
		      		nextSet = it.next();
		      		setText = nextSet.toString();
		      		setText = setText.replaceAll("\\s+","");
			      	updateQuery = "UPDATE genres SET genre = \'" + setText.substring(1, setText.length()-1) + 
			      			"\' WHERE id = " + rowId;
			      	System.out.println(updateQuery);
			      	PreparedStatement preparedStmt = conn.prepareStatement(updateQuery);
			      	preparedStmt.execute();
			      	preparedStmt.close();
			      	rowId++;
		      	}
				
			}
		    catch (Exception e)
		    {
		      e.printStackTrace();
		    }
		}
		
		private void insertIntoDB(Movies m) {
			try
	    	{		
				System.out.println("INSERT INTO DB M.MOVIE_TITLES: " + m.movie_titles.toString());
				System.out.println("INSERT INTO DB M.YEARS: " + m.years.toString());
		      	String query = "insert into movies (movieTitle, year) values (?, ?)";
		      	 
		      	// multiple batches are required as otherwise it's stopping after 80 entries
		      	for(int j = 0; j < 	LINES_TO_READ/BATCH_SIZE; j++) {
			      	PreparedStatement preparedStmt = conn.prepareStatement(query);
			      	for(int i = 0; i < BATCH_SIZE; i++) {
				        preparedStmt.setString (1, m.movie_titles.remove());
				        preparedStmt.setString (2, m.years.remove());
				      	preparedStmt.addBatch();
			      	}
			        preparedStmt.executeBatch();
			        preparedStmt.close();
		      	}
		      	
		      	// handling the remainder in cases where LINES_TO_READ isn't a multiple of BATCH_SIZE
		      	int remainder = LINES_TO_READ % 50;
		      	if(remainder != 0) {
			      	PreparedStatement preparedStmt = conn.prepareStatement(query);
			      	for(int i = 0; i < remainder; i++) {
				        preparedStmt.setString (1, m.movie_titles.remove());
				        preparedStmt.setString (2, m.years.remove());
				      	preparedStmt.addBatch();
			      	}
			        preparedStmt.executeBatch();
			        preparedStmt.close();
		      	}
		      	
		      		
		    }
		    catch (Exception e)
		    {
		      e.printStackTrace();
		    }
		}
		
		private void queryDB(String dataRequested) {
			try {
				if(dataRequested.equals("movies")) {
			        String query2 = "SELECT * FROM movies";
			        // create the java statement
			        Statement st = conn.createStatement();
			         
			        // execute the query, and get a java resultset
			        ResultSet rs = st.executeQuery(query2);
			         
			        // iterate through the java resultset
			        System.out.println("RESULTS");
			        String id, movieTitle, year;
			        while (rs.next())
			        {
			          id = rs.getString("id");
			          movieTitle = rs.getString("movieTitle");
			          year = rs.getString("year");
			           
			          // print the results
			          System.out.format("%s, %s, %s\n", id, movieTitle, year);
			        }
			        st.close();
			      	conn.close();
				}				
				else if(dataRequested.equals("genres")) {
			        String query2 = "SELECT * FROM genres";
			        // create the java statement
			        Statement st = conn.createStatement();
			         
			        // execute the query, and get a java resultset
			        ResultSet rs = st.executeQuery(query2);
			         
			        // iterate through the java resultset
			        System.out.println("RESULTS");
			        String id, title, year, genres;
			        while (rs.next())
			        {
			          id = rs.getString("id");
			          title = rs.getString("title");
			          year = rs.getString("year");
			          genres = rs.getString("genre");
			           
			          // print the results
			          System.out.format("%s, %s, %s, %s\n", id, title, year, genres);
			        }
			        st.close();
			      	conn.close();
				}
		    }
		    catch (Exception e)
		    {
		      e.printStackTrace();
		    }
		}		
	}
	
	
	public void readFrom(String password) {
		//RunningTimes rt = new RunningTimes();
		//rt.readRunningTimesFromFile();
		
		//Movies m = new Movies();
		//m.readMoviesFromFile();
		
		Genres g = new Genres();
		g.readGenresFromFile();
		
		try {
			String myDriver = "org.gjt.mm.mysql.Driver";
	      	String myUrl = "jdbc:mysql://localhost/ca4002?" + 
	      					"&rewriteBatchedStatements=true";
	      	Class.forName(myDriver);
	      	Connection conn = DriverManager.getConnection(myUrl, "root", password);
	      	System.out.println("connected");
			Database db = new Database(conn);
			
			db.insertIntoDB(g);
			db.queryDB("genres");
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	}
	
	public static void main(String [] args) {
		String dbPassword = "Sh4k3sp34r3";
		ReadFromFileTest r = new ReadFromFileTest();
		r.readFrom(dbPassword);
	}
}
