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
		private int numOfExceptions;
		private RunningTimes() {
			titles = new LinkedList<String>();
			years = new LinkedList<String>();
			countries = new LinkedList<String>();
			runningTimes = new LinkedList<String>();
			notes = new LinkedList<String>();
			numOfExceptions = 0;
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
				while ((content = br.readLine()) != null)
					//(int i = 0; i < LINES_TO_READ; i++) 
					{
					
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
						try {
							year = beforeRunningTime.substring(beforeRunningTime.lastIndexOf('(') + 1, 
									beforeRunningTime.lastIndexOf(')'));
							title = beforeRunningTime.substring(0, beforeRunningTime.lastIndexOf('('));
							
							System.out.println("TITLE: " + title + " YEAR: " + year + " COUNTRY: " + country + 
									" RUNNING TIME: " + runningTime + " NOTE: " + note);
							titles.add(title); years.add(year); countries.add(country); runningTimes.add(runningTime);
							notes.add(note);
						} catch(StringIndexOutOfBoundsException e) {
							numOfExceptions++;
							e.printStackTrace();
						}
					}
					/*
					else {
						// not counting the line
						i--;
					}
					*/
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
				while ((content = br.readLine()) != null)
					//(int i = 0; i < LINES_TO_READ; i++) 
					{
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
							else if(tokens[0].equals("$Money$")) {
								firstHalf = content.substring(0, content.indexOf(':'));
								secondHalf = content.substring(content.indexOf(':'));
								country = firstHalf.substring(firstHalf.lastIndexOf(')') + 1).trim();
								year = firstHalf.substring(firstHalf.lastIndexOf('(') + 1, firstHalf.lastIndexOf(')'));
								
							}
						}
						title = firstHalf.substring(0, firstHalf.lastIndexOf('('));

						// this try-catch block is for line 197 (five lines down)
						try {
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
							
							//System.out.println("CONTENT: " + content);
							System.out.println("TITLE: " + title + " YEAR: " + year + " COUNTRY: " + country + 
									" RELEASE DATE: " + releaseDate + " RELEASE TYPE: " + releaseType);
							titles.add(title); years.add(year); countries.add(country); releaseDates.add(releaseDate);
							releaseTypes.add(releaseType);							
						} catch (StringIndexOutOfBoundsException e) {
							e.printStackTrace();
						}
					}
					/*
					else {
						// not counting the line
						i--;
					}
					*/
				}
				br.close();
			} catch(StringIndexOutOfBoundsException s) {
				s.printStackTrace();
			}
			catch (IOException e) {
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
				
				while ((content = br.readLine()) != null)
					//(int i = 0; i < LINES_TO_READ; i++) 
					{
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
					/*
					else {
						// not counting the line
						System.out.println("i--");
						i--;
					}
					*/
					
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
				while ((content = br.readLine()) != null)
					//(int i = 0; i < LINES_TO_READ; i++) 
					{
					// not including any entries marked “(TV)” or “(V)” or "(VG)"
					if(!(content.contains("(TV)") || content.contains("(V)") || content.contains("(VG)"))) {
						//System.out.println("CONTENT: " + content);
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
					/*
					else {
						// not counting the line
						i--;
					}
					*/
				}
				br.close();
			} catch(ArrayIndexOutOfBoundsException a) {
				a.printStackTrace();
			}
			catch (IOException e) {
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

				while ((content = br.readLine()) != null)
					//(int i = 0; i < LINES_TO_READ; i++) 
					{
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
					/*
					else {
						// not counting the line
						i--;
					}
					*/
				}
				br.close();
				
			} catch(StringIndexOutOfBoundsException i) {
				i.printStackTrace();
			}
			catch (IOException e) {
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
			try {
		      	String query = "insert into ratings (distribution, votes, rating, title, "
		      			+ "year) values (?, ?, ?, ?, ?)";
		      	PreparedStatement preparedStmt = conn.prepareStatement(query);
		      	int numOfTitles = r.titles.size();
		      	for(int i = 0; i < numOfTitles; i++) {
			        preparedStmt.setString (1, r.distributions.remove());
			        preparedStmt.setString (2, r.votesList.remove());
			        preparedStmt.setString (3, r.ratings.remove());
			        preparedStmt.setString (4, r.titles.remove());
			        preparedStmt.setString (5, r.years.remove());
			      	preparedStmt.addBatch();
		      	}
		        preparedStmt.executeBatch();
		        preparedStmt.close();
			}
		    catch (Exception e)
		    {
		      e.printStackTrace();
		    }
		}
		
		private void insertIntoDB(RunningTimes rt) {
			try {
		      	String query = "insert into running_times (title, year, country, runningTime, "
		      			+ "note) values (?, ?, ?, ?, ?)";
		      	PreparedStatement preparedStmt = conn.prepareStatement(query);
		      	int numOfTitles = rt.titles.size();
		      	for(int i = 0; i < numOfTitles; i++) {
			        preparedStmt.setString (1, rt.titles.remove());
			        preparedStmt.setString (2, rt.years.remove());
			        preparedStmt.setString (3, rt.countries.remove());
			        preparedStmt.setString (4, rt.runningTimes.remove());
			        preparedStmt.setString (5, rt.notes.remove());
			      	preparedStmt.addBatch();
		      	}
		        preparedStmt.executeBatch();
		        preparedStmt.close();
			}
		    catch (Exception e)
		    {
		      e.printStackTrace();
		    }
		}
		
		private void insertIntoDB(ReleaseDates rd) {
			try {
		      	String query = "insert into release_dates (title, year, country, releaseDate, "
		      			+ "releaseType) values (?, ?, ?, ?, ?)";
		      	int numOfTitles = rd.titles.size();		      	 
		      	// multiple batches are required as otherwise it's stopping after 80 entries
		      	for(int j = 0; j < 	numOfTitles/BATCH_SIZE; j++) {
			      	PreparedStatement preparedStmt = conn.prepareStatement(query);
			      	for(int i = 0; i < BATCH_SIZE; i++) {
				        preparedStmt.setString (1, rd.titles.remove());
				        preparedStmt.setString (2, rd.years.remove());
				        preparedStmt.setString (3, rd.countries.remove());
				        preparedStmt.setString (4, rd.releaseDates.remove());
				        preparedStmt.setString (5, rd.releaseTypes.remove());
				      	preparedStmt.addBatch();
			      	}
			        preparedStmt.executeBatch();
			        preparedStmt.close();
		      	}		      	
		      	
		      	// handling the remainder in cases where LINES_TO_READ isn't a multiple of BATCH_SIZE
		      	int remainder = numOfTitles % BATCH_SIZE;
		      	if(remainder != 0) {
			      	PreparedStatement preparedStmt = conn.prepareStatement(query);
			      	for(int i = 0; i < remainder; i++) {
				        preparedStmt.setString (1, rd.titles.remove());
				        preparedStmt.setString (2, rd.years.remove());
				        preparedStmt.setString (3, rd.countries.remove());
				        preparedStmt.setString (4, rd.releaseDates.remove());
				        preparedStmt.setString (5, rd.releaseTypes.remove());
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
		
		private void insertIntoDB(Genres g) {
			try {
				// inserting titles & years first, then adding the sets of genres				
		      	String query = "insert into genres (title, year) values (?, ?)";
		      	// multiple batches are required as otherwise it's stopping after 80 entries
		      	//System.out.println("LINES_TO_READ: " + LINES_TO_READ + " BATCH_SIZE: " + 
		      	//		BATCH_SIZE + " LINES_TO_READ/BATCH_SIZE: " + LINES_TO_READ/BATCH_SIZE);
		      	for(int j = 0; j < 	LINES_TO_READ/BATCH_SIZE; j++) {
			      	//System.out.println("TITLES (" + g.titlesList.size() + 
			      	//		"): " + g.titlesList.toString());
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
				//System.out.println("INSERT INTO DB M.MOVIE_TITLES: " + m.movie_titles.toString());
				//System.out.println("INSERT INTO DB M.YEARS: " + m.years.toString());
		      	String query = "insert into movies (title, year) values (?, ?)";
		      	 int size = m.movie_titles.size();
		      	// multiple batches are required as otherwise it's stopping after 80 entries
		      	for(int j = 0; j < 	size/BATCH_SIZE; j++) {
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
		      	int remainder = size % 50;
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
			          movieTitle = rs.getString("title");
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
				else if(dataRequested.equals("releaseDates")) {
			        String query = "SELECT * FROM release_dates";
			        // create the java statement
			        Statement st = conn.createStatement();
			         
			        // execute the query, and get a java resultset
			        ResultSet rs = st.executeQuery(query);
			         
			        // iterate through the java resultset
			        System.out.println("RESULTS");
			        String id, title, year, country, releaseDate, releaseType;
			        while (rs.next())
			        {
			          id = rs.getString("id");
			          title = rs.getString("title");
			          year = rs.getString("year");
			          country = rs.getString("country");
			          releaseDate = rs.getString("releaseDate");
			          releaseType = rs.getString("releaseType");
			           
			          // print the results
			          System.out.format("%s, %s, %s, %s, %s, %s \n", id, title, year, 
			        		  country, releaseDate, releaseType);
			        }
			        st.close();
			      	conn.close();					
				}
				else if(dataRequested.equals("runningTimes")) {
			        String query = "SELECT * FROM running_times";
			        // create the java statement
			        Statement st = conn.createStatement();
			         
			        // execute the query, and get a java resultset
			        ResultSet rs = st.executeQuery(query);
			         
			        // iterate through the java resultset
			        System.out.println("RESULTS");
			        String id, title, year, country, runningTime, note;
			        while (rs.next())
			        {
			          id = rs.getString("id");
			          title = rs.getString("title");
			          year = rs.getString("year");
			          country = rs.getString("country");
			          runningTime = rs.getString("runningTime");
			          note = rs.getString("note");
			           
			          // print the results
			          System.out.format("%s, %s, %s, %s, %s, %s\n", id, title, year, country, 
			        		  runningTime, note);
			        }
			        st.close();
			      	conn.close();
				}	
				else if(dataRequested.equals("ratings")) {
			        String query = "SELECT * FROM ratings";
			        // create the java statement
			        Statement st = conn.createStatement();
			         
			        // execute the query, and get a java resultset
			        ResultSet rs = st.executeQuery(query);
			         
			        // iterate through the java resultset
			        System.out.println("RESULTS");
			        String id, distribution, votes, rating, title, year;
			        while (rs.next())
			        {
			          id = rs.getString("id");
			          distribution = rs.getString("distribution");
			          votes = rs.getString("votes");
			          rating = rs.getString("rating");
			          title = rs.getString("title");
			          year = rs.getString("year");
			           
			          // print the results
			          System.out.format("%s, %s, %s, %s, %s, %s\n", id, distribution, 
			        		  votes, rating, title, year);
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
		
		// attributeTypes indicate the column of each attribute
		// e.g. attributes: [19, Short, 2008] attributeTypes: [runningTime, genre, year]
		/* classification is:
		    8.0 - 10: great
			6.5 - 8.0: good
			5.0 - 6.5: mediocre
			0 - 5.0: bad
		 */
		private void naiveBayes(String [] attributes, String [] attributeTypes) {
			try {
				String query = "select runningTime, genre, ratings.year," + 
					" running_times.title, rating from genres, ratings, running_times where " + 
					"(running_times.title = ratings.title AND " + 
					"ratings.title = genres.title AND running_times.year " +
					"= ratings.year AND ratings.year = genres.year);";
		        // create the java statement
		        Statement st = conn.createStatement();
		         
		        // execute the query, and get a java resultset
		        ResultSet rs = st.executeQuery(query);
		         
		        // iterate through the java resultset
		        System.out.println("RESULTS");
		        String runningTime, year, rating, title, genre;
		        
		        double [] priorProbabilities = new double[4];
		        double [] frequencies = {0, 0, 0, 0}; // frequencies of each class
		        double [][] attributeFrequencies = new double [4][attributes.length];
		        double frequency;
		        double totalInstances = 0;
		        double [] classes = {8.0, 6.5, 5.0, 0};
		        while (rs.next())
		        {
				      runningTime = rs.getString("runningTime");
			          genre = rs.getString("genre");
			          year = rs.getString("year");
			          title = rs.getString("title");
			          rating = rs.getString("rating");
			          
			          for(int i = 0; i < attributes.length; i++) {
				          if(attributeTypes[i].equals("runningTime")) {
				        	  if(attributes[i].equals(runningTime)) {
				        		  System.out.println("INSIDE IF 1");
						          for(int a = 0; a < classes.length; a++) {
					        		  // if the rating matches the classification
							          if(Double.parseDouble(rating) >= classes[a]) {
							        	  attributeFrequencies[a][i]++;
							          }
						          }
				        	  }
				          } else if(attributeTypes[i].equals("genre")) {
				        	  if(attributes[i].equals(genre)) {
				        		  System.out.println("INSIDE IF 2");
						          for(int a = 0; a < classes.length; a++) {
					        		  // if the rating matches the classification
							          if(Double.parseDouble(rating) >= classes[a]) {
							        	  attributeFrequencies[a][i]++;
							          }
						          }
				        	  }
				          } else if(attributeTypes[i].equals("year")) {
				        	  if(attributes[i].equals(year)) {
				        		  System.out.println("INSIDE IF 3");
						          for(int a = 0; a < classes.length; a++) {
					        		  // if the rating matches the classification
							          if(Double.parseDouble(rating) >= classes[a]) {
							        	  attributeFrequencies[a][i]++;
							          }
						          }
				        	  }
				          }
			          }	

		          
			          // print the results
			          //System.out.format("%s, %s, %s, %s, %s\n", runningTime, genre, year, title, rating);
	
				       // prior probability of the classes: great, good, mediocre, bad
			          frequency = Double.parseDouble(rating);
			          //System.out.println("Freq: " + frequency);
			          if(frequency >= 8.0)
			        	  frequencies[0]++;
			          else if(frequency >= 6.5)
			        	  frequencies[1]++;
			          else if(frequency >= 5.0)
			        	  frequencies[2]++;
			          else if(frequency >= 0)
			        	  frequencies[3]++;

			          totalInstances++;
		        }     
		        System.out.println("FREQUENCIES: " + Arrays.toString(frequencies));
		        priorProbabilities[0] = frequencies[0]/totalInstances;
		        priorProbabilities[1] = frequencies[1]/totalInstances;
		        priorProbabilities[2] = frequencies[2]/totalInstances;
		        priorProbabilities[3] = frequencies[3]/totalInstances;
		        System.out.println("PRIOR: " + Arrays.toString(priorProbabilities));
		        
		        // getting probability of attribute given the class
		        
		        double [][] pAttClass = new double [4][attributes.length];
		        /*
		        for(int j = 0; j < pAttClass.length; j++) {
			        // finding the class
			          if(classification >= 8.0)
			        	  pAttClass[j] = attributeFrequencies[j] / frequencies[0];
			          else if(classification >= 6.5)
			        	  pAttClass[j] = attributeFrequencies[j] / frequencies[1];
			          else if(classification >= 5.0)
			        	  pAttClass[j] = attributeFrequencies[j] / frequencies[2];
			          else if(classification >= 0)
			        	  pAttClass[j] = attributeFrequencies[j] / frequencies[3];
		        }
		        */
		        
		        double [] multiples = new double[classes.length];
		        for(int b = 0; b < classes.length; b++) {
		        	for(int c = 0; c < attributes.length; c++) {
			        	pAttClass[b][c] = attributeFrequencies[b][c] / frequencies[b];
		        	}
			        // multiplying the values of each attribute-given-class
		        	
		        }
		        String chosenClass = ""; double max = 0;
		        for (int b = 0; b < classes.length; b++) {
		        	for(int c = 0; c < attributes.length; c++) {
		        		if(c == 0) {
		        			multiples[b] = pAttClass[b][c];
		        		}
		        		else {
		        			multiples[b] = multiples[b] * pAttClass[b][c];
		        		}
		        	}
		        	if(multiples[b] >= max)
		        		max = multiples[b];
		        }

		        System.out.println("max: " + max);
		          if(max >= 8.0)
		        	  chosenClass = "great";
		          else if(max >= 6.5)
		        	  chosenClass = "good";
		          else if(max >= 5.0)
		        	  chosenClass = "mediocre";
		          else if(max >= 0)
		        	  chosenClass = "bad";
		        System.out.println("MULTIPLES: " + Arrays.toString(multiples));
		        System.out.println("CHOSEN CLASS: " + chosenClass);
		        
		        
		        st.close();
		      	conn.close();
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
		
		//Genres g = new Genres();
		//g.readGenresFromFile();
		
		//ReleaseDates rd = new ReleaseDates();
		//rd.readReleaseDatesFromFile();
		
		//RunningTimes rt = new RunningTimes();
		//rt.readRunningTimesFromFile();
		
		//Ratings r = new Ratings();
		//r.readRatingsFromFile();
		
		try {
			String myDriver = "org.gjt.mm.mysql.Driver";
	      	String myUrl = "jdbc:mysql://localhost/ca4002?" + 
	      					"&rewriteBatchedStatements=true";
	      	Class.forName(myDriver);
	      	Connection conn = DriverManager.getConnection(myUrl, "root", password);
	      	System.out.println("connected");
			Database db = new Database(conn);
			
			//System.out.println("NUM OF EXCEPTIONS: " + rt.numOfExceptions);
			//db.insertIntoDB(rd);
			//db.queryDB("releaseDates");
			String [] atts = {"19", "2008"}; String [] attTypes = {"runningTime", "year"};
			db.naiveBayes(atts, attTypes);
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
