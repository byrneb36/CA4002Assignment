import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

class ReadFromFileTest {
	// it seems to be just TV entries before these lines
	private static int MOVIES_LINES_TO_SKIP = 2128910;
	private static int RATINGS_LINES_TO_SKIP = 262652;
	private static int GENRES_LINES_TO_SKIP = 155886;
			
			//154269;
	
	private static int LINES_TO_READ = 200;
	private static LinkedList<String> movie_titles;
	private static LinkedList<String> years;
	
	private static void readGenresFromFile() {
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
			
			List<Set<String>> listOfGenreSets = new ArrayList<Set<String>>();
			String [] yearsList = new String[LINES_TO_READ];
			String [] titlesList = new String[LINES_TO_READ];
			
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
			yearsList[0] = firstYearIncludingVersion;
			titlesList[0] = firstTitle;
			
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
						yearsList[i] = firstYearIncludingVersion;
						titlesList[i] = firstTitle;
						
						// change the previous entry's data with the current entry's
						firstGenre = genre; firstYearIncludingVersion = yearIncludingVersion; firstTitle = title;	
						firstGenresSet = genresSet;
						
						firstGenresSet.add(firstGenre);
					}
				}
				
			}

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
	
	private static void readRatingsFromFile() {
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
				}
			}
			

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
	
	private static void readMoviesFromFile() {
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
	
	private static void insertIntoDB(Connection conn) {
		try
    	{		
	      	String query = "insert into movies (movieTitle) values (?)";
	      	 
	      	PreparedStatement preparedStmt = conn.prepareStatement(query);
	      	for(int i = 0; i < LINES_TO_READ; i++) {
		        preparedStmt.setString (1, movie_titles.remove());
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
	
	private static void queryDB(Connection conn) {
		try {
	        String query2 = "SELECT * FROM movies";
	        // create the java statement
	        Statement st = conn.createStatement();
	         
	        // execute the query, and get a java resultset
	        ResultSet rs = st.executeQuery(query2);
	         
	        // iterate through the java resultset
	        System.out.println("RESULTS");
	        while (rs.next())
	        {
	          String id = rs.getString("id");
	          String movieTitle = rs.getString("movieTitle");
	           
	          // print the results
	          System.out.format("%s, %s\n", id, movieTitle);
	        }
	        st.close();
	      	conn.close();
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	}
	
	public static void main(String [] args) {
		try {
			/*
			String myDriver = "org.gjt.mm.mysql.Driver";
	      	String myUrl = "jdbc:mysql://localhost/ca4002";
	      	Class.forName(myDriver);
	      	Connection conn = DriverManager.getConnection(myUrl, "root", "Sh4k3sp34r3");
	      	System.out.println("connected");
	      	*/
			readGenresFromFile();
	      	//readRatingsFromFile();
	      	//readMoviesFromFile();
	      	//insertIntoDB(conn);
	      	//queryDB(conn);
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	}
}
