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
import java.util.LinkedList;

class ReadFromFileTest {
	private static int MOVIES_LINES_TO_SKIP = 2128910;
	private static int RATINGS_LINES_TO_SKIP = 262652;
	private static int LINES_TO_READ = 200;
	private static LinkedList<String> movie_titles;
	private static LinkedList<String> years;
	
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
					title = content.substring( (content.indexOf( (String) tokens[4]) ) , (content.length() - 6));
					year = content.substring((content.length() - 5), (content.length() - 1));
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
			int firstBracketIndex;
			for (int i = 0; i < LINES_TO_READ; i++) {
				content = br.readLine();
				// not including any entries marked “(TV)” or “(V)” or "(VG)"
				if(!(content.contains("(TV)") || content.contains("(V)")|| content.contains("(VG)"))) {
					firstBracketIndex = content.indexOf('(');
					// taking the title to be everything before the first open bracket
					title = content.substring(0, firstBracketIndex);
					// taking the year to be everything between the first and second brackets
					year = content.substring(firstBracketIndex + 1, content.indexOf((')')));
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
	      	System.out.println("connected");
	      	 
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
			String myDriver = "org.gjt.mm.mysql.Driver";
	      	String myUrl = "jdbc:mysql://localhost/ca4002";
	      	Class.forName(myDriver);
	      	Connection conn = DriverManager.getConnection(myUrl, "root", "Sh4k3sp34r3");
	      	
	      	readRatingsFromFile();
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
