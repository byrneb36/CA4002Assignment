import java.sql.*;

class MySQLTest {

	public static void main(String [] args) {
		try
    	{		
			String myDriver = "org.gjt.mm.mysql.Driver";
	      	String myUrl = "jdbc:mysql://localhost/test";
	      	Class.forName(myDriver);
	      	Connection conn = DriverManager.getConnection(myUrl, "root", "Sh4k3sp34r3");
	      	System.out.println("connected");
	      	String query = " insert into test_table_persons (PersonID, LastName, FirstName, Address, City)"
	      	        + " values (?, ?, ?, ?, ?)";
	      	 
	      	PreparedStatement preparedStmt = conn.prepareStatement(query);
	        preparedStmt.setString (1, "2");
	        preparedStmt.setString (2, "Joe");
	        preparedStmt.setString (3, "Bloggs");
	        preparedStmt.setString (4, "Athlumney Village");
	        preparedStmt.setString (5, "Navan");
	        preparedStmt.execute();
	        
	        String query2 = "SELECT * FROM test_table_persons";
	        // create the java statement
	        Statement st = conn.createStatement();
	         
	        // execute the query, and get a java resultset
	        ResultSet rs = st.executeQuery(query2);
	         
	        // iterate through the java resultset
	        while (rs.next())
	        {
	          int id = rs.getInt("PersonID");
	          String firstName = rs.getString("FirstName");
	          String lastName = rs.getString("LastName");
	           
	          // print the results
	          System.out.format("%s, %s, %s\n", id, firstName, lastName);
	        }
	        st.close();
	      	conn.close();
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	}
}