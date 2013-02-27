
package performance;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import utilities.PropertyLoader;

/*
 * Class handles statistics (storing and retrieving)
 */

public class DataBaseStatistics {
	public static PropertyLoader pl = new PropertyLoader();
	
	public static Connection con = null;
	private static Statement stmt = null;
	private String url = null;
	private String dbName = new String();
	
	private Properties db_properties = pl.loadProperties("connection.properties");
	
	public DataBaseStatistics() {
		dbName = db_properties.getProperty("statdbname");
	}
	
	public boolean connect() {
		try {
			Class.forName("org.postgresql.Driver");
			url = "jdbc:postgresql://" + "localhost" +  "/" + dbName;
			con = DriverManager.getConnection(url, "gis", "gis");
			stmt = con.createStatement();
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public boolean disconnect() {
		try {
			con.close();
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public boolean isConnected() {
		if (con == null) return false;
		else return true;
	}
	
	
	public static void createStatsTable () {
		try {
			String simpleCreator = "CREATE TABLE statistics (id integer PRIMARY KEY, testcase varchar(30), connections integer, iterations integer, replica char(5), mseconds bigint, pseconds bigint, cseconds bigint);" ;
			stmt.executeUpdate(simpleCreator);
			System.out.println("Stats table created");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	// Method to drop the table in database
	public static void dropAll () {
		ResultSet tables;
		try {
			tables = getAllTables();
			if (tables.next()) {
				tables.beforeFirst();
				while (tables.next()) {
					String tableName = tables.getString(3);
					String dropAll = "DROP TABLE IF EXISTS "+tableName+";";
					stmt.executeUpdate(dropAll);
				}
				System.out.println("successfully deleted all tables");
			} else {
				System.out.println("nothing to delete");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static ResultSet getAllTables() {
		try {
			String[] types = {"TABLE"};
			DatabaseMetaData dbmd = con.getMetaData();
			ResultSet resultSet = dbmd.getTables(null, null, "%", types);
			return resultSet;
		} catch (SQLException sqlex) {
			sqlex.printStackTrace();
			return null;
		}
	}
	
	public static void insertStatSet(String testcase, int conn, int iter, String replica, long mseconds, long pseconds, long cseconds) {
		try {
			String simpleInserter = "INSERT INTO statistics (id, testcase, connections, iterations, replica, mseconds, pseconds, cseconds) VALUES (nextval(\'id\'),\'"+testcase+"\', \'"+conn+"\', \'"+iter+"\',\'"+replica+"\', \'"+mseconds+"\', \'"+pseconds+"\',\'"+cseconds+"\');";
			stmt.executeUpdate(simpleInserter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	public List<String[]> getStat(int testcase){
		ResultSet res=null;
		String s;
		List<String[]> strings = new ArrayList<String[]> ();
	
		try{
			if (testcase==1||testcase==2||testcase==6)
				s="SELECT testcase, connections, iterations,replica, avg(pseconds), avg(mseconds), avg(cseconds) from statistics where testcase='"+Integer.toString(testcase)+"' group by testcase, connections, iterations, replica order by connections, iterations";
			else
				s="SELECT testcase, connections, iterations,replica, avg(pseconds), avg(mseconds), avg(cseconds) from statistics where testcase<>'1' and testcase<>'2' and testcase<>'6' group by testcase, connections, iterations, replica order by testcase, connections, iterations";
				
			res = stmt.executeQuery(s);
			while (res.next()){
				String[] record= new String[8];
				for (int i=1; i<=7; i++){
					record[i]=res.getString(i);
				}
				strings.add(record);				
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return strings;
	}
	
	/**
	 * Reset the database. The following methods are invoked:<br><br>
	 * <ul>
	 * <li>{@link DataBaseStatistics}.dropAll()
	 * <li>{@link DataBaseStatistics}.createStatsTable()
	 * </ul>
	 */
	public void resetStat() {
		try {
			dropAll();
			createStatsTable();
			Performance.console.append("Successfull reset of Statistics DB\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
