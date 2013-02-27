
package performance;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import javax.swing.JTextArea;

import org.bson.types.*;

import utilities.*;

/**
 * This is the main class handling database connections.<br>
 * PostgresDataBase can be initialized containing either a simple connection or as a thread containing a connection 
 */
public class PostgresDataBase extends DataBase {
	private String id;
	private Connection con = null;
	private Statement stmt = null;
	private String url = null;

	/**
	 * Initializes a simple database object containing the standard settings provided by the connection.properties file. 
	 */
	public PostgresDataBase() {
		server = db_properties.getProperty("dbserver");
		dbName = db_properties.getProperty("dbname");
		port = Integer.valueOf(db_properties.getProperty("dbport")).intValue();
		user = db_properties.getProperty("dbuser");
		pass = db_properties.getProperty("dbpass");
		rg = RandomGenerator.getInstance();
	}
	
	
	
	/**
	 * Initializes a database object with a thread number and a testcase number.<br>
	 * This constructor should be used when using PostgresDataBase as a thread. 
	 * All other values are taken from connection.properties
	 * 
	 * @param threadnumber the thread number
	 * @param testcase the testcase number
	 */
	public PostgresDataBase(int threadnumber, int testcase) {
		
		server = db_properties.getProperty("dbserver");
		dbName = db_properties.getProperty("dbname");
		port = Integer.valueOf(db_properties.getProperty("dbport")).intValue();
		user = db_properties.getProperty("dbuser");
		pass = db_properties.getProperty("dbpass");
		this.threadnumber = threadnumber;
		this.testcase = testcase;
		rg = RandomGenerator.getInstance();
	}
	
	/**
	 * Initializes a database object with completely custom values in order to connect to a database of your own.
	 * This constructor does not use connection.properties
	 * and should be used when using PostgresDataBase as a thread.
	 * 
	 * @param dbserver name of the server
	 * @param dbport port
	 * @param dbname database name
	 * @param dbuser database user
	 * @param dbpass user password
	 * @param threadnumber the thread number
	 * @param testcase the testcase number
	 */
	public PostgresDataBase(String dbserver, String dbport, String dbname, String dbuser, String dbpass, int threadnumber, int testcase) {
		server = dbserver;
		port = Integer.parseInt(dbport);
		dbName = dbname;
		user = dbuser;
		pass = dbpass;
		this.threadnumber = threadnumber;
		this.testcase = testcase;
		rg = RandomGenerator.getInstance();

	}
	
	
	/**
	 * Connects to the database
	 * @return true or false depending on successful database connection
	 */
	public boolean connect() {
		try {
			Class.forName("org.postgresql.Driver");
			url = "jdbc:postgresql://" + server + ":"+port+ "/" + dbName;
			con = DriverManager.getConnection(url, user, pass);
			stmt = con.createStatement();
	
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Closes the current database connection
	 * @return true or false 
	 */
	public boolean disconnect() {
		try {
			stmt.close();
			con.close();
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * Checks if there is a connection or not
	 * @return true or false
	 */
	public boolean isConnected() {
		if (con == null) return false;
		else return true;
	}
	
	/**
	 * This is the enrty point if used as a thread.<br><br>
	 * Connects to database<br>
	 * Waits for other threads to get connected and goes into a paused mode<br>
	 * If the paused mode is cancelled the actual testcase is called<br>
	 * The thread waits for all other threads to complete their testcases and is then finished.
	 */
	public void run() {
		try {
			if (!connect()){
				System.err.println("Postgres Thread: "+threadnumber+" NOT connected");
				Performance.console.append("Postgres Thread: "+threadnumber+" NOT connected\n");
				Performance.error=true;
			} else {
				System.out.println("Postgres Thread: "+threadnumber+" connected");
			
			}			
			//wait for others to connect and generate optional data for testcases.
			if (Performance.EXTERNAL_CONNECTION)
				Performance.threadconnectionbarrierextern.waitForOthers();
			else
				Performance.threadconnectionbarrier.waitForOthers();		
			
			Performance.threadstartbarrierP.waitForOthers(this);
			
			if (!Thread.interrupted()){
			test(testcase);
			//wait for other threads to finish		
			if (Performance.EXTERNAL_CONNECTION)
				Performance.threadbarrierextern.waitForOthers(this);
			else
				Performance.threadbarrier.waitForOthers(this);	
			}
			} catch (Exception e) {
				e.printStackTrace();
				}
	}
	


	
	/**
	* This is the method called in order to start a testcase. The testcase is called via the following switch cases:<br><br>
	* <ul>
     	* <li>1 - INSERT
     	* <li>2 - SELECT
    	* <li>4 - CUSTOM
     	* <li>6 - BULK LOAD
    	* </ul>
     	* <br>
    	* The following are no testcases but database operations also called via this method
    	* <ul>
   	* <li>3 - reset the database
     	* <li>7 - create a spatial index 
     	* </ul>
     	* <br>
	* @param testcase the testcase number
	*/
	public void test(int testcase) {
		
		switch (testcase) {
		case 1: insertTestcase(); break;
		case 2: selectTestcase();  break;
		case 3: resetDB(); break;
		case 4: externalTestcase(); break;
		case 6: bulkLoad(); break;
		case 7: addIndex(); break;
		}
	}
	
	/**
	 * This is the method to setup a certain testcase<br>
	 * There are several options to choose from.<br><br>
	 * <ul>
	 * <li>1 - sets up the INSERT testcase
	 * <li>2 - sets up the SELECT testcase
	 * <li>4 - sets up the EXTERN testcase
	 * <li>6 - sets up the BULKLOAD testcase (the same as INSERT)
	 * </ul>
	 * <br>
	 * 
	 * @param option the testcase to setup
	 * @param setup the JTextArea where things will be displayed
	 * @param progress the progressbar where progress is displayed
	 * @throws IOException
	 */
	public void setup(int option, JTextArea setup) throws IOException {
		switch (option) {
		case 1: setupInsertTestcase(setup); break;
		case 2: setupSelectTestcase(setup); break;
		case 4: setupExternTestcase(setup); break;
		case 6: setupInsertTestcase(setup); break;
		}			
	}
	
	/*
	 * Parses the content of a .testcase file
	 * and searchs for parameters to generate
	 */
	public void parseTest(StringBuffer s){
		Integer index;
		char[] c = new char[6];
		int j=0, i=0;
		String x;
	
		while (i<s.length()){
			if (s.charAt(i)=='§'){
				index=i;
				while (s.charAt(i)!=')'){
					if (s.charAt(i)!=' '){
						c[j]=s.charAt(i);
						s.deleteCharAt(i);
						j++;
					}else
						s.deleteCharAt(i);
				}
					
				s.deleteCharAt(i);
				x=(new String(c)).substring(0, j);
				Performance.paramsP.add(x);
				Performance.placesP.add(index);
				j=0;
				c=c.clone();
			}
			else
				i++;
		}

		Performance.requestP = new StringBuffer(s);
	}
	
	
	/*
	 * Creates a spatial Index
	 */
	
	public void addIndex(){
		String simpleCreator = "CREATE INDEX generictable_gix ON generictable USING GIST ( RandomGeo );";//+
		String vac = "VACUUM ANALYZE generictable;";
		try {
			start=Calendar.getInstance().getTimeInMillis();
			stmt.execute(simpleCreator);
			stop = Calendar.getInstance().getTimeInMillis() - start;
			stmt.execute(vac);
			System.out.println("Postgres Index Timer "+Long.toString(stop));
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
		
	
	}
	
	/**
	 * Creates a table
	 */
	public void createSimpleTable () {
		try {
		
			String simpleCreator = "CREATE TABLE generictable (ID char(24) PRIMARY KEY, RandomName varchar(50), RandomSize integer); "+ "SELECT AddGeometryColumn( 'generictable', 'randomgeo', 4326, 'POINT', 2);";//+
			stmt.execute(simpleCreator);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	/**
	 * Method to drop the table and the index
	 */
	public void dropAll () {
		try {			
			String drop = "DROP INDEX IF EXISTS generictable_gix;" +
						"DROP TABLE IF EXISTS generictable;";
			stmt.executeUpdate(drop);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Bulk Loading
	 */
	public void bulkLoad() {
		String a, c, d;
		int b;
		try {
			rg = RandomGenerator.getInstance();
			System.out.println("Starting Benchmark");
			for (int x = 0; x<insert; x++) { 
				a=rg.getRandomString(stringlength);
				b=rg.getRandomNum(numlength);
				c=rg.getRandom(180);
				d=rg.getRandom(90);
				id =ObjectId.get().toString();
				String simpleInserter = "INSERT INTO generictable (ID, RandomName, RandomSize, randomgeo) VALUES ( \'"+id+"\', \'"+a+"\', "+b+", ST_GeomFromText(\'POINT("+ c+" "+d+")\', 4326));";
				stmt.addBatch(simpleInserter);
			}
			start=Calendar.getInstance().getTimeInMillis();
			stmt.executeBatch();
			stop = Calendar.getInstance().getTimeInMillis() - start;
			Timer=stop;	
			stmt.clearBatch();
			disconnect();
			System.out.println("Postgres Timer "+Long.toString(Timer));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	/**
	 * Reset the database. The following methods are invoked:<br><br>
	 * <ul>
	 * <li>{@link #dropAll()}
	 * <li>{@link #createSimpleTable()}
	 * </ul>
	 */
	public void resetDB() {
		try {
			dropAll();
			createSimpleTable();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		Performance.console.append("Successfull reset of Postgres DB\n");
	}
	
	
	/**
	 * This methods collects the table structure of the generic table in the database.<br>
	 * @param table the table to get the structure from
	 * @return a List containing the structure info 
	 */
	public List<String[]> getTableStructure(String table) {
		ResultSet tableStructure;
		List<String[]> strings = new ArrayList<String[]> ();
		try {
			String getTableStructure = "SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_name = '"+table+"';";
			tableStructure = stmt.executeQuery(getTableStructure);
			while (tableStructure.next()){
				String[] record= new String[5];
				for (int i=1; i<5; i++){
					record[i]=tableStructure.getString(i);
				}
				strings.add(record);				
			}
			return strings;
		} catch (SQLException ex) {
			ex.printStackTrace();
			return null;
		}
	}	

	
	
	/*
	 * SETUP METHODS
	 * to show the user an example iteration
	 */
		
	
	public void setupInsertTestcase(JTextArea setup) {
		try {
			setup.setText("");
			setup.append("INSERT INTO generictable\n(ID, RandomName, RandomSize, RandomGeo) \nVALUES (\'"+ObjectId.get().toString()+"\', \'"+rg.getRandomString(stringlength)+"\', "+rg.getRandomNum(numlength)+", \nST_GeomFromText(\'POINT("+ rg.getRandom(180)+" "+rg.getRandom(90)+")\', 4326) );");																																																				
			setup.append("\n---------------------------------------------------------------");
			System.err.println("Setup complete!");
		
		} catch (Exception e) {
		}
	}

	
	public void setupSelectTestcase(JTextArea setup) {
	
			setup.setText("");
			setup.append("SELECT * FROM generictable\nWHERE  SetSRID('BOX3D("+rg.getRandomN(180)+" "+rg.getRandomN(90)+","+ rg.getRandomP(180)+" "+rg.getRandomP(90)+")\'::box3d,\n 4326) ~ RandomGeo;");
			setup.append("\n---------------------------------------------------------------");

		
		
	}

	public void setupExternTestcase(JTextArea setup) {
		String str;
		try {
			str=FileLoader.getLine(1)[0]; 
			Performance.paramsP.clear();
			Performance.placesP.clear();
			parseTest(new StringBuffer(str)); 
			setup.setText(parseToRequest(Performance.requestP)+"\n");
		
		} catch (Exception e) {
		}
	}

	
	public void selectTestcase() {			
		try {
	
			ResultSet resultSet=null;
			System.out.println("Starting Benchmark");
			String a, b, c, d, query;
	
				
			for (int x = 0; x<select; x++) {
				
				a=rg.getRandomN(180);
				b=rg.getRandomN(90);
				c=rg.getRandomP(180);
				d=rg.getRandomP(90);
				query ="SELECT * FROM generictable WHERE SetSRID('BOX3D("+a+" "+b+","+ c+" "+d+")'::box3d,4326) ~ RandomGeo;";
		
				start=Calendar.getInstance().getTimeInMillis();
				resultSet= stmt.executeQuery(query);
				stop = Calendar.getInstance().getTimeInMillis() - start;

				Timer=Timer+stop;
				
			}
			System.out.println("Postgres Timer "+Long.toString(Timer));
			resultSet.close();
			this.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertTestcase() {
		String a, c, d;
		int b;
		try {
			for (int x = 0; x<insert; x++) {
				a=rg.getRandomString(stringlength);
				b=rg.getRandomNum(numlength);
				c=rg.getRandom(180);
				d=rg.getRandom(90);
				id = ObjectId.get().toString();
				start=Calendar.getInstance().getTimeInMillis();
				stmt.executeUpdate("INSERT INTO generictable (ID, RandomName, RandomSize, RandomGeo) VALUES (\'"+id+" \', \'"+a+"\', "+b+", ST_SetSRID(ST_MakePoint("+c+", "+d+"), 4326) );\n");	
				stop = Calendar.getInstance().getTimeInMillis() - start;
				Timer=Timer+stop;	
			}
			this.disconnect();
			System.out.println("Postgres Timer "+Long.toString(Timer));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Generate parameters for a custom case 
	 */
	public String parseToRequest(StringBuffer s){
		Iterator<String> it;
		StringBuffer tmp = new StringBuffer(s);
		int next=0;
		it= Performance.paramsP.iterator();
		int count=0;
		float dis;
		String subst;
		while (it.hasNext()){
			String parameter=it.next();
			if (parameter.contains("§S")){
				subst = rg.getRandom(Integer.parseInt(parameter.substring(3, parameter.length())));
			} else {
				if (parameter.contains("§P"))
					subst = rg.getRandomP(Integer.parseInt(parameter.substring(3, parameter.length()))); 
				else {
					if (parameter.contains("§N"))
						subst = rg.getRandomN(Integer.parseInt(parameter.substring(3, parameter.length()))); 
					else {
						if (parameter.contains("§D")){
							subst = rg.getRandomP(Integer.parseInt(parameter.substring(3, parameter.length())));
							dis = Float.parseFloat(subst)/6371;
							subst = String.valueOf(dis);
						}	
						else {
							if (parameter.contains("§F")){
								subst = rg.getRandomP(Integer.parseInt(parameter.substring(3, parameter.length())));
								dis = Float.parseFloat(subst)/111;
								subst = String.valueOf(dis);
							}
							else {
								subst="";
								System.err.println("Unknown parameter");
								return null;
							}
						}
					}
				}
			}
			tmp.insert(Performance.placesP.get(count)+next, subst);
			count++;
			next=next+subst.length();
		}
		return tmp.toString();
	}
	
	/**
	 * External Testcase loaded by the user 
	 * 
	 */
	public void externalTestcase() {
		try {
			ResultSet resultSet=null;
			String s;
			System.out.println("Starting Benchmark..");
			for (int i = 0; i < Performance.externaliterations; i++) {
				s = parseToRequest(Performance.requestP);
				start=Calendar.getInstance().getTimeInMillis();
				resultSet= stmt.executeQuery(s);	
				stop = Calendar.getInstance().getTimeInMillis() - start;
				Timer=Timer+stop;
					
			}

			System.out.println("Postgres Timer "+Long.toString(Timer));
			resultSet.close();
			this.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	
}
