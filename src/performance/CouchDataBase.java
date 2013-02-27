package performance;

import java.io.IOException;
import org.bson.types.*;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map; 
import org.jcouchdb.db.Database;
import org.jcouchdb.db.Server;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.HttpEntity;
import javax.swing.JTextArea;
import utilities.FileLoader;
import utilities.RandomGenerator;



public class CouchDataBase extends DataBase {
	
	private String slave = new String();  
	private HttpClient httpClient;
	private CouchDbInstance dbInstance ;
	private CouchDbConnector dbc;
	boolean batch;
	boolean full_commit;

    	public CouchDataBase(){
    		server = db_properties.getProperty("couchdbserver");
		dbName = db_properties.getProperty("couchdbname");
		port = Integer.valueOf(db_properties.getProperty("couchdbport")).intValue();
		user = db_properties.getProperty("couchdbuser");
		pass = db_properties.getProperty("couchdbpass");
		rg = RandomGenerator.getInstance();
		
    	}
    
	/**
	 * Initializes a simple database object containing the standard settings provided by the connection.properties file. 
	 */
	public CouchDataBase(int i, int testcase) {
		
		server = db_properties.getProperty("couchdbserver");
		dbName = db_properties.getProperty("couchdbname");
		slave = db_properties.getProperty("couchdbslave");
		port = Integer.valueOf(db_properties.getProperty("couchdbport")).intValue();
		user = db_properties.getProperty("couchdbuser");
		pass = db_properties.getProperty("couchdbpass");
		rg = RandomGenerator.getInstance();
		threadnumber = i;
		this.testcase=testcase;
		
	}
	
	/**
	 * Initializes a database object with completely custom values in order to connect to a database of your own.
	 * This constructor does not use connection.properties
	 * and should be used when using CouchDataBase as a thread.
	 * 
	 * @param dbserver name of the server
	 * @param dbport port
	 * @param dbname database name
	 * @param dbuser database user
	 * @param dbpass user password
	 * @param threadnumber the thread number
	 * @param testcase the testcase number
	 */
	public CouchDataBase(String dbserver, String dbport, String dbname, String dbuser, String dbpass, int threadnumber, int testcase) {
		server = dbserver;
		port = Integer.parseInt(dbport);
		dbName = dbname;
		slave = db_properties.getProperty("couchdbslave");
		this.threadnumber = threadnumber;
		this.testcase = testcase;
		user = dbuser;
		pass = dbpass;
		rg = RandomGenerator.getInstance();
	}
	
	/**
	 * Connects to the database
	 * @return true or false depending on successful database connection
	 */
	public boolean connect(){
        try {
        	batch = false;
        	full_commit = false;
        	{
        	StdHttpClient.Builder b= new StdHttpClient.Builder();
        	if (!(user.equals("") || pass.equals(""))){
        		
        		b.username(user);
        		b.password(pass);  
        	}
        	b.connectionTimeout(10000000);
        	httpClient = b.host(server).port(port).build();
        	dbInstance = new StdCouchDbInstance(httpClient);
        	dbc = new StdCouchDbConnector(dbName, dbInstance);
        	dbc.createDatabaseIfNotExists();
        
        	}
        	if (test_properties.getProperty("batch").equals("true"))
        		batch = true;
        	if (test_properties.getProperty("fullcommit").equals("true"))
        		full_commit = true;
        	
        } catch (Exception e) {
        	e.printStackTrace();
			return false;
        }
        return true;
    }
	
	/*
	 * Creates a spatial index
	 */
	public void createDesignDoc(){
		Map<String, Object> viewdoc = new HashMap<String, Object>();
		Map<String, Object> points = new HashMap<String, Object>();
		points.put("points", "function(doc) { if (doc.RandomGeo) { emit({ type: \"Point\", coordinates: [doc.RandomGeo[0], doc.RandomGeo[1]]}, [doc._id, doc.RandomName, doc.RandomSize, doc.RandomGeo]);}}");
		viewdoc.put("_id", "_design/geocouch");
		viewdoc.put("spatial", points);
		start=Calendar.getInstance().getTimeInMillis();
		dbc.create(viewdoc);
		stop = Calendar.getInstance().getTimeInMillis() - start;
		System.out.println("Couch Index Timer "+Long.toString(stop));
		
	}
	
	
	/**
	 * Checks if there is a connection or not
	 * @return true or false
	 */
	public boolean isConnected() {
		if (dbc == null) 
			return false;
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
				System.err.println("Couch Thread: "+threadnumber+" NOT connected");
				Performance.console.append("Couch Thread: "+threadnumber+" NOT connected\n");
				Performance.error=true;
			} else {
				System.out.println("Couch Thread: "+threadnumber+" connected");
			}			
	
			//wait for others to connect
			if (Performance.EXTERNAL_CONNECTION)
				Performance.threadconnectionbarrierextern.waitForOthers();
			else
				Performance.threadconnectionbarrier.waitForOthers();				
			Performance.threadstartbarrierC.waitForOthers(this);
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
		case 1: insertCase(); break;
		case 2: selectCase(); break;
		case 3: resetDB(); break;
		case 4: externalTestcase(); break;
		case 6: bulkLoad(); break;
		case 7: createDesignDoc(); break;
		
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
		char[] c = new char[7];
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
				Performance.paramsC.add(x);
				Performance.placesC.add(index);
				c=c.clone();
				j=0;
			}
			else
					i++;
		}
		Performance.requestC = new StringBuffer(s);
	}
	

	/**
	 * Method to get a clean database
	 */
	public void dropAll () {
		dbc.cleanupViews();
		dbInstance.deleteDatabase(dbName);
		dbInstance.createDatabase(dbName);
	
	
	}

	/**
	 * Bulk loading
	 */
	public void bulkLoad() {
	
		try {
			rg = RandomGenerator.getInstance();
			List<Map<String, Object>>docs = new ArrayList<Map<String, Object>>();
			int a, b;
			
			System.out.println("Starting Benchmark");
			for (int x = 0; x<insert; x++) {
				a=new Integer(rg.getRandom(180));
				b=new Integer(rg.getRandom(90));
				Map<String, Object> doc = new HashMap<String, Object>();
				doc.put("_id", ObjectId.get().toString());
				doc.put("RandomName", rg.getRandomString(stringlength));
				doc.put("RandomSize", rg.getRandomNum(numlength));
				doc.put("RandomGeo", new Integer[]{new Integer(a), new Integer(b)});
				docs.add(doc);
					
			}
			start=Calendar.getInstance().getTimeInMillis();
			dbc.executeBulk(docs);
			stop = Calendar.getInstance().getTimeInMillis() - start;
			Timer=stop;	
			dbc.clearBulkBuffer();
			if (Performance.replica.equals("true"))
				dbc.replicateTo("http://"+slave+":"+port+"/"+dbName);
							
			System.out.println("Couch Timer "+Long.toString(Timer));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	
	/**
	 * Resets the complete database
	 * </ul>
	 */
	public void resetDB() {
		try {
			dropAll();	
			
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		Performance.console.append("Successfull reset of Couch DB\n");
	}
	
	
	
	/*
	 * SETUP METHODS
	 * to show the user an example iteration
	 */
		
	public void setupInsertTestcase(JTextArea setup) {
		try {
			setup.append("\nPUT '{\"_id\": \""+ObjectId.get()+"\",\n\"RandomName\": \""+rg.getRandomString(stringlength)+"\",\n\"RandomSize\": "+rg.getRandomNum(numlength)+",\n\"RandomGeo\": ["+ rg.getRandom(180)+", "+rg.getRandom(90)+"]}'");
			System.err.println("Setup complete!");
		} catch (Exception e) {
		}
	}

	public void setupSelectTestcase(JTextArea setup) {
		try {
			setup.append("\nGET /"+dbName+"/_design/geocouch/_spatial/\npoints?bbox="+rg.getRandomN(180)+","+rg.getRandomN(90)+","+rg.getRandomP(180)+","+rg.getRandomP(90));
			System.err.println("Setup complete!");
		} catch (Exception e) {
		}
	}


	public void setupExternTestcase(JTextArea setup) {
		String str;
		try {
			str=FileLoader.getLine(1)[0]; 
			Performance.paramsC.clear();
			Performance.placesC.clear();
			parseTest(new StringBuffer(str)); 
			setup.setText(setup.getText()+"\nGET "+parseToRequest(Performance.requestC));
		
		} catch (Exception e) {
		}
	}
	
	public void selectCase(){
		try {
			String a,b,c,d;
			System.out.println("Starting Benchmark");
			org.apache.http.HttpResponse response=null;
			HttpUriRequest httpMethod = null;
				
			for (int x = 0; x<select; x++) {
				a=rg.getRandomN(180);
				b=rg.getRandomN(90);
				c=rg.getRandomP(180);
				d=rg.getRandomP(90);
				DefaultHttpClient client = new DefaultHttpClient();
				HttpUriRequest httpMethod1 = new HttpGet("http://"+server+":"+port+"/"+dbName+"/_design/geocouch/_spatial/points?bbox="+a+","+b+","+c+","+d);
				HttpUriRequest httpMethod2 = new HttpGet("http://"+slave+":"+port+"/"+dbName+"/_design/geocouch/_spatial/points?bbox="+a+","+b+","+c+","+d);
				if (!(slave.equals(""))){
					if (x % 2 == 0) {
						httpMethod = httpMethod1;
						}
					else 
						httpMethod = httpMethod2;
				}
				else 
					httpMethod = httpMethod1;
				start = Calendar.getInstance().getTimeInMillis();
				response = client.execute(httpMethod);
				stop = Calendar.getInstance().getTimeInMillis() - start;
				Timer=Timer+stop;	
			}
			System.out.println("Couch Timer "+Long.toString(Timer));
		} catch (Exception e) {
			e.printStackTrace();
			Performance.console.append(e.getMessage()+"\n");
		}
	}
	
	public void insertCase(){
		try {
			rg = RandomGenerator.getInstance();
			int a, b;
			for (int x = 0; x<insert; x++) {
				a=new Integer(rg.getRandom(180));
				b=new Integer(rg.getRandom(90));
				Map<String, Object> doc = new HashMap<String, Object>();
				
				doc.put("_id", ObjectId.get().toString());
				doc.put("RandomName", rg.getRandomString(stringlength));
				doc.put("RandomSize", rg.getRandomNum(numlength));
				doc.put("RandomGeo",  new Integer[]{new Integer(a), new Integer(b)});
				String id = ObjectId.get().toString();
				String name =rg.getRandomString(stringlength);
				int size =rg.getRandomNum(numlength);
				int ag =new Integer(a);
				int bg= new Integer(b);
				String batchput1="http://"+server+":"+port+"/"+dbName+"/"+id+"?batch=ok";
				String batchput2="{\"RandomName\": \""+name+"\", \"RandomSize\":"+size+", \"RandomGeo\":["+ag+","+bg+" ]}";
					
				start=Calendar.getInstance().getTimeInMillis();
				if (batch)
					httpClient.put(batchput1,batchput2);
				else
					dbc.create(doc);
				if (full_commit)
					dbc.ensureFullCommit();
				stop = Calendar.getInstance().getTimeInMillis() - start;
				Timer=Timer+stop;	
							
			}
			if (Performance.replica.equals("true"))
				dbc.replicateTo("http://"+slave+":"+port+"/"+dbName);
			
			System.out.println("Couch Timer "+Long.toString(Timer));

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
		it= Performance.paramsC.iterator();
		int count=0;
		String subst;
		int next=0;
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
						subst="";
						System.err.println("Unknown parameter");
						return null;
					}
				}
			}
			tmp.insert(Performance.placesC.get(count)+next, subst);
			count++;
			next=next+subst.length();
		}
		return tmp.toString();
	}
	
	/**
	 * External Testcase loaded by the user 
	 */
	public void externalTestcase() {
		try {
			String s;
			org.apache.http.HttpResponse response=null;
			System.out.println("Starting Benchmark");
			for (int i = 0; i < Performance.externaliterations; i++) {
				DefaultHttpClient client = new DefaultHttpClient();
				s = parseToRequest(Performance.requestC);
				HttpUriRequest httpMethod = new HttpGet(s);
				start = Calendar.getInstance().getTimeInMillis();
				response = client.execute(httpMethod);
				stop = Calendar.getInstance().getTimeInMillis() - start;
				Timer=Timer+stop;
					
			}
			FileLoader.reset();
			System.out.println("Couch Timer "+Long.toString(Timer));
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}


}
