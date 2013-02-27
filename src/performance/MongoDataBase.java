package performance;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

import org.bson.types.*;
import java.io.IOException;

import java.util.*;

import javax.swing.JTextArea;


import utilities.*;

/**
 * The main class handling database connections<br>
 * MongoDataBase can be initialized containing either a simple connection or as a thread containing a connection 
 */
public class MongoDataBase extends DataBase{
	
	private DBCollection dbCol;
	private int port = 0;
	private Mongo m = null;
	private DB mdb = null;
    	private WriteConcern writeConcern;

    	public MongoDataBase(){
    		server = db_properties.getProperty("mongodbserver");
		dbName = db_properties.getProperty("mongodbname");
		port = Integer.valueOf(db_properties.getProperty("mongodbport")).intValue();
		user = db_properties.getProperty("mongodbuser");
		pass = db_properties.getProperty("mongodbpass");
		rg = RandomGenerator.getInstance();
		
    	}
    
	/**
	 * Initializes a simple database object containing the standard settings provided by the connection.properties file. 
	 */
	public MongoDataBase(int i, int testcase) {
		
		server = db_properties.getProperty("mongodbserver");
		dbName = db_properties.getProperty("mongodbname");
		port = Integer.valueOf(db_properties.getProperty("mongodbport")).intValue();
		user = db_properties.getProperty("mongodbuser");
		pass = db_properties.getProperty("mongodbpass");
		rg = RandomGenerator.getInstance();
		threadnumber = i;
		this.testcase=testcase;

	}
	
	/**
	 * Initializes a database object with completely custom values in order to connect to a database of your own.
	 * This constructor does not use connection.properties
	 * and should be used when using MongoDataBase as a thread.
	 * 
	 * @param dbserver name of the server
	 * @param dbport port
	 * @param dbname database name
	 * @param dbuser database user
	 * @param dbpass user password
	 * @param threadnumber the thread number
	 * @param testcase the testcase number
	 */
	public MongoDataBase(String dbserver, String dbport, String dbname, String dbuser, String dbpass,int threadnumber, int testcase) {
		server = dbserver;
		dbName = dbname;
		port = Integer.parseInt(dbport);
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
        		String writeConcernType = test_properties.getProperty("writeConcern");
            		if ("none".equals(writeConcernType)) {
              			writeConcern = WriteConcern.NONE;
           		} else if ("safe".equals(writeConcernType)) {
               			writeConcern = WriteConcern.SAFE;
           		} else if ("normal".equals(writeConcernType)) {
                		writeConcern = WriteConcern.NORMAL;
            		} else if ("journal".equals(writeConcernType)) {
                		writeConcern = WriteConcern.JOURNAL_SAFE;
           		} else if ("fsync".equals(writeConcernType)) {
                		writeConcern = WriteConcern.FSYNC_SAFE;
            		}
        	
        		m = new Mongo (server, port);
        		m.setWriteConcern(writeConcern);
            		mdb = m.getDB(dbName);
            
           	 	// Try to authenticate.
           		if (!(user.equals("") || pass.equals("")))
            			mdb.authenticate(user, pass.toCharArray());
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
			this.m.close();
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
		if (m == null) return false;
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
				System.err.println("Mongo Thread: "+threadnumber+" NOT connected");
				Performance.console.append("Mongo Thread: "+threadnumber+" NOT connected\n");
				Performance.error=true;
			} else {
				System.out.println("Mongo Thread: "+threadnumber+" connected");
		
			}			
	
			//wait for others to connect and generate optional data for testcases.
			if (Performance.EXTERNAL_CONNECTION)
				Performance.threadconnectionbarrierextern.waitForOthers();
			else
				Performance.threadconnectionbarrier.waitForOthers();		
				
			Performance.threadstartbarrierM.waitForOthers(this);
			
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
		int i=0, j=0;
		Integer index;
		char[] c = new char[8];
		String x;
		
		if (s.toString().contains("geoNear")){
			Performance.near=(new String(s)).substring(s.indexOf("{"), s.lastIndexOf("}")+1);
			
			s=new StringBuffer(Performance.near);
		
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
					Performance.paramsM.add(x);
					Performance.placesM.add(index);
					c=c.clone();
					j=0;
				}
					else
						i++;
			}
			
				Performance.requestM = new StringBuffer(s);
			
		}
		
		else {
			
		if (s.toString().contains("limit"))	{
			int a, b;
			a =s.lastIndexOf("(");
			b=a+1;
			while (s.charAt(b)!=')'){
				if (Character.isDigit(s.charAt(b)))
					Performance.limit.append(s.charAt(b));
				b++;
			}
		}
			
		int l=3;

		while (s.charAt(l)!='.'){
			Performance.col.append(s.charAt(l));
			l++;
		}
	
		int k=s.indexOf("{");
		k++;
		while (s.charAt(k)!=':'){
			if (s.charAt(k)!=' '& s.charAt(k)!='"'& s.charAt(k)!='\'')
				Performance.geoField.append(s.charAt(k));
			k++;
		}
	
		k++;
	
		s=new StringBuffer(s.substring(k, s.lastIndexOf("}")));
		
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
			
				Performance.paramsM.add(x);
				Performance.placesM.add(index);
				c=c.clone();
				j=0;
			}
			else
				i++;
		}
		
			Performance.requestM = new StringBuffer(s);
	}
}
	
	
	/**
	 * Creates a coolection
	 */
	public void createSimpleTable () {
		try {
			dbCol = mdb.getCollection("genericCollection");
				
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	/**
	 * Method to get a clean database
	 */
	public void dropAll () {
		dbCol=mdb.getCollection("genericCollection");
		if (dbCol.getIndexInfo().size()>0)
			dbCol.dropIndexes();
		dbCol.drop();
			   
		
	}

	/**
	 * Bulk Loading
	 */
	public void bulkLoad() {
		List<DBObject>docs=new ArrayList<DBObject>();
		int a, b;
		try {
			rg = RandomGenerator.getInstance();
			System.out.println("Starting Benchmark");
			dbCol=mdb.getCollection("genericCollection");
			for (int x = 0; x<insert; x++) {
				a=new Integer(rg.getRandom(180));
				b=new Integer(rg.getRandom(90));
				BasicDBObject doc = new BasicDBObject();
				doc.put("_id", ObjectId.get());
				doc.put("RandomName", rg.getRandomString(stringlength));
				doc.put("RandomSize", rg.getRandomNum(numlength));
				doc.put("RandomGeo", new Integer[]{a, b});
				docs.add(doc);
			
			}
			mdb.requestStart();
			start=Calendar.getInstance().getTimeInMillis();
			dbCol.insert(docs);
			stop = Calendar.getInstance().getTimeInMillis() - start;
			mdb.requestDone();
			Timer=stop;	
			System.out.println("Mongo Timer "+Long.toString(Timer));
			disconnect();
		
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
		Performance.console.append("Successfull reset of Mongo DB\n");
		
	}
	
	/*
	 * Creates a spatial index
	 */
	public void addIndex(){
		dbCol=mdb.getCollection("genericCollection");
		start=Calendar.getInstance().getTimeInMillis();
		dbCol.ensureIndex(new BasicDBObject("RandomGeo", "2d"));
		stop = Calendar.getInstance().getTimeInMillis() - start;
		System.out.println("Mongo Index Timer "+Long.toString(stop));
	}
	
	
	/*
	 * SETUP METHODS
	 * to show the user an example iteration
	 */
		
	
	public void setupInsertTestcase(JTextArea setup) {
		try {
	
			setup.append("\ndb.genericCollection.insert \n({\"_id\" : "+ObjectId.get()+",\n\"RandomName\" : \""+ rg.getRandomString(stringlength)+"\", \n\"RandomSize\" : "+ rg.getRandomNum(numlength)+",\n\"RandomGeo\" : ["+ rg.getRandom(180)+", "+rg.getRandom(90)+"]})");
			setup.append("\n---------------------------------------------------------------");
			System.err.println("Setup complete!");
	
		} catch (Exception e) {
		}
	}

	
	public void setupSelectTestcase(JTextArea setup) {
		try {
			setup.append("\ndb.genericCollection.find \n( { RandomGeo : {'$within' : {'$box' : [["+rg.getRandomN(180)+","+rg.getRandomN(90)+"], \n["+rg.getRandomP(180)+","+rg.getRandomP(90)+"]] } } })");
			setup.append("\n---------------------------------------------------------------");
			System.err.println("Setup complete!");
		
		
		} catch (Exception e) {
		}
	}

	
	public void setupExternTestcase(JTextArea setup) {
		String str;
		try {
			str=FileLoader.getLine(1)[0]; 
			Performance.paramsM.clear();
			Performance.placesM.clear();
			
			if (str.contains("geoNear")){
				parseTest(new StringBuffer(str)); 
				setup.setText(setup.getText()+"\ndb.runCommand("+parseToRequest(Performance.requestM)+")\n");
				Performance.geonear=true;
			}
			else
			{
			Performance.geoField=new StringBuffer();
			Performance.col=new StringBuffer();
			parseTest(new StringBuffer(str)); 
			if (str.contains("limit"))
				setup.setText(setup.getText()+"\n"+"db."+Performance.col+".find({\""+Performance.geoField+"\":"+
						parseToRequest(Performance.requestM)+"})."+str.substring(str.lastIndexOf(".")+1, str.length()) +"\n");
			else
				setup.setText(setup.getText()+"\n"+"db."+Performance.col+".find({\""+Performance.geoField+"\":"+
						parseToRequest(Performance.requestM)+"})\n");
			
			}
		} catch (Exception e) {
		}
	}
	
	
	public void selectCase(){
		try {
			String a,b,c,d;
	
			System.out.println("Starting Benchmark");
			DBCursor cur=null;;
			DBCollection col;
			int t=0, ex=0;
		
			col=mdb.getCollection("genericCollection");
		
			for (int x = 0; x<select; x++) {
				a=rg.getRandomN(180);
				b=rg.getRandomN(90);
				c=rg.getRandomP(180);
				d=rg.getRandomP(90);
				mdb.requestStart();
				BasicDBObject bo=new BasicDBObject(new BasicDBObject("RandomGeo",JSON.parse("{'$within' : {'$box' : [["+a+", "+b+"],["+c+", "+d+"]]} }")));
				start=Calendar.getInstance().getTimeInMillis();
				cur= col.find(bo);
			
				while(cur.hasNext()) {
				    cur.next();
				}
				
				stop = Calendar.getInstance().getTimeInMillis() - start;
				mdb.requestDone();
				Timer=Timer+stop;	
				
			}
			cur.close();
			System.out.println("Mongo Timer "+Long.toString(Timer));
			disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertCase(){
		try {
			rg = RandomGenerator.getInstance();
			int a, b;
			
			dbCol=mdb.getCollection("genericCollection");
			for (int x = 0; x<insert; x++) {
				a=new Integer(rg.getRandom(180));
				b=new Integer(rg.getRandom(90));
				mdb.requestStart();
				BasicDBObject doc = new BasicDBObject();
				doc.put("_id", ObjectId.get());
				doc.put("RandomName", rg.getRandomString(stringlength));
				doc.put("RandomSize", rg.getRandomNum(numlength));
				doc.put("RandomGeo", new Integer[]{a, b});
				start=Calendar.getInstance().getTimeInMillis();
				dbCol.insert(doc);
			 	stop = Calendar.getInstance().getTimeInMillis() - start;
				mdb.requestDone();
				Timer=Timer+stop;	
			}
			System.out.println("Mongo Timer "+Long.toString(Timer));
			disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String parseToRequest(StringBuffer s){
		Iterator<String> it;
		StringBuffer tmp = new StringBuffer(s);
		it= Performance.paramsM.iterator();
		
		int count=0;
		int next=0;
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
			
			tmp.insert(Performance.placesM.get(count)+next, subst);
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
		
			String s;
			DBCursor cur=null;
			DBCollection col;
			col=mdb.getCollection(Performance.col.toString());//
			System.out.println("Starting Benchmark");
			if (Performance.geonear){
				for (int i = 0; i < Performance.externaliterations; i++) {
					s = parseToRequest(Performance.requestM);
					BasicDBObject cmd = new BasicDBObject();
					cmd =(BasicDBObject)(JSON.parse(s));
					mdb.requestStart();
					start=Calendar.getInstance().getTimeInMillis();
					CommandResult r = mdb.command(cmd); 
					stop = Calendar.getInstance().getTimeInMillis() - start;
					mdb.requestDone();
					Timer=Timer+stop;
				
				}
				Performance.geonear=false;
			}
			else{
				for (int i = 0; i < Performance.externaliterations; i++) {
					s = parseToRequest(Performance.requestM);
					mdb.requestStart();
					if (Performance.limit.length()>0){
						BasicDBObject bo=new BasicDBObject(Performance.geoField.toString(),JSON.parse(s));
						int lim = Integer.parseInt(Performance.limit.toString());
						start=Calendar.getInstance().getTimeInMillis();
						cur= col.find(bo).limit(lim);
						while(cur.hasNext()) {
							cur.next();
							}
						stop = Calendar.getInstance().getTimeInMillis() - start;
				}
				else
				{
					BasicDBObject bo=new BasicDBObject(Performance.geoField.toString(),JSON.parse(s));
					start=Calendar.getInstance().getTimeInMillis();
					cur= col.find(bo);
					while(cur.hasNext()) {
						cur.next();
						}
					stop = Calendar.getInstance().getTimeInMillis() - start;
					
				}
				mdb.requestDone();
				Timer=Timer+stop;
			
			}
				cur.close();
			}
		
		
			System.out.println("Mongo Timer "+Long.toString(Timer));
			disconnect();

		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}

