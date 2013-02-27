package performance;

import java.util.Properties;

import utilities.PropertyLoader;
import utilities.RandomGenerator;

public abstract class DataBase extends Thread{
	public long Timer=0;		  
	protected long start=0, stop=0;
	protected RandomGenerator rg;
	protected PropertyLoader pl = new PropertyLoader();
	protected String server = new String();
	protected int port = 0;
	protected String dbName = new String();
	protected int threadnumber = 0;
	protected int testcase = 0;
	protected String user = new String();
	protected String pass = new String();
	
	//load Property files
	protected Properties test_properties = pl.loadProperties("test.properties");
	protected Properties db_properties = pl.loadProperties("connection.properties");
	
	//initialize values from property files
	protected int insert = Integer.valueOf(test_properties.getProperty("insert")).intValue();
	protected int select = Integer.valueOf(test_properties.getProperty("select")).intValue();
	
	protected int stringlength = Integer.valueOf(test_properties.getProperty("stringlength")).intValue();
	protected int numlength = Integer.valueOf(test_properties.getProperty("numlength")).intValue();
	
	
	
	
	
	
}
