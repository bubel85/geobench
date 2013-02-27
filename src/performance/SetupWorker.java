
package performance;

import java.io.IOException;

import javax.swing.JTextArea;
/*
 * A Setup Thread for a testcase
 * 
 */
public class SetupWorker extends Thread {
	private int i = 0;
	private JTextArea setup;
	private PostgresDataBase db;
	private MongoDataBase mdb;
	private CouchDataBase cdb;

	
	public SetupWorker (int i, JTextArea o, PostgresDataBase db, MongoDataBase mdb, CouchDataBase cdb) {
		this.i = i;
		this.setup = o;
		this.db = db;
		this.mdb = mdb;
		this.cdb = cdb;
	

	}
	
	public void run() {
		try {
			db.setup(i, setup);
			mdb.setup(i, setup);
			cdb.setup(i, setup);
		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
