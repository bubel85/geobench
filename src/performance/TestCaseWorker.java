
package performance;

/*
 * Starts a thread for executing of
 * ResetDBs or
 * Reset Statistics or
 * Create Spatial Index
 */

public class TestCaseWorker extends Thread {
	private int query = 0;
	private PostgresDataBase db;
	private MongoDataBase mdb;
	private CouchDataBase cdb;
	private DataBaseStatistics dbs;
	
	
	public TestCaseWorker (int o, PostgresDataBase db, MongoDataBase mdb, CouchDataBase cdb, DataBaseStatistics dbs) {
		this.query = o;
		this.db = db;
		this.mdb = mdb;
		this.cdb = cdb;
		this.dbs = dbs;
	}
	
	public void run() {
		if (query==3 || query==7){
			db.test(query);
			mdb.test(query);
			cdb.test(query);
		}
		if (query==5)
			dbs.resetStat();
		
	}
}
