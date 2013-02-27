package performance;

/*
 * A barrier for Client Threads of a database -
 * they have to wait till the user clicks "Run"
 */

public class ThreadStartBarrier {

	public ThreadStartBarrier() {
	}

	
	public synchronized void waitForOthers(PostgresDataBase db) {
		try {
			wait();

		} catch (InterruptedException e) {
			db.disconnect();
			Thread.currentThread().interrupt();
		
			
		}
	}
	public synchronized void waitForOthers(MongoDataBase db) {
		try {
			wait();
	
		} catch (InterruptedException e) {
			db.disconnect();
			Thread.currentThread().interrupt();
		
			
		}
	}
	public synchronized void waitForOthers(CouchDataBase db) {
		try {
			wait();

		} catch (InterruptedException e) {
			
			Thread.currentThread().interrupt();
			
		}
	}
	
	public synchronized void release() {
		notifyAll();
	
	}
	
}
