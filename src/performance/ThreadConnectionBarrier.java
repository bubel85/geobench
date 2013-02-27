
package performance;

/*
 * A barrier for Client Threads of a database -
 * they have to wait for all Threads to be connected to the database
 * before the user gets a response about established connections
 */

public class ThreadConnectionBarrier {
	protected int threshold;
	protected int count = 0;
	
	public ThreadConnectionBarrier(int t) {
		threshold = t;
	}

	public void reset() {
		count=0;
	}
	
	public synchronized void waitForOthers() 
		throws InterruptedException
		{
			count++;
			if (count==threshold) {
				done();
				notifyAll();
			}
			else while (count<threshold) {
				wait();
			}
		}
	
	public void done() {
	
		if (!Performance.error){
			System.out.println("All Threads are connected and initialized");
			Performance.console.append("All Threads are connected and initialized\n");
		}
		Performance.error=false;
	}
}