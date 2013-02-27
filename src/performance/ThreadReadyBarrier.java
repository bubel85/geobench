
package performance;

import info.monitorenter.gui.chart.traces.Trace2DLtd;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.io.BufferedWriter;
import java.io.FileWriter;


/*
 * A barrier for Client Threads of a database -
 * they have to wait for all Threads to get ready with a testcase
 * before it calculates an average time and the user gets a response about results of the test
 */
public class ThreadReadyBarrier {
	protected int threshold;
	protected int count1=0, count2 = 0, count3=0;
	String s=null;
	long time1=0, time2=0, time3=0;
	int counter=0;
	int conns, iter;
	String test;
	
	public ThreadReadyBarrier(int t, String s) {
		threshold = t;
		this.s=s;
	}
	public ThreadReadyBarrier(int t) {
		threshold = t;
	}

	public void reset() {
		count1=0;
		count2=0;
		count3=0;
		counter=0;
		time1=0;
		time2=0;
		time3=0;
	}
	
	public synchronized void waitForOthers(PostgresDataBase db) 
		throws InterruptedException
		{
			counter++;
			count1++;
			time1=time1+db.Timer;
			if (count1==threshold/3) {
				finalCalculations();
				notifyAll();
			}
			else /*while (count<threshold) */{
				wait();
			}
		}
	public synchronized void waitForOthers(MongoDataBase mdb) 
			throws InterruptedException
			{
				counter++;
				count2++;
				time2=time2+mdb.Timer;
				if (count2==threshold/3) {
					finalCalculations();
					notifyAll();
				}
				else 
					wait();
				
			}

	public synchronized void waitForOthers(CouchDataBase cdb) 
		throws InterruptedException
		{
			counter++;
			count3++;
			time3=time3+cdb.Timer;
			if (count3==threshold/3) {
				finalCalculations();
				notifyAll();
			}
			else 
				wait();
			
		}
	
	
	public void finalCalculations() {
		if (counter==threshold) {
		Performance.console.append("All Threads are done\n");
		Performance.console.setCaretPosition(Performance.console.getDocument().getLength());
		System.out.println("All Threads are done");
		
		if (Performance.EXTERNAL_CONNECTION)
			conns=Performance.externalconnections;
		else
			conns=Performance.connectionsINT;
		
		if (Performance.EXTERNAL_CONNECTION){
			iter=Performance.externaliterations;
			test=Performance.testcaseName;
		}
		else {
			if (Performance.testcase==1 ||Performance.testcase==6){
				iter=Performance.insert;
				test=Integer.toString(Performance.testcase);
			}
			else {
				iter=Performance.select;
				test=Integer.toString(Performance.testcase);
			}
		}
		
		
		try {
			
			BufferedWriter out = new BufferedWriter(new FileWriter("testcase.log", true));			
			{
				out.write("Testcase: "+Performance.testcaseLOG+" "+Performance.testcaseName+"\n" +
						"---settings---\n" +
						"connections: "+conns+"\n" +
						"iterations: "+iter+"\n" +
						"replica: "+Performance.replica+"\n"+				
						"---summary---\n" +
						"elapsed time postgres: "+time1/conns+"\n" +
						"elapsed time mongodb: "+time2/conns+"\n" +
						"elapsed time couchdb: "+time3/conns+"\n" +
						"-------------\n\n");
				
				Performance.updateSummary(time1/conns, time2/conns, time3/conns);
				DataBaseStatistics.insertStatSet(test, conns, iter, Performance.replica, time2/conns, time1/conns, time3/conns);
			} 
			Performance.setExternaConnection(false);
			Performance.setExternalInfo("", "", "", "", "", "");
			Performance.connected=false;
			Performance.open=false;
			out.close();
			createGraph();
		} catch (Exception e) {
			e.printStackTrace();
		} }
	}
	
	public void createGraph(){
		Performance.graphPanel.removeAll();
		Performance.trace=new Trace2DLtd((int)(time1/conns/1000));
		Performance.trace2=new Trace2DLtd((int)(time2/conns/1000));
		Performance.trace3=new Trace2DLtd((int)(time3/conns/1000));
		Performance.trace.setColor(Color.RED);
		Performance.trace.addPropertyChangeListener("Listener", null);
		Performance.trace.setName("Postgres");
		Performance.trace2.setColor(Color.BLUE);
		Performance.trace2.addPropertyChangeListener("Listener", null);
		Performance.trace2.setName("MongoDB");
		Performance.trace3.setColor(Color.GREEN);
		Performance.trace3.setName("CouchDB");
		Performance.trace3.addPropertyChangeListener("Listener", null);
		
		Performance.chart.addTrace(Performance.trace2);
		Performance.chart.addTrace(Performance.trace);
		Performance.chart.addTrace(Performance.trace3);
	
		Performance.chart.setGridColor(Color.BLACK);
		Performance.graphPanel.setPreferredSize(new Dimension(480, 130));
		Performance.chart.setPreferredSize(new Dimension(485, 125));
		Performance.graphPanel.add(Performance.chart, BorderLayout.PAGE_START);
		for (int i=0; i<time1/conns/1000; i++)
			Performance.addValue(i, 2, Performance.trace);
		for (int i=0; i<time2/conns/1000; i++)
			Performance.addValue(i, 2.5, Performance.trace2);
		for (int i=0; i<time3/conns/1000; i++)
			Performance.addValue(i, 3, Performance.trace3);
		Performance.chart.revalidate();
		Performance.graphPanel.revalidate();
		Performance.graphPanel.setVisible(true);
	}
	
}

