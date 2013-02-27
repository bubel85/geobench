

package performance;


import info.monitorenter.gui.chart.Chart2D;
import info.monitorenter.gui.chart.ITrace2D;
import info.monitorenter.gui.chart.TracePoint2D;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileFilter;

import utilities.FileLoader;
import utilities.PropertyLoader;
import utilities.TestCaseFilter;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.management.ManagementFactory;

/**
 * The main class for geobench containing everything concerning the GUI creation and handling
 * 
 */
public class Performance extends JFrame implements ActionListener{
	
	public static final String SUN_JAVA_COMMAND = "sun.java.command";  
	private static final long serialVersionUID = 1L;
	
	public static boolean error = false;
	public static List<Thread> threads  = new ArrayList<Thread> ();
	public static PostgresDataBase db = new PostgresDataBase();
	public static MongoDataBase mongodb = new MongoDataBase();
	public static CouchDataBase couchdb = new CouchDataBase();
	public static DataBaseStatistics dbstats = new DataBaseStatistics();
	public static List<String> paramsP  = new ArrayList<String> ();
	public static List<Integer> placesP  = new ArrayList<Integer> ();
	public static StringBuffer requestP = new StringBuffer();
	public static List<String> paramsM  = new ArrayList<String> ();
	public static List<Integer> placesM  = new ArrayList<Integer> ();
	public static StringBuffer requestM = new StringBuffer();
	public static List<String> paramsC = new ArrayList<String> ();
	public static List<Integer> placesC  = new ArrayList<Integer> ();
	public static StringBuffer requestC = new StringBuffer();
	public static StringBuffer col=new StringBuffer();
	public static StringBuffer geoField=new StringBuffer();
	public static StringBuffer limit=new StringBuffer();
	public static boolean geonear=false;
	public static String near;
	public static PropertyLoader pl = new PropertyLoader();
	public static String testcaseName="";
	public static Properties test_properties = pl.loadProperties("test.properties");
	public static Properties connection_properties = pl.loadProperties("connection.properties");
	public static int insert = Integer.valueOf(test_properties.getProperty("insert")).intValue();
	public static int select = Integer.valueOf(test_properties.getProperty("select")).intValue();
	public static int externaliterations;
	public static int externalconnections;
	public static String connections = test_properties.getProperty("connections");
	public static int connectionsINT = Integer.valueOf(connections).intValue();
	public static String inserts = test_properties.getProperty("inserts");
	public static String replica = test_properties.getProperty("replica");
	private static String dbserver = new String();
	private static String dbname = new String();
	private static String mongodbserver = new String();
	private static String mongodbname = new String();
	private static String couchdbserver = new String();
	private static String couchdbname = new String();
	public static boolean EXTERNAL_CONNECTION = false;
	public static int testcase = 0;
	public static int setup = 0;
	public static String testcaseString = new String();
	public static String testcaseField = new String();
	public static List<String[]> strings  = new ArrayList<String[]> ();
	public static List<String[]> strings2  = new ArrayList<String[]> ();
	
	private Thread[] mongos  = new Thread[connectionsINT+1];
	private Thread[] couchs = new Thread[connectionsINT+1];
	private Thread[] postgis = new Thread[connectionsINT+1];

	public static ThreadReadyBarrier threadbarrier = new ThreadReadyBarrier(3*connectionsINT);
	public static ThreadConnectionBarrier threadconnectionbarrier = new ThreadConnectionBarrier(3*connectionsINT);//*3
	public static ThreadReadyBarrier threadbarrierextern;
	public static ThreadConnectionBarrier threadconnectionbarrierextern;
	public static ThreadStartBarrier threadstartbarrierP = new ThreadStartBarrier();
	public static ThreadStartBarrier threadstartbarrierM = new ThreadStartBarrier();
	public static ThreadStartBarrier threadstartbarrierC = new ThreadStartBarrier();
	
	public static String testcaseLOG = new String();
	private final static JFileChooser fc = new JFileChooser();
	public static FileLoader fileloader;
	public static boolean open=false;
	public static boolean connected=false;
	public static boolean singleconnected=false;
	
	//Chart Definition
	public static Chart2D chart = new Chart2D();
	public static ITrace2D trace; 
	public static ITrace2D trace2; 
	public static ITrace2D trace3;
	
	//Menu Bar Definition
	private final JMenuBar menubar = new JMenuBar();

	//Panels
	public static JPanel settingsPanel = new JPanel();
	public static JPanel statisticsPanel = new JPanel();
	public static JPanel summaryPanel = new JPanel();
	public static JPanel bottomPanelRight = new JPanel();
	public static JPanel bottomPanelLeft = new JPanel();
	public static JPanel graphPanel = new JPanel();

	//Buttons
	public static JButton setupButton = new JButton("Setup");
	public static JButton connectButton = new JButton("Connect");
	public static JButton runButton = new JButton("Run");
	public static JButton connectNowButton = new JButton("Connect now...");

	//Labels
	private JLabel initLabel = new JLabel ("No testcase selected");
	private static JLabel multithreadedLabel = new JLabel("Connections: ");
	public static JLabel multithreadedText = new JLabel();
	public static JLabel warningText = new JLabel();
	private static JLabel structureLabel = new JLabel("Table description: ");;
	private static JLabel testcaseLabel = new JLabel("Sample testcase: ");
	public static JLabel testcaseText = new JLabel();
	private static JLabel iterationsLabel = new JLabel("Testcase iterations: ");
	public static JLabel iterationsText = new JLabel();
	private static JLabel postgresLabel = new JLabel("PostgreSQL: ");
	public static JLabel postgresText = new JLabel();
	private static JLabel mongoLabel = new JLabel("MongoDB: ");
	public static JLabel mongoText = new JLabel();
	private static JLabel couchLabel = new JLabel("CouchDB: ");
	public static JLabel couchText = new JLabel();
	
	
	private static JLabel serverLabel = new JLabel("Postrges Server: ");
	public static JLabel serverText = new JLabel();
	private static JLabel nameLabel = new JLabel("Postgres DB Name: ");
	public static JLabel nameText = new JLabel();
	
	private static JLabel mserverLabel = new JLabel("Mongo Server: ");
	public static JLabel mserverText = new JLabel();
	private static JLabel mnameLabel = new JLabel("Mongo DB Name: ");
	public static JLabel mnameText = new JLabel();
	
	private static JLabel cserverLabel = new JLabel("Couch Server: ");
	public static JLabel cserverText = new JLabel();
	private static JLabel cnameLabel = new JLabel("Couch DB Name: ");
	public static JLabel cnameText = new JLabel();
	
	private static JLabel connectedLabel = new JLabel("Connected: ");
	public static JLabel connectedText = new JLabel("no");

	//TextAreas
	public static JTextArea testcaseListing = new JTextArea(50, 30);
	public static JTextArea console = new JTextArea(20, 30);

	//Array for GUI Table
	public static String[] columnNames = {"Columnname", "Datatype", "Nullable"};
	public static String[] columnNames2 = {"Testcase", "Clients", "Iterations", "Replication", "Postgres", "MongoDB", "CouchDB"};

	// Border Definition
	private Border colored = BorderFactory.createLineBorder(Color.black);
	private TitledBorder border1 = BorderFactory.createTitledBorder(colored, "Settings");
	private TitledBorder border2 = BorderFactory.createTitledBorder(colored, "Statistics");	
	    
	    
	/**
	 * Calls initGUI
	 */    
	private Performance() {
		try {
			initGUI();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * Initializes the main GUI Elements
	 */
	private void initGUI() {

		runButton.addActionListener(this);
		connectButton.addActionListener(this);
		setupButton.addActionListener(this);
		connectNowButton.addActionListener(this);

		menubar.add(File());
		menubar.add(Testcase());
		menubar.add(CustomTestcase());
		menubar.add(Stats());

		settingsPanel.add(initLabel, BorderLayout.PAGE_START);
		settingsPanel.setPreferredSize(new Dimension(400, 600));
		
		statisticsPanel.setPreferredSize(new Dimension(490, 600)); //500, 600
		summaryPanel.setPreferredSize(new Dimension(480, 270));
		
		JPanel postgres = new JPanel();
		postgres.setLayout(new BoxLayout(postgres, BoxLayout.LINE_AXIS));
		postgres.add(postgresLabel);
		postgres.add(postgresText);
		postgres.add(Box.createHorizontalGlue());
		postgres.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel mongo = new JPanel();
		mongo.setLayout(new BoxLayout(mongo, BoxLayout.LINE_AXIS));
		mongo.add(mongoLabel);
		mongo.add(mongoText);
		mongo.add(Box.createHorizontalGlue());
		mongo.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel couch = new JPanel();
		couch.setLayout(new BoxLayout(couch, BoxLayout.LINE_AXIS));
		couch.add(couchLabel);
		couch.add(couchText);
		couch.add(Box.createHorizontalGlue());
		couch.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel consolePanel = new JPanel();
		consolePanel.setLayout(new BoxLayout(consolePanel, BoxLayout.LINE_AXIS));
		consolePanel.add(console);
		console.setEditable(false);
		console.setText("");
		console.append("");
		consolePanel.add(Box.createHorizontalGlue());
		consolePanel.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		JScrollPane scrollableConsoleArea = new JScrollPane(consolePanel);
		
		summaryPanel.setLayout(new BoxLayout(summaryPanel, BoxLayout.PAGE_AXIS));
		summaryPanel.add(scrollableConsoleArea);
		summaryPanel.add(postgres);
		summaryPanel.add(mongo);
		summaryPanel.add(couch);
		
		
		statisticsPanel.add(summaryPanel, BorderLayout.PAGE_START);
		statisticsPanel.add(graphPanel, BorderLayout.PAGE_START);
		
		bottomPanelRight.add(connectButton, BorderLayout.LINE_END);
		connectButton.setEnabled(false);
		bottomPanelRight.add(runButton, BorderLayout.LINE_END);
		runButton.setEnabled(false);
		
		bottomPanelLeft.setLayout(new BoxLayout(bottomPanelLeft, BoxLayout.LINE_AXIS));
		bottomPanelLeft.add(Box.createRigidArea(new Dimension(220,5)));
		bottomPanelLeft.add(setupButton);
		setupButton.setVisible(false);	

		// Main window layout and cascading
		GridBagConstraints gbc;
		GridBagLayout gbMain = new GridBagLayout();
		setLayout(gbMain);

		settingsPanel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
		statisticsPanel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));

		border1.setTitleJustification(TitledBorder.LEFT);
		settingsPanel.setBorder(border1);
		border2.setTitleJustification(TitledBorder.LEFT);
		statisticsPanel.setBorder(border2);

		gbc = makegbc(1, 1, 1, 1);
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbMain.setConstraints(settingsPanel, gbc);
		this.add(settingsPanel);

		gbc = makegbc(2, 1, 1, 1);
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbMain.setConstraints(statisticsPanel, gbc);
		add(statisticsPanel);
		
		gbc = makegbc(1, 2, 1, 1);
		gbc.anchor = GridBagConstraints.NORTHEAST;
		gbMain.setConstraints(bottomPanelLeft, gbc);
		add(bottomPanelLeft);

		gbc = makegbc(2, 2, 1, 1);
		gbc.anchor = GridBagConstraints.NORTHEAST;
		gbMain.setConstraints(bottomPanelRight, gbc);
		add(bottomPanelRight);

		pack();		

		setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE ); 
		setTitle("geobench");
		setResizable(false);
		setBackground(Color.lightGray);

		setJMenuBar(menubar);
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
		this.setLocation((dim.width - getWidth()) / 2, (dim.height - getHeight()) / 2);
		setSize(1020, 700);
	}
	
	/**
	 * A helper method for using GridBagLayout with its GridBagConstraints.<br>
	 * 
	 * @param x the x coordinate specifying the horizontal positioning
	 * @param y the y coordinate specifying the vertical positioning
	 * @param width the width of the cell to take up, measured in x values
	 * @param height the height of the cell to take up, measured in y values
	 * @return an initiated GridBagConstraints with the provided values set
	 */
	private GridBagConstraints makegbc(int x, int y, int width, int height) {
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.gridx = x;
		gbc.gridy = y;
		gbc.gridwidth = width;
		gbc.gridheight = height;
		gbc.insets = new Insets(1, 1, 1, 1);
		return gbc;
	}
	
	/**
	 * Defines the File menu
	 * 
	 * @return JMenu
	 */
	private JMenu File() {
		JMenu menu = new JMenu("File");
		menu.setMnemonic('F');
		JMenuItem mi;

		mi = new JMenuItem("Restart", 'R');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Exit", 'E');
		mi.addActionListener(this);
		menu.add(mi);

		return menu;
	}
	
	
	/**
	 * Defines the Testcase menu
	 * 
	 * @return JMenu
	 */
	private JMenu Testcase() {
		JMenu menu = new JMenu("GenericBenchmark");
		menu.setMnemonic('G');
		JMenuItem mi;
		
		mi = new JMenuItem("Create Spatial Index", 'C');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Bulk Load", 'B');
		mi.addActionListener(this);
		menu.add(mi);
			
		mi = new JMenuItem("INSERT test", 'I');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("SELECT test", 'S');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Reset DBs", 'D');
		mi.addActionListener(this);
		menu.add(mi);

		return menu;
	}
	
	/**
	 * Defines the Custom Testcase menu
	 * 
	 * @return JMenu
	 */
	private JMenu CustomTestcase() {
		JMenu menu = new JMenu("CustomBenchmark");
		menu.setMnemonic('C');
		JMenuItem mi;
		
		mi = new JMenuItem("Open Testcase", 'O');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Connect to...", 'C');
		mi.addActionListener(this);
		menu.add(mi);

		return menu;
	}
	
	
	/**
	 * Defines the Chart menu
	 * 
	 * @return JMenu
	 */
	private JMenu Stats() {
		JMenu menu = new JMenu("Statistics");
		menu.setMnemonic('S');
		JMenuItem mi;

		mi = new JMenuItem("Selects", 'L');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("BulkInserts", 'B');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Inserts", 'I');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Custom", 'C');
		mi.addActionListener(this);
		menu.add(mi);
		
		mi = new JMenuItem("Reset Statistics", 'S');
		mi.addActionListener(this);
		menu.add(mi);
		
		return menu;
	}
	
	/**
	 * Starts the benchmark threads
	 */
	public void runBench(){
		runButton.setEnabled(false);
		if (testcase == 4) { // custom testcase
			threadstartbarrierP.release(); // postgres Threads don't have to wait anymore
		
			threadstartbarrierM.release();	// mongo Threads don't have to wait anymore
		
				
			threadstartbarrierC.release(); // couch Threads don't have to wait anymore
			for (int j = 1; j <= externalconnections; j++) {
				try {
					ConnectToDialog.epostgis[j].join();
					ConnectToDialog.emongos[j].join();
					ConnectToDialog.ecouchs[j].join();
				} catch (InterruptedException ex) {
					System.out.println("error: interrupted: " + ex);
				}
			}	
		
		} else { // generic testcase
			threadstartbarrierP.release();
			
			threadstartbarrierM.release();	
	
			threadstartbarrierC.release();	
			for (int j = 1; j <= connectionsINT; j++) {
				try {
					postgis[j].join();
					mongos[j].join();
					couchs[j].join();
				} catch (InterruptedException ex) {
					System.out.println("error: interrupted: " + ex);
				}
			}			
		}
	}
	
	/**
	 * Stops the active threads
	 */
	public void stopThreads(){
		Thread[] allthreads = new Thread[Thread.activeCount()];
		Thread.enumerate(allthreads);
		for (Thread ct: allthreads){
			if (threads.contains(ct)){
				ct.interrupt();
			}
		}
	}
	
	/**
	 * Clears the summary of the last test
	 */
	public void clearSummary(){
		graphPanel.setVisible(false);
		postgresText.setText("");
		mongoText.setText("");
		couchText.setText("");
		chart.removeAllTraces();
	}
	
	/**
	 * Handles all events from the GUI<br>
	 * @param e the action set by the user
	 */
	public void actionPerformed(ActionEvent e) {
		if (e.getActionCommand().equals("Exit")) {
			if (singleconnected) {
				mongodb.disconnect();
				db.disconnect();
			}
			dbstats.disconnect();
			stopThreads();
			System.exit(0);
		} else if (e.getActionCommand().equals("Run")) {
			runBench();
		} else if (e.getActionCommand().equals("Restart")) {
			try {
				if (singleconnected) {
					mongodb.disconnect();
					db.disconnect();
				}
				dbstats.disconnect();
				stopThreads();
				setVisible(false);
				restartApplication(null);
				dispose();
               
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} else if (e.getActionCommand().equals("Connect")) {				
			setupButton.setEnabled(false);
			threadconnectionbarrier.reset();
			threadbarrier.reset();
			connectButton.setEnabled(false);
			threads.clear();
			// start postgres Threads
			for (int i=1; i<=connectionsINT; i++) {
				PostgresDataBase multiuserDB = new PostgresDataBase(i, testcase);
				postgis[i] = new Thread(multiuserDB);
				postgis[i].start();
				threads.add(postgis[i]);
			}
			// start mongo Threads
			for (int i=1; i<=connectionsINT; i++) {	
				MongoDataBase mongomultiuserDB = new MongoDataBase(i,testcase);
				mongos[i] = new Thread(mongomultiuserDB);
				mongos[i].start();
				threads.add(mongos[i]);
			}	
			// start couch Threads
			for (int i=1; i<=connectionsINT; i++) {	
				CouchDataBase couchmultiuserDB = new CouchDataBase(i,testcase);
				couchs[i] = new Thread(couchmultiuserDB);
				couchs[i].start();
				threads.add(couchs[i]);
			}	
			runButton.setEnabled(true);
			
		} else if (e.getActionCommand().equals("Reset DBs")) {
			if (!singleconnected) {
				initSingleDBConnection();
				singleconnected=true;
			}
			clearSummary();
			startCase(3);
		} else if (e.getActionCommand().equals("Reset Statistics")) {
			startCase(5);
		} else if (e.getActionCommand().equals("Create Spatial Index")) {
			if (!singleconnected) {
				initSingleDBConnection();
				singleconnected=true;
			}
			startCase(7);
			Performance.console.append("Indexes added\n");
		} else if (e.getActionCommand().equals("Inserts")) {
			getStat(1);
		} else if (e.getActionCommand().equals("Selects")) {
			getStat(2);
		} else if (e.getActionCommand().equals("Custom")) {
			getStat(4);
		} else if (e.getActionCommand().equals("BulkInserts")) {
			getStat(6);
		} else {
			updateGUI(e.getActionCommand());
			clearSummary();
		}
	}
	
	/**
	 * Calls a statistic dialog for the selected option
	 * @param e the statistic option
	 */
	public void getStat(int tc){
		String testcase = new String();
		switch (tc) {
					case 1: testcase="Inserts"; break;
					case 2: testcase="Selects";; break;
					case 4: testcase="Custom"; break;
					case 6: testcase="Bulk"; break;
					}	
		
		JDialog f = new StatDialog(new JFrame(), tc, testcase);
		f.setVisible(true);
		
	}
	
	/**
	 * This method calls drawSettings and sets the testcaseLOG String
	 * 
	 * @param option the action set by user
	 * 
	 */
	public void updateGUI(String option) {
		if (option.equals("INSERT test")) {
			testcaseLOG = option;
			drawSettings(1); 
			setupTask(setup, testcaseListing);
		    connectButton.setEnabled(true);
		} 
		if (option.equals("SELECT test")) {
			testcaseLOG = option;
			drawSettings(2); 
			setupTask(setup, testcaseListing);
		    connectButton.setEnabled(true);
		}
		
		if (option.equals("Bulk Load")) {
			testcaseLOG = option;
			drawSettings(6); 
			setupTask(setup, testcaseListing);
		    connectButton.setEnabled(true);
		}
		
		if (option.equals("Connect to...")) {
			testcase = 4;
			ConnectToDialog connectdialog = new ConnectToDialog();
			connectdialog.setVisible(true);
		}
		if (option.equals("Connect now...")) {
			testcase = 4;
			ConnectToDialog connectdialog = new ConnectToDialog();
			connectdialog.setVisible(true);
		}
		if (option.equals("Open Testcase")) {
			testcaseLOG="CUSTOM";
			openTestcase();
		}
		if (option.equals("Open File...")) {
			testcaseLOG="CUSTOM";
			openTestcase();
		}
	}
    
	
    /**
     * Method refreshes the GUI and draws everything needed for the selected testcase.<br>
     * 
     * @param testcase defines which testcase will be set. The following numbers define which testcase is meant:<br><br>
     * <ul>
     * <li>1 - INSERT
     * <li>2 - SELECT
     * <li>4 - EXTERN
     * <li>6 - FILL_DB
     * </ul>
     * <br>
     * @return a fully validated JPanel for the selected testcase
     */
	public static JPanel drawSettings(int testcase) {
	
		settingsPanel.removeAll();
		Performance.testcase = testcase;
		setup = testcase;
		testcaseString = setTestcase(testcase);
		setupButton.setEnabled(true);
		multithreadedText.setText(connections);
		testcaseField = "You have selected the "+testcaseString+" Testcase\nPlease reset the DBs and click Setup to continue!";
		connectedText.setForeground(Color.RED);
		
		if (testcase == 4) {
			if (!connected)
				runButton.setEnabled(false);
			iterationsText.setText(""+externaliterations);
			bottomPanelLeft.removeAll();
			bottomPanelLeft.setLayout(new BoxLayout(bottomPanelLeft, BoxLayout.LINE_AXIS));
			bottomPanelLeft.add(Box.createHorizontalStrut(200));
			bottomPanelLeft.add(setupButton);
			if (!open){
				setupButton.setText("Open File...");
				setupButton.setVisible(true);
				bottomPanelLeft.revalidate();
				
			}
			else
				setupButton.setVisible(false);
			serverText.setText(dbserver);
			nameText.setText(dbname);
			mserverText.setText(mongodbserver);
			mnameText.setText(mongodbname);
			cserverText.setText(couchdbserver);
			cnameText.setText(couchdbname);
			
			JPanel server = new JPanel();
			server.setLayout(new BoxLayout(server, BoxLayout.LINE_AXIS));
			server.add(serverLabel);
			server.add(serverText);
			server.add(Box.createHorizontalGlue());
			server.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel mserver = new JPanel();
			mserver.setLayout(new BoxLayout(mserver, BoxLayout.LINE_AXIS));
			mserver.add(mserverLabel);
			mserver.add(mserverText);
			mserver.add(Box.createHorizontalGlue());
			mserver.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel cserver = new JPanel();
			cserver.setLayout(new BoxLayout(cserver, BoxLayout.LINE_AXIS));
			cserver.add(cserverLabel);
			cserver.add(cserverText);
			cserver.add(Box.createHorizontalGlue());
			cserver.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel dbname = new JPanel();
			dbname.setLayout(new BoxLayout(dbname, BoxLayout.LINE_AXIS));
			dbname.add(nameLabel);
			dbname.add(nameText);
			dbname.add(Box.createHorizontalGlue());
			dbname.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel mdbname = new JPanel();
			mdbname.setLayout(new BoxLayout(mdbname, BoxLayout.LINE_AXIS));
			mdbname.add(mnameLabel);
			mdbname.add(mnameText);
		
			mdbname.add(Box.createHorizontalGlue());
			mdbname.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel cdbname = new JPanel();
			cdbname.setLayout(new BoxLayout(cdbname, BoxLayout.LINE_AXIS));
			cdbname.add(cnameLabel);
			cdbname.add(cnameText);
			cdbname.add(Box.createHorizontalGlue());
			cdbname.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel connected = new JPanel();
			connected.setLayout(new BoxLayout(connected, BoxLayout.LINE_AXIS));
			
			connected.add(connectedLabel);
			if (dbserver.equals("")||mongodbserver.equals("")||couchdbserver.equals("")) {
				connectedText.setText("connections not established ");
				connected.add(connectedText);
			} else {
				connectedText.setText("connections established");
				connectedText.setForeground(Color.GREEN);
				connected.add(connectedText);
			}			
			connected.add(Box.createHorizontalGlue());
			connected.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel iterations = new JPanel();
			iterations.setLayout(new BoxLayout(iterations, BoxLayout.LINE_AXIS));
			iterations.add(iterationsLabel);
			iterations.add(iterationsText);
			iterations.add(Box.createHorizontalGlue());
			iterations.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			JPanel users = new JPanel();
			users.setLayout(new BoxLayout(users, BoxLayout.LINE_AXIS));;
			users.add(multithreadedLabel);
			users.add(multithreadedText);
			users.add(Box.createHorizontalGlue());
			users.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			
			multithreadedText.setText(Integer.toString(externalconnections));
			
			JPanel topLeftPanel = new JPanel();
			topLeftPanel.setLayout(new BoxLayout(topLeftPanel, BoxLayout.PAGE_AXIS));
			topLeftPanel.setPreferredSize(new Dimension(450, 250));//330,150
			topLeftPanel.add(server);
			topLeftPanel.add(dbname);
			topLeftPanel.add(mserver);
			topLeftPanel.add(mdbname);
			topLeftPanel.add(cserver);
			topLeftPanel.add(cdbname);
			topLeftPanel.add(iterations);
			topLeftPanel.add(users);
			topLeftPanel.add(connected);
			if (connectedText.getText().equals("connections not established "))
				topLeftPanel.add(connectNowButton);
			else
				if (open)
					runButton.setEnabled(true);
			
			JPanel topRightPanel = new JPanel();
			topRightPanel.setLayout(new BoxLayout(topRightPanel, BoxLayout.PAGE_AXIS));
			topRightPanel.add(Box.createVerticalStrut(110));
			
			JPanel topPanel = new JPanel();
			topPanel.setLayout(new BoxLayout(topPanel, BoxLayout.LINE_AXIS));		
			topPanel.add(topLeftPanel);
			topPanel.add(topRightPanel);
			
			JPanel testcaseTitle = new JPanel();
			testcaseTitle.setLayout(new BoxLayout(testcaseTitle, BoxLayout.LINE_AXIS));
			testcaseTitle.add(testcaseLabel);
			testcaseTitle.add(Box.createHorizontalGlue());
			testcaseTitle.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

			JPanel testcaseLabel = new JPanel();
			testcaseLabel.setLayout(new BoxLayout(testcaseLabel, BoxLayout.LINE_AXIS));
			testcaseLabel.add(testcaseListing);
		
			testcaseListing.setEditable(false);
			if (!open)
				testcaseListing.setText("");
	
			testcaseLabel.add(Box.createHorizontalGlue());
			testcaseLabel.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
			JScrollPane scrollableTestcaseArea = new JScrollPane(testcaseLabel);

			settingsPanel.setLayout(new BoxLayout(settingsPanel, BoxLayout.PAGE_AXIS));
			settingsPanel.add(topLeftPanel);
			settingsPanel.add(testcaseTitle);
			settingsPanel.add(scrollableTestcaseArea);
			
		} else {
			if (!singleconnected) {
				db.connect();
			}
		strings = db.getTableStructure("generictable");
		if (!singleconnected)
			db.disconnect();
		
		iterationsText.setText(test_properties.getProperty(testcaseString));
		
		JPanel table = new JPanel();
		JPanel tablesize = new JPanel();
		JPanel multithreaded = new JPanel();			
			
		table.setLayout(new BoxLayout(table, BoxLayout.LINE_AXIS));
		table.add(Box.createHorizontalGlue());
		table.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		tablesize.setLayout(new BoxLayout(tablesize, BoxLayout.LINE_AXIS));
		tablesize.add(Box.createHorizontalGlue());
		tablesize.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		multithreaded.setLayout(new BoxLayout(multithreaded, BoxLayout.LINE_AXIS));
		multithreaded.add(multithreadedLabel);
		multithreaded.add(multithreadedText);
		multithreaded.add(Box.createHorizontalGlue());
		multithreaded.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel iterationsTitle = new JPanel();
		iterationsTitle.setLayout(new BoxLayout(iterationsTitle, BoxLayout.LINE_AXIS));
		iterationsTitle.add(iterationsLabel);
		iterationsTitle.add(iterationsText);
		iterationsTitle.add(Box.createHorizontalGlue());
		iterationsTitle.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel topLeftPanel = new JPanel();
		topLeftPanel.setLayout(new BoxLayout(topLeftPanel, BoxLayout.PAGE_AXIS));
		
		topLeftPanel.setPreferredSize(new Dimension(200, 150));
		topLeftPanel.add(tablesize);
		topLeftPanel.add(iterationsTitle);
		topLeftPanel.add(multithreaded);
		
		JPanel topRightPanel = new JPanel();
		topRightPanel.setLayout(new BoxLayout(topRightPanel, BoxLayout.PAGE_AXIS));
		
		JPanel topPanel = new JPanel();
		topPanel.setLayout(new BoxLayout(topPanel, BoxLayout.LINE_AXIS));		
		topPanel.add(topLeftPanel);
		topPanel.add(topRightPanel);
		
		JPanel tablestructure = new JPanel();
		tablestructure.setLayout(new BoxLayout(tablestructure, BoxLayout.LINE_AXIS));
		tablestructure.add(structureLabel);
		tablestructure.add(Box.createHorizontalGlue());
		tablestructure.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		//begin setup for the JTable containing the table structure
		Object[][] data = new Object[strings.size()] [4]; 
		
		for (int c=0; c<strings.size(); c++) {
			for (int d=1; d<4; d++) {
				data[c][d-1] = getString(strings, c, d);
			}
		}

		JTable tabelle = new JTable(data, columnNames);
		JScrollPane struct = new JScrollPane(tabelle);		
		tabelle.setEnabled(false);
		//end setup of table
		
		JPanel testcaseTitle = new JPanel();
		testcaseTitle.setLayout(new BoxLayout(testcaseTitle, BoxLayout.LINE_AXIS));
		testcaseTitle.add(testcaseLabel);
		testcaseTitle.add(Box.createHorizontalGlue());
		testcaseTitle.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		JPanel testcaseLabel = new JPanel();
		testcaseLabel.setLayout(new BoxLayout(testcaseLabel, BoxLayout.LINE_AXIS));
		testcaseListing.setEditable(false);
		testcaseLabel.add(testcaseListing);
		testcaseListing.setText("");
		testcaseListing.append(testcaseField);
		testcaseLabel.add(Box.createHorizontalGlue());
		testcaseLabel.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		JScrollPane scrollableTestcaseArea = new JScrollPane(testcaseLabel);

		settingsPanel.setLayout(new BoxLayout(settingsPanel, BoxLayout.PAGE_AXIS));
		settingsPanel.add(topPanel);
		settingsPanel.add(tablestructure);
		settingsPanel.add(struct);
		settingsPanel.add(testcaseTitle);
		settingsPanel.add(scrollableTestcaseArea);
	}
		settingsPanel.revalidate();
		
		return settingsPanel;
		
	}
	
    /**
	* Method converts the testcase number into a readable String version.
	* 
	* @param testcase defines the String testcase will be set. The following numbers define which testcase is meant:<br><br>
	* <ul>
    	* <li>1 - INSERT
     	* <li>2 - SELECT
     	* </ul>
     	* <br>
	 * @return the testcase in a readable String version
	 */
	public static String setTestcase(int testcase) {
		String result = new String();
		switch (testcase) {
		case 1: 
			result = "insert";
			return result;
		case 2: 
			result = "select";
			return result; 
		case 6: 
			result = "insert";
			return result; 
		}
		
		return result;
	}
	
	
	
	
	
	/**
	 * The entry point for the application.<br>
	 * Performance is initialised and set to visible,
	 * initStatDBConnection is called to connect with the statistics database 
	 * 
	 * @param args possible command line arguments
	 */
	public static void main(String[] args) {          
	       
		Performance perform = new Performance();
		perform.pack();
		perform.setVisible(true);		
		
		initStatDBConnection();
	}
	
	
	/**
	 * Starts a TestCaseWorker in a Thread with corresponding databases connections.<br>
	 * 
	 * @param testcase the testcase number
	 */
	public void startCase(int testcase) {
		TestCaseWorker executor = new TestCaseWorker(testcase, db, mongodb, couchdb, dbstats);
		Thread thread = new Thread(executor);
		thread.start();
	}
	
	/**
	 * A helper method to handle a List<String[]>
	 * 
	 * @param list the list to be worked on
	 * @param x this value is used to call list.get(x) therefore navigating on the provided list
	 * @param y this value is used to navigate on the String Array
	 * @return the value collected
	 */
	public static String getString(List<String[]> list, int x, int y){
		String[] a=list.get(x);	
		return(a[y]);
	}
	
	
	/**
	 * This method starts a SetupWorker in a Thread with the corresponding databases connections
	 * 
	 * @param i defines the setting to setup for the current testcase
	 * @param setup this is the JTextArea which is used in order to provide a sample testcase
	 */
	public void setupTask(int i, JTextArea setup) {
		SetupWorker executor = new SetupWorker(i, setup, db, mongodb, couchdb);
		Thread thread = new Thread(executor);
		thread.start();
	
	}
	
	
	
	/**
	 * Updates certain GUI Elements from the summaryPanel.
	 * 
	 * @param postgres, mongo and couch (time in mseconds)
	 */
	public static void updateSummary(long postgres, long mongo, long couch) {
		try {
			postgresText.setForeground(Color.RED);
			mongoText.setForeground(Color.RED);
			couchText.setForeground(Color.RED);
			if (postgres>0)
				postgresText.setText(""+postgres+" ms");
			if (mongo>0)
				mongoText.setText(""+mongo+" ms");
			couchText.setText(""+couch+" ms");
			summaryPanel.revalidate();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Initialises a single database connection for the test databases
	 */
	public static void initSingleDBConnection() {
		try {
			if (!db.connect()||!mongodb.connect()||!couchdb.connect()) {
				System.err.println("There was an error connecting to the databases");
				Performance.console.append("There was an error connecting to the database\n");
			} else {
				System.out.println("Database connections established");
				Performance.console.append("Database connections established\n");
				singleconnected=true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialises a single database connection for the statistics database
	 */
	public static void initStatDBConnection() {
		try {
			if (!dbstats.connect()) {
				System.err.println("There was an error connecting to the statistics database");
				Performance.console.append("There was an error connecting to the statistics database\n");
			} else {
				System.out.println("Statistics database connection established");
				Performance.console.append("Statistics database connection established\n");				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Sets values for the custom benchmark.<br>
	 * 
	 * @param pdbserver the postgreSQL server to connect to
	 * @param mdbserver the mongoDB server to connect to
	 * @param cdbserver the couchDB server to connect to
	 * @param pdbname the name of the PostgreSQL database
	 * @param mdbname the name of the MongoDB database
	 * @param cdbname the name of the CouchDB database
	 */
	public static void setExternalInfo(String pdbserver, String pdbname, String mdbserver, String mdbname,String cdbserver, String cdbname) {
		dbserver = pdbserver;
		dbname = pdbname;
		mongodbserver = mdbserver;
		mongodbname = mdbname;
		couchdbserver = cdbserver;
		couchdbname = cdbname;
    	
	}
	
	/**
	 * Defines whether there is an external connection for a custom testcase or not
	 * 
	 * @param b true or false
	 */
	public static void setExternaConnection(boolean b) {
		Performance.EXTERNAL_CONNECTION = b;		
	}
	
	/**
	 * Starts a dialog to choose and load the correct testcase file (*.testcase)
	 */
	public void openTestcase() {
		try {
			clearSummary();
		fc.addChoosableFileFilter(new TestCaseFilter());
		int returnVal = fc.showOpenDialog(this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
        	fileloader = new FileLoader(fc.getSelectedFile());
        	testcaseName=fc.getName(fc.getSelectedFile());
        	Thread.sleep(500);
        	open=true;
        	drawSettings(4);
        	Thread.sleep(1000);
        	setupTask(setup, testcaseListing);
	        removeFileChoosers();
        } else {
        	removeFileChoosers();
        	}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * A helper method to remove all ChoosableFileFilters.<br>
	 */
	public void removeFileChoosers() {
    	FileFilter[] filterlist = fc.getChoosableFileFilters();
    	for (int i = 0; i<filterlist.length; i++) {
    		fc.removeChoosableFileFilter(filterlist[i]);
    	}
	}
	
	/**
	 * restarts the application
	 * 
	 */
	public static void restartApplication(Runnable runBeforeRestart) throws IOException {  
	    try {  
	    	
	        // java binary  
	        String java = System.getProperty("java.home") + "/bin/java";  
	        // vm arguments  
	        List<String> vmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();  
	        StringBuffer vmArgsOneLine = new StringBuffer();  
	        for (String arg : vmArguments) {   
	            if (!arg.contains("-agentlib")) {  
	                vmArgsOneLine.append(arg);  
	                vmArgsOneLine.append(" ");  
	            }  
	        }  
	        // init the command to execute, add the vm args   
	        final StringBuffer cmd = new StringBuffer(java + " " + vmArgsOneLine);  
	        // program main and program arguments  
	        String[] mainCommand = System.getProperty(SUN_JAVA_COMMAND).split(" ");  
	        // program main is a jar  
	        if (mainCommand[0].endsWith(".jar")) {  
	            // if it's a jar, add -jar mainJar  
	            cmd.append("-jar " + new File(mainCommand[0]).getPath());  
	        } else {  
	            // else it's a .class, add the classpath and mainClass  
	            cmd.append("-cp \"" + System.getProperty("java.class.path") + "\" " + mainCommand[0]); 
	        }  
	        // finally add program arguments  
	        for (int i = 1; i < mainCommand.length; i++) {  
	            cmd.append(" ");  
	            cmd.append(mainCommand[i]);  
	        }  
	        // execute the command in a shutdown hook, to be sure that all the  
	        // resources have been disposed before restarting the application  
	        
	        Runtime.getRuntime().addShutdownHook(new Thread() {  
	            @Override  
	            public void run() {  
	                try {  
	                	Process procompile=Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", cmd.toString()}); 
	                	try {
							procompile.waitFor();
						} catch (InterruptedException e) {
						
							e.printStackTrace();
						}
	            
	                } catch (IOException e) {  
	                    e.printStackTrace();  
	                } 
	            }  
	        });  
	
	        if (runBeforeRestart!= null) {  
	            runBeforeRestart.run();  
	        }  
	    
	       System.exit(0);  
	       
	    } catch (Exception e) {  
	        throw new IOException("Error while trying to restart the application", e);  
	    }  
	}  
	
	
	/**
	 * Adds a point with the provided x and y values to the ITrace2D, which is then displayed on the Chart2D.
	 * 
	 * @param x x coordinate
	 * @param y y coordinate
	 */
	
	
	public static void addValue(double x, double y, ITrace2D tr){
		TracePoint2D point = new TracePoint2D(x, y);
		tr.addPoint(point);
	}
	
	
	/**
	 * Clear the Chart by calling trace.removeAllPoints()<br>
	 * Consequently all trace points are removed
	 */
	public static void clearGraph(){
		trace.removeAllPoints();
	}
	
	

	
}
