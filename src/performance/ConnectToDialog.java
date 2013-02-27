
package performance;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.Border;


/* The class makes connections to custom databases
 * 
 */

public class ConnectToDialog extends JFrame implements ActionListener {
	private static final long serialVersionUID = 1L;

	public JPanel GUIPanel = new JPanel();
	public JPanel inputPanel = new JPanel();
	public JPanel buttonPanel = new JPanel();

	public JButton connectButton = new JButton("Connect");
	public JButton cancelButton = new JButton("Cancel");

	public JLabel serverLabel = new JLabel("Postgres Server: ");
	public JTextField serverField = new JTextField(20);
	public JLabel portLabel = new JLabel("Postgres Port: ");
	public JTextField portField = new JTextField(20);
	public JLabel nameLabel = new JLabel("Postgres DBName: ");
	public JTextField nameField = new JTextField(20);
	public JLabel usernameLabel = new JLabel("Username: ");
	public JTextField usernameField = new JTextField(20);
	public JLabel passwordLabel = new JLabel("Password: ");
	public JTextField passwordField = new JTextField(20);
	public JLabel connectionsLabel = new JLabel("Connections: ");
	public JTextField connectionsField = new JTextField(20);
	public JLabel iterationsLabel = new JLabel("Iterations: ");
	public JTextField iterationsField = new JTextField(20);
	
	public JLabel mserverLabel = new JLabel("Mongo Server: ");
	public JTextField mserverField = new JTextField(20);
	public JLabel mportLabel = new JLabel("Mongo Port: ");
	public JTextField mportField = new JTextField(20);
	public JLabel mnameLabel = new JLabel("Mongo DBName: ");
	public JTextField mnameField = new JTextField(20);
	public JLabel musernameLabel = new JLabel("Username: ");
	public JTextField musernameField = new JTextField(20);
	public JLabel mpasswordLabel = new JLabel("Password: ");
	public JTextField mpasswordField = new JTextField(20);
	
	public JLabel cserverLabel = new JLabel("Couch Server: ");
	public JTextField cserverField = new JTextField(20);
	public JLabel cportLabel = new JLabel("Couch Port: ");
	public JTextField cportField = new JTextField(20);
	public JLabel cnameLabel = new JLabel("Couch DBName: ");
	public JTextField cnameField = new JTextField(20);
	public JLabel cusernameLabel = new JLabel("Username: ");
	public JTextField cusernameField = new JTextField(20);
	public JLabel cpasswordLabel = new JLabel("Password: ");
	public JTextField cpasswordField = new JTextField(20);
	
	public static Thread[] ecouchs;
	public static Thread[] epostgis;
	public static Thread[] emongos;

	
	public void initGUI() {
		Border raisedbevel, loweredbevel;
		connectButton.addActionListener(this);
		cancelButton.addActionListener(this);

		raisedbevel = BorderFactory.createRaisedBevelBorder();
		loweredbevel = BorderFactory.createLoweredBevelBorder();

		JPanel server = new JPanel();
		server.setLayout(new BoxLayout(server, BoxLayout.LINE_AXIS));
		server.add(serverLabel);
		server.add(serverField);
		server.add(Box.createHorizontalGlue());
		server.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
	
		JPanel port = new JPanel();
		port.setLayout(new BoxLayout(port, BoxLayout.LINE_AXIS));
		port.add(portLabel);
		port.add(portField);
		port.add(Box.createHorizontalGlue());
		port.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		JPanel name = new JPanel();
		name.setLayout(new BoxLayout(name, BoxLayout.LINE_AXIS));
		name.add(nameLabel);
		name.add(nameField);
		name.add(Box.createHorizontalGlue());
		name.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		JPanel username = new JPanel();
		username.setLayout(new BoxLayout(username, BoxLayout.LINE_AXIS));
		username.add(usernameLabel);
		username.add(usernameField);
		username.add(Box.createHorizontalGlue());
		username.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel password = new JPanel();
		password.setLayout(new BoxLayout(password, BoxLayout.LINE_AXIS));
		password.add(passwordLabel);
		password.add(passwordField);
		password.add(Box.createHorizontalGlue());
		password.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel mserver = new JPanel();
		mserver.setLayout(new BoxLayout(mserver, BoxLayout.LINE_AXIS));
		mserver.add(mserverLabel);
		mserver.add(mserverField);
		mserver.add(Box.createHorizontalGlue());
		mserver.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel mport = new JPanel();
		mport.setLayout(new BoxLayout(mport, BoxLayout.LINE_AXIS));
		mport.add(mportLabel);
		mport.add(mportField);
		mport.add(Box.createHorizontalGlue());
		mport.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		JPanel mname = new JPanel();
		mname.setLayout(new BoxLayout(mname, BoxLayout.LINE_AXIS));
		mname.add(mnameLabel);
		mname.add(mnameField);
		mname.add(Box.createHorizontalGlue());
		mname.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel musername = new JPanel();
		musername.setLayout(new BoxLayout(musername, BoxLayout.LINE_AXIS));
		musername.add(musernameLabel);
		musername.add(musernameField);
		musername.add(Box.createHorizontalGlue());
		musername.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		musernameField.setText("");
		
		JPanel mpassword = new JPanel();
		mpassword.setLayout(new BoxLayout(mpassword, BoxLayout.LINE_AXIS));
		mpassword.add(mpasswordLabel);
		mpassword.add(mpasswordField);
		mpassword.add(Box.createHorizontalGlue());
		mpassword.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		mpasswordField.setText("");
		
		JPanel cserver = new JPanel();
		cserver.setLayout(new BoxLayout(cserver, BoxLayout.LINE_AXIS));
		cserver.add(cserverLabel);
		cserver.add(cserverField);
		cserver.add(Box.createHorizontalGlue());
		cserver.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel cport = new JPanel();
		cport.setLayout(new BoxLayout(cport, BoxLayout.LINE_AXIS));
		cport.add(cportLabel);
		cport.add(cportField);
		cport.add(Box.createHorizontalGlue());
		cport.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));

		JPanel cname = new JPanel();
		cname.setLayout(new BoxLayout(cname, BoxLayout.LINE_AXIS));
		cname.add(cnameLabel);
		cname.add(cnameField);
		cname.add(Box.createHorizontalGlue());
		cname.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		
		JPanel cusername = new JPanel();
		cusername.setLayout(new BoxLayout(cusername, BoxLayout.LINE_AXIS));
		cusername.add(cusernameLabel);
		cusername.add(cusernameField);
		cusername.add(Box.createHorizontalGlue());
		cusername.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		cusernameField.setText("");
		
		JPanel cpassword = new JPanel();
		cpassword.setLayout(new BoxLayout(cpassword, BoxLayout.LINE_AXIS));
		cpassword.add(mpasswordLabel);
		cpassword.add(mpasswordField);
		cpassword.add(Box.createHorizontalGlue());
		cpassword.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		cpasswordField.setText("");
		
		JPanel connections = new JPanel();
		connections.setLayout(new BoxLayout(connections, BoxLayout.LINE_AXIS));
		connections.add(connectionsLabel);
		connections.add(connectionsField);
		connections.add(Box.createHorizontalGlue());
		connections.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		connections.setBorder(BorderFactory.createCompoundBorder(raisedbevel, loweredbevel));
		
		JPanel iterations = new JPanel();
		iterations.setLayout(new BoxLayout(iterations, BoxLayout.LINE_AXIS));
		iterations.add(iterationsLabel);
		iterations.add(iterationsField);
		iterations.add(Box.createHorizontalGlue());
		iterations.setBorder(BorderFactory.createEmptyBorder(5,5,2,5));
		iterations.setBorder(BorderFactory.createCompoundBorder(raisedbevel, loweredbevel));
		
		JPanel postgresPanel = new JPanel();
		postgresPanel.setLayout(new BoxLayout(postgresPanel, BoxLayout.PAGE_AXIS));
		postgresPanel.add(server);
		postgresPanel.add(port);
		postgresPanel.add(name);
		postgresPanel.add(username);
		postgresPanel.add(password);
		postgresPanel.setBorder(BorderFactory.createCompoundBorder(raisedbevel, loweredbevel));
		
		JPanel mongoPanel = new JPanel();
		mongoPanel.setLayout(new BoxLayout(mongoPanel, BoxLayout.PAGE_AXIS));
		mongoPanel.add(mserver);
		mongoPanel.add(mport);
		mongoPanel.add(mname);
		mongoPanel.add(musername);
		mongoPanel.add(mpassword);
		mongoPanel.setBorder(BorderFactory.createCompoundBorder(raisedbevel, loweredbevel));
		
		JPanel couchPanel = new JPanel();
		couchPanel.setLayout(new BoxLayout(couchPanel, BoxLayout.PAGE_AXIS));
		couchPanel.add(cserver);
		couchPanel.add(cport);
		couchPanel.add(cname);
		couchPanel.add(cusername);
		couchPanel.add(cpassword);
		couchPanel.setBorder(BorderFactory.createCompoundBorder(raisedbevel, loweredbevel));
		
		
		inputPanel.setLayout(new BoxLayout(inputPanel, BoxLayout.PAGE_AXIS));
	
		inputPanel.add(postgresPanel);
		inputPanel.add(mongoPanel);
		inputPanel.add(couchPanel);
		inputPanel.add(connections);
		inputPanel.add(iterations);

		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.LINE_AXIS));
		buttonPanel.add(Box.createRigidArea(new Dimension(115,5)));
		buttonPanel.add(connectButton);
		buttonPanel.add(cancelButton);

		GUIPanel.setLayout(new BoxLayout(GUIPanel, BoxLayout.PAGE_AXIS));
		GUIPanel.add(inputPanel);
		GUIPanel.add(buttonPanel);

		// Main window layout
		GridBagConstraints gbc;
		GridBagLayout gbMain = new GridBagLayout();
		setLayout(gbMain);

		gbc = makegbc(1, 1, 1, 1);
		gbc.fill = GridBagConstraints.HORIZONTAL;
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbMain.setConstraints(GUIPanel, gbc);
		add(GUIPanel);

		pack();	

		setDefaultCloseOperation( JFrame.DISPOSE_ON_CLOSE ); 
		setTitle("Connect to Databases");
		setResizable(false);
		setBackground(Color.lightGray);

		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
		this.setLocation((dim.width - getWidth()) / 2, (dim.height - getHeight()) / 2);


	}

	private GridBagConstraints makegbc(int x, int y, int width, int height) {
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.gridx = x;
		gbc.gridy = y;
		gbc.gridwidth = width;
		gbc.gridheight = height;
		gbc.insets = new Insets(1, 1, 1, 1);
		return gbc;
	}

	public void actionPerformed(ActionEvent e) {
		if (e.getActionCommand().equals("Cancel")) {
			setVisible(false);
		} else 
			if (e.getActionCommand().equals("Connect")) {
				Performance.setExternaConnection(true);
				Performance.externalconnections=Integer.valueOf(connectionsField.getText());
				Performance.externaliterations=Integer.valueOf(iterationsField.getText());
				emongos  = new Thread[Integer.valueOf(connectionsField.getText())+1];
				ecouchs = new Thread[Integer.valueOf(connectionsField.getText())+1];
				epostgis = new Thread[Integer.valueOf(connectionsField.getText())+1];
				
				Performance.threadbarrierextern=new ThreadReadyBarrier(3*Performance.externalconnections);;
				Performance.threadconnectionbarrierextern=new ThreadConnectionBarrier(3*Performance.externalconnections);
				

				Performance.setupButton.setEnabled(false);
				Performance.threadconnectionbarrierextern.reset();
				Performance.threadbarrierextern.reset();
				Performance.connectButton.setEnabled(false);
			 
				Performance.threads.clear();

				
				for (int i=1; i<=Performance.externalconnections; i++) {
					PostgresDataBase multiuserDB = new PostgresDataBase(serverField.getText(), portField.getText(), nameField.getText(), usernameField.getText(), passwordField.getText(), i, Performance.testcase);
					epostgis[i] = new Thread(multiuserDB);
					epostgis[i].start();
					Performance.threads.add(epostgis[i]);
	
				}	

					
						
				
				for (int i=1; i<=Performance.externalconnections; i++) {	
					MongoDataBase mongomultiuserDB = new MongoDataBase(mserverField.getText(), mportField.getText(), mnameField.getText(), musernameField.getText(), mpasswordField.getText(),i, Performance.testcase);
					emongos[i] = new Thread(mongomultiuserDB);
					emongos[i].start();
					Performance.threads.add(emongos[i]);
		
				}	
				
					
				for (int i=1; i<=Performance.externalconnections; i++) {	
					CouchDataBase couchmultiuserDB = new CouchDataBase(cserverField.getText(), cportField.getText(),cnameField.getText(), cusernameField.getText(), cpasswordField.getText(),i, Performance.testcase);
					ecouchs[i] = new Thread(couchmultiuserDB);
					ecouchs[i].start();
					Performance.threads.add(ecouchs[i]);
		
				}	
					
				Performance.setExternalInfo(serverField.getText(), nameField.getText(), mserverField.getText(), mnameField.getText(),cserverField.getText(), cnameField.getText());
					
				if(Performance.open)
					Performance.runButton.setEnabled(true);
		
				Performance.drawSettings(4);
				Performance.connected=true;
			}
		
		setVisible(false);
	}

	public ConnectToDialog() {
		try {
			initGUI();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
