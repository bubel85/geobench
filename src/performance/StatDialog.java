package performance;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

/*
 * The class for a statistics window  
 */ 
public class StatDialog extends JDialog {

	private static final long serialVersionUID = 1L;
	public List<String[]> strings2  = new ArrayList<String[]> ();
	public JTable stattabelle;
	public JScrollPane statstruct;
	public StatDialog(JFrame parent, int tc, String testcase) {
    super(parent, "Statistics: "+ testcase, true);

    Box b = Box.createVerticalBox();

	strings2=Performance.dbstats.getStat(tc);

	Object[][] statdata = new Object[strings2.size()] [8]; 
	
	for (int c=0; c<strings2.size(); c++) {
		for (int d=1; d<8; d++) {
			statdata[c][d-1] = Performance.getString(strings2, c, d);
		}
	}
	try {
	
	} catch (Exception ex) {
		ex.printStackTrace();
		
	}
	stattabelle = new JTable(statdata, Performance.columnNames2);
	statstruct = new JScrollPane(stattabelle);		
	stattabelle.setEnabled(false);
	
	statstruct.setPreferredSize(new Dimension(580, 290));	
	
    b.add(statstruct);
    getContentPane().add(b, "Center");

    JPanel p2 = new JPanel();
    JButton ok = new JButton("Ok");
    p2.add(ok);
    getContentPane().add(p2, "South");
    
	Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
	this.setLocation((dim.width - getWidth()) / 2, (dim.height - getHeight()) / 2);
	setSize(585, 296);

    ok.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent evt) {
    	  dispose();
        setVisible(false);
      }
    });
  }
}
