
package utilities;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertyLoader {
	private Properties properties = new Properties();
	private FileInputStream in;	
	
	public PropertyLoader() {

	}
	
	public Properties loadProperties(String propertyFile) {
		try {
			in = new FileInputStream(propertyFile);
			properties.load(in);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return properties;
	}
	
	public void close() {
		try {
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
