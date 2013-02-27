
package utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileLoader {
	protected static BufferedReader in;
	protected static String[] strings;
	protected static String file;
	protected static File tsearchFile;
	
	public FileLoader (String file) {
		try {
			in = new BufferedReader(new FileReader(file));
			FileLoader.file = file;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public FileLoader (File file) {
		try {
			in = new BufferedReader(new FileReader(file));
			FileLoader.tsearchFile = file;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized String getLine() throws IOException {
		return in.readLine();
	}
	
	public static synchronized void reset() throws IOException {
		in.close();
		in = new BufferedReader(new FileReader(FileLoader.tsearchFile));
	}
	 
	public static synchronized String[] getLine(int lines) throws IOException {
		strings = new String[lines];
		if ((strings[0] = in.readLine()) != null) {
			for (int i = 1; i<lines; i++) {
				strings[i] = in.readLine();
			}
		} else {
			strings[0] = "endofFile";
		}
		return strings;
	}
}
