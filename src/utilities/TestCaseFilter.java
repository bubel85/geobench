
package utilities;

import java.io.File;
import javax.swing.filechooser.*;

public class TestCaseFilter extends FileFilter {

    public boolean accept(File f) {
        if (f.isDirectory()) {
            return true;
        }

        String extension = Extension.getExtension(f);
        if (extension != null) {
            if (extension.equals(Extension.testcase)) {
                    return true;
            } else {
                return false;
            }
        }

        return false;
    }

    //The description of this filter
    public String getDescription() {
        return "Testcase File";
    }
}
