/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.qlik;

/*
 *
 * @author Raffaele
 */
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggerNew {

    public Logger log;
    FileHandler fileTxt;

    public LoggerNew(String appl, String logpath) {
        try {
            Date d = new Date();
            String dataOdierna = (new SimpleDateFormat("ddMMyyyy")).format(d);
            String ora = (new SimpleDateFormat("HHmmss")).format(d);
            
            // Sanitizzazione del percorso
            File dir1 = new File(sanitizePath(logpath));
            dir1.mkdirs();
            File dir2 = new File(dir1.getPath() + File.separator + dataOdierna);
            dir2.mkdirs();
            
            log = Logger.getLogger(appl);
            SimpleFormatter formatterTxt;
            fileTxt = new FileHandler(dir2.getPath() + File.separator + appl + "_" + ora + ".log");
            formatterTxt = new SimpleFormatter();
            fileTxt.setFormatter(formatterTxt);
            log.addHandler(fileTxt);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    // Metodo per sanificare il percorso
    private static String sanitizePath(String path) {
        return path.replaceAll("[^a-zA-Z0-9-_./]", "");
    
    }
    

}
