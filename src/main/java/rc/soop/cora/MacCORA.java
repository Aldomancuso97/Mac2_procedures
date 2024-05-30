/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.cora;

import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import static rc.soop.cora.CORA.getStringBase64;
import static rc.soop.cora.CORA.patternsqldate;
import static rc.soop.cora.CORA.zipListFiles;
import static rc.soop.esolver.Util.formatStringtoStringDate_null;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.rilasciofile.Utility.pattermonthnorm;
import static rc.soop.rilasciofile.Utility.patternmonthsql;
import java.util.Iterator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.nio.file.*;

/**
 *
 * @author rcosco
 */
public class MacCORA {

    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";
    public static final ResourceBundle rb = ResourceBundle.getBundle("cora.conf");
    public static final Logger log = createLog("Mac2.0_CORA_", rb.getString("path.log"), pattern4);
    private static final String ALLOWED_DIR = "/opt/app/temp";


    private static Logger createLog(String appname, String folderini, String patterndatefolder) {
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + dt.toString(pattern3) + ".log";
            File dirING = new File(folderini);
            dirING.mkdirs();
            if (patterndatefolder != null) {
                File dirINGNew = new File(dirING.getPath() + File.separator + dt.toString(patterndatefolder));
                dirINGNew.mkdirs();
                filename = dirINGNew.getPath() + File.separator + filename;
            } else {
                filename = dirING.getPath() + File.separator + filename;
            }
            Handler fileHandler = new FileHandler(filename);
            LOGGER.addHandler(fileHandler);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return LOGGER;
    }

    public static void generamensile() {
        Db_Master db = new Db_Master();
        String path = sanitizePath(db.getPath("temp"));
        db.closeDB();
        DateTime dtSTART1 = new DateTime().minusMonths(1);
        String from1 = dtSTART1.toString(pattermonthnorm);
        log.log(Level.WARNING, "MENSILE: {0}", from1);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsql);
        File out1 = null;
        String rif = formatStringtoStringDate_null(from1, pattermonthnorm, patternmonthsql);
        if (rif != null) {
            Iterable<String> parameters = Splitter.on("-").split(rif);
            Iterator<String> it = parameters.iterator();
            if (it.hasNext()) {
                String anno = it.next();
                String mese = it.next();
                String f1 = null;
                String f2 = null;
                String primomese = "01";
                String primogiorno = "01";
                if (!mese.equals(primomese)) {
                    DateTime dt = formatter.parseDateTime(rif + "-01").minusMonths(1);
                    int ultimom = dt.monthOfYear().get();
                    String ultimomese;
                    if (ultimom < 10) {
                        ultimomese = "0" + ultimom;
                    } else {
                        ultimomese = "" + ultimom;
                    }
                    String ultimogiorno = "" + dt.dayOfMonth().withMaximumValue().getDayOfMonth();
                    f1 = anno + "-" + primomese + "-" + primogiorno;
                    f2 = anno + "-" + ultimomese + "-" + ultimogiorno;
                }
                out1 = CORA.mensile(path, rif, f1, f2, anno, mese).getFile();
            }
        }
        if (out1 != null) {
            ArrayList<File> tozip = new ArrayList<>();
            tozip.add(out1);
            File zipped = new File(path, out1.getName() + ".zip");
            if (zipListFiles(tozip, zipped)) {
                String base64 = getStringBase64(zipped);
                log.log(Level.WARNING, "INSERT FILE CORA {0}", out1.getName());
                Db_Master dbb = new Db_Master();
                dbb.insertCORA(dtSTART1.toString(patternsqldate), base64, "0");
                dbb.closeDB();
            }
        }
    }

    private static String sanitizePath(String path) {
        // Ensure the path is absolute and normalize it
        Path normalizedPath = Paths.get(path).normalize().toAbsolutePath();
        // Ensure the path is within the allowed directory
        Path allowedDir = Paths.get(ALLOWED_DIR).toAbsolutePath();
        if (!normalizedPath.startsWith(allowedDir)) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        return normalizedPath.toString();
    }

    public static void generaannuale() {
        Db_Master db = new Db_Master();
        String path = sanitizePath(db.getPath("temp")); 
        db.closeDB();
        DateTime dtSTART = new DateTime().minusYears(1);
        String year = dtSTART.toString("yyyy");
        log.log(Level.WARNING, "ANNUALE: {0}", year);
        File out = CORA.annuale_TXT(year);
        log.warning("ENDING...");
        if (out != null) {
            ArrayList<File> tozip = new ArrayList<>();
            tozip.add(out);
            File zipped = new File(path, out.getName() + ".zip"); 
            if (zipListFiles(tozip, zipped)) {
                String base64 = getStringBase64(zipped);
                log.log(Level.WARNING, "INSERT FILE CORA {0}", out.getName());
                Db_Master dbb = new Db_Master();
                dbb.insertCORA(dtSTART.toString(patternsqldate), base64, "1");
                dbb.closeDB();
            }
        }
    }

    public static void main(String[] args) {
        generaannuale();
    }
//        int scelta;
//        try {
//            scelta = Integer.parseInt(args[0]);
//        } catch (Exception e) {
//            scelta = 1;
//        }
//
//        log.warning("STARTING...");
//        switch (scelta) {
//            case 1:
//                generamensile();
//                break;
//            case 2:
//                generaannuale();
//                break;
//            default:
//                log.severe("ERROR. NESSUNA SCELTA.");
//        }
//
//        log.warning("END...");
//    }
}
