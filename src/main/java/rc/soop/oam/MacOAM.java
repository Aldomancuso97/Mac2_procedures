/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.oam;

import rc.soop.aggiornamenti.oggettoFile;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import static rc.soop.oam.OAM.creaFile;
import static rc.soop.oam.OAM.getStringBase64;
import static rc.soop.oam.OAM.patternsqldate;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import static rc.soop.cora.CORA.pattermonthnorm;

/**
 *
 * @author rcosco
 */
public class MacOAM {

    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";
    private static final ResourceBundle rb = ResourceBundle.getBundle("oam.conf");

    public static final Logger log = createLog("Mac2.0_OAM_", rb.getString("path.log"), pattern4);

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
        }
        return LOGGER;
    }

    public static void main(String[] args) {
        engine();
    }

    public static void engine() {
        log.warning("STARTING...");
        DateTime dtSTART1 = new DateTime().minusMonths(1);
        String from1 = dtSTART1.toString(pattermonthnorm);
        log.log(Level.WARNING, "MENSILE: {0}", from1);
        Iterable<String> parameters = Splitter.on("/").split(from1);
        Iterator<String> it = parameters.iterator();
        if (it.hasNext()) {
            String codicetrasm = "OPMEN";
            String mese = it.next();
            String anno = it.next();
            String cfpi = "12951210157";
            String denom = "Maccorp Italiana";
            String comune = "Milano";
            String provincia = "MI";
            String controllo = "A";
            String tipologia = "1";
            int progressivo = 1;

            Db_Master db = new Db_Master();
            String path = sanitizePath(db.getPath("temp"));
            ArrayList<String[]> nations = db.country();
            ArrayList<String[]> tipodoc = db.identificationCard();
            ArrayList<Branch> allbr = db.list_branch_enabledB();
            ArrayList<Currency> curlist = db.list_figures();
            ArrayList<Ch_transaction> tran = db.list_transaction_oam(anno, mese);
            db.closeDB();

            String name1 = "OAM_0_" + anno + mese + RandomStringUtils.randomAlphanumeric(15).trim().toLowerCase()
                    + ".txt";
            oggettoFile og1 = creaFile(progressivo, codicetrasm, anno, mese, "0", cfpi, denom,
                    comune, provincia, tipologia, controllo, tran, nations, curlist, allbr, tipodoc, path + name1);

            File out1 = og1.getFile();

            if (out1 != null) {
                ArrayList<File> tozip = new ArrayList<>();
                tozip.add(out1);
                File zipped = new File(path + out1.getName() + ".zip");
                if (OAM.zipListFiles(tozip, zipped)) {
                    String base64 = getStringBase64(zipped);
                    log.log(Level.WARNING, "INSERT FILE OAM {0}", out1.getName());
                    Db_Master dbb = new Db_Master();
                    dbb.insertOAM(dtSTART1.toString(patternsqldate), base64, "0");
                    dbb.closeDB();
                }
            } else {
                // Invio di una mail in caso di errore
            }
        }

        log.warning("END...");
    }

    private static String sanitizePath(String path) {
        try {
            return Paths.get(path).normalize().toString();
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid path provided");
        }
    }

    // if (out2 != null) {
    // ArrayList<File> tozip = new ArrayList<>();
    // tozip.add(out2);
    // File zipped = new File(path + out2.getName() + ".zip");
    // if (OAM.zipListFiles(tozip, zipped)) {
    // String base64 = getStringBase64(zipped);
    // Db_Master dbb = new Db_Master();
    // dbb.insertOAM(dt.toString(patternsqldate), base64, "1");
    // dbb.closeDB();
    // }
    // }

}
