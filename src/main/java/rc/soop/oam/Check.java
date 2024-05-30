/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.oam;

import rc.soop.aggiornamenti.oggettoFile;
import com.google.common.base.Splitter;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import static rc.soop.oam.OAM.creaFile;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class Check {

    public static void main(String[] args) {

        Db_Master db0 = new Db_Master();
        String path = db0.getPath("temp");
        db0.closeDB();

        DateTime dt = new DateTime().minusMonths(1);
        Iterable<String> parameters = Splitter.on("/").split(dt.toString("MM/yyyy"));
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
            ArrayList<String[]> nations = db.country();
            ArrayList<String[]> tipodoc = db.identificationCard();
            ArrayList<Branch> allbr = db.list_branch_enabledB();
            ArrayList<Currency> curlist = db.list_figures();
            ArrayList<Ch_transaction> tran = db.list_transaction_oam(anno, mese);
            db.closeDB();

            String name1 = "OAM_CHECK_" + anno + mese + randomAlphanumeric(15).trim().toLowerCase() + ".txt";
            oggettoFile og1 = creaFile(progressivo, codicetrasm, anno, mese, "0", cfpi, denom,
                    comune, provincia, tipologia, controllo, tran, nations, curlist, allbr, tipodoc, path + name1);

            File out1 = og1.getFile();

            if (out1 == null) {
                //INVIARE MAIL ERRORE
                System.err.println("ERRORE : " + og1.getErrore());
            } else {
                System.out.println("OK " + og1.getFile());
            }
        }

        System.out.println("com.fl.upload.GeneraFile.main(END) " + new DateTime());

    }
}
