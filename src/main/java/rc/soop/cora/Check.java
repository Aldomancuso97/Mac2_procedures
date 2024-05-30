/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.cora;

import com.google.common.base.Splitter;
import java.util.Iterator;
import static rc.soop.cora.CORA.formatStringtoStringDate_null;
import static rc.soop.cora.CORA.pattermonthnorm;
import static rc.soop.cora.CORA.patternmonthsql;
import static rc.soop.cora.CORA.patternsql;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.aggiornamenti.oggettoFile;

/**
 *
 * @author rcosco
 */
public class Check {

    public static void main(String[] args) {

//        FileReader fr = null;
//        BufferedReader br = null;
        try {
//            File Marzo = new File("marzo2019.csv");
//            File Annuale = new File("2019.TXT");
//            List<String> list = new ArrayList<>();
//            fr = new FileReader(Marzo);
//            br = new BufferedReader(fr);
//            String line;
//            while ((line = br.readLine()) != null) {
//                if (line.startsWith("12")) {
//                    list.add("3" + StringUtils.substringBefore(line, ";").trim());
//                }
//            }
//            br.close();
//            fr.close();
//
//            fr = new FileReader(Annuale);
//            br = new BufferedReader(fr);
//            String line2;
//            while ((line2 = br.readLine()) != null) {
////                if()
////                if (line2.startsWith("12")) {
////                    list.add(StringUtils.substringBefore(line2, ";").trim());
////                }
//                if (line2.startsWith("312")) {
//                    if (list.contains(StringUtils.substring(line2, 0, 51).trim())) {
//                        System.out.println(line2);
//                    }
//                }
//            }
//            br.close();
//            fr.close();

        Db_Master db = new Db_Master();
        String path = db.getPath("temp");
        db.closeDB();
//        DateTime dtSTART = new DateTime(2021, 10, 31, 0, 0).withMillisOfDay(0);
        DateTime dtSTART = new DateTime().minusMonths(1);
        
        
        
        

        String from = dtSTART.toString(pattermonthnorm);
        System.out.println("com.fl.upload.GeneraFile.main(START) " + pattermonthnorm);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsql);
        oggettoFile out = null;
        String rif = formatStringtoStringDate_null(from, pattermonthnorm, patternmonthsql);
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
                out = CORA.mensile(path, rif, f1, f2, anno, mese);
            }
        }
        if (out == null || out.getFile() == null) {
            String msg = "NULL";
            if (out != null) {
                msg = out.getErrore();
            }
            //INVIARE MAIL ERRORE
            System.out.println("ERRORE " + msg);
        } else {
            System.out.println("OK " + out.getFile().getPath());
        }
//
//        System.out.println("com.fl.upload.GeneraFile.main(END) " + new DateTime());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
