/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.testarea;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import rc.soop.aggiornamenti.Db;
import static rc.soop.aggiornamenti.Mactest.host_PROD;
import static rc.soop.aggiornamenti.Mactest.host_PROD_CZ;
import rc.soop.aggiornamenti.StatusBranch;
import rc.soop.esolver.Branch;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import rc.soop.riallinea.Util;
import rc.soop.rilasciofile.ControlloGestione;
import rc.soop.rilasciofile.DatabaseCons;
import rc.soop.rilasciofile.GeneraFile;
import static rc.soop.rilasciofile.Utility.patternmonthsql;
import rc.soop.start.Central_Branch;
import static rc.soop.start.Utility.createLog;
import static rc.soop.start.Utility.pattern4;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class test {

    public static void main(String[] args) {

        try {
            GeneraFile gf = new GeneraFile();
            gf.setIs_IT(true);
            gf.setIs_UK(false);
            gf.setIs_CZ(false);
            DatabaseCons db = new DatabaseCons(gf);
            ArrayList<Branch> allenabledbr = db.list_branch();
            ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
//         
            DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
            DateTime ieri = new DateTime().minusDays(1);
            String datecreation = new DateTime().withZone((DateTimeZone.forID("Europe/Rome"))).toString("yyyyMMddHHmmss");
//        DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue();
//        System.out.println("com.fl.upload.GeneraFile.rilasciafile() "+);
//            String mesemysql = iniziomese.toString(patternmonthsql);
//
//            String meseriferimento = iniziomese.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
//            String annoriferimento = iniziomese.year().getAsText(Locale.ITALY).toUpperCase();

            String data1 = iniziomese.toString(patternsql);
            String data2 = ieri.toString(patternsql);

            String nomereport = "MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL N1 DA " + data1 + " A " + data2 + "_" + datecreation + ".xlsx";

            File Output = new File("C:\\mnt\\temp\\" + nomereport);
            String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, true, allenabledbr, db);

//            String nomereport = "CASHIER OPENCLOSE ERRORS DA " + data1 + " A " + data2 + "_" + datecreation + ".xlsx";
//            File Output = new File("C:\\mnt\\temp\\" + nomereport);
//            String base64 = ControlloGestione.C_OpenCloseError(Output, br1, data1, data2, allenabledbr, db);
            System.out.println("rc.soop.testarea.test.main() " + Output.getPath());

//            String path = db.getPath("temp");
//            DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
//            DateTime ieri = new DateTime(2023, 3, 3, 0, 0);
//            String datecreation = new DateTime().withZone((DateTimeZone.forID("Europe/Rome"))).toString("yyyyMMddHHmmss");
////        DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue();
////        System.out.println("com.fl.upload.GeneraFile.rilasciafile() "+);
////            String mesemysql = iniziomese.toString(patternmonthsql);
//            ArrayList<Branch> allenabledbr = db.list_branch();
//            ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
////            String meseriferimento = iniziomese.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
////            String annoriferimento = iniziomese.year().getAsText(Locale.ITALY).toUpperCase();
//
//            String data1 = iniziomese.toString(patternsql);
//            String data2 = ieri.toString(patternsql);
//
//            String nomereport = "MANAGEMENT CONTROL - REPORT CHANGE ACCOUNTING N1 DA " + data1 + " A " + data2 + "_" + datecreation + ".xlsx";
//            File Output = new File(path + nomereport);
//            String base64 = ControlloGestione.management_change_accounting1(Output, br1,
//                    iniziomese, ieri, allenabledbr, db);
            db.closeDB();
            System.out.println("rc.soop.testarea.test.main() " + Output.getPath());
//            List<String> dafare = new ArrayList<>();
//            dafare.add("033");
//            dafare.add("047");
//            dafare.add("072");
//            dafare.add("139");
//            dafare.add("159");
//            dafare.add("162");
//            dafare.add("196");
//            dafare.add("202");
//            dafare.add("203");
//            dafare.add("800");
//            dafare.add("801");
//            dafare.add("802");
//            dafare.add("803");
//            dafare.add("306");
//            dafare.add("307");
//            dafare.add("312");
//            dafare.add("321");
//            dafare.add("322");
//            ArrayList<StatusBranch> liout = new ArrayList<>();
//            Db db = new Db(host_PROD_CZ, false);
//            Db db = new Db(host_PROD, false);
//            ArrayList<String[]> ip = db.getIpFiliale();
//            db.closeDB();
//            
//            for (String[] f1 : ip) {
//
////                if (dafare.contains(f1[0])) {
////            System.out.println("rc.soop.testarea.test.main() "+f1[0]);
////            System.out.println("rc.soop.testarea.test.main() "+f1[1]);
//                Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
//                if (dbfil.getC() != null) {
//                    System.out.println("rc.soop.testarea.test.main(RAGG) " + f1[0]);
//                    
//                    String sql1 = "SELECT cod FROM sito_prenotazioni s WHERE s.timestamp LIKE '2023-01-28%'";
//                    
//                    try ( Statement st1 = dbfil.getC().createStatement();  ResultSet rs1 = st1.executeQuery(sql1)) {
//                        while (rs1.next()) {
//                            System.out.println("rc.soop.testarea.test.main() " + rs1.getString(1));
//                        }
//                    }
////                                }
////                                System.out.println("rc.soop.testarea.test.main(OKKK) " + f1[0]);
//
////                        String sql = "SELECT cod FROM aggiornamenti_mod WHERE fg_stato='1' AND cod LIKE '230128%'";
//////                    String update = "UPDATE aggiornamenti_mod SET fg_stato='0' ";
////                        try {
////                            try ( Statement st1 = dbfil.getC().createStatement();  ResultSet rs1 = st1.executeQuery(sql)) {
////                                while (rs1.next()) {
////                                    String update = "UPDATE aggiornamenti_mod SET fg_stato='0' WHERE cod='" + rs1.getString(1) + "'";
////                                    try ( Statement st2 = dbfil.getC().createStatement()) {
////                                        st2.executeUpdate(update);
////                                    }
////                                }
////                                System.out.println("rc.soop.testarea.test.main(OKKK) " + f1[0]);
//////                        if (st1.executeLargeUpdate(update) > 0) {
//////                            System.out.println("rc.soop.testarea.test.main(OKKK) " + f1[0]);
//////                        } else {
//////                            System.out.println("rc.soop.testarea.test.main(KOOO2) " + f1[0]);
//////                        }
////                            }
////                        } catch (Exception e) {
////                            System.out.println("rc.soop.testarea.test.main(KOOO3) " + f1[0] + " -- " + Util.estraiEccezione(e));
////                        }
//                    dbfil.closeDB();
//                } else {
//                    System.out.println("rc.soop.testarea.test.main(KOOO1) " + f1[0]);
//                }
////                } else {
////                    System.out.println("rc.soop.testarea.test.main(GIA' FATTA) " + f1[0]);
////                }
//            }
//            String datecreation1 = new DateTime().toString("yyyyMMddHHmmss");
//            String datecreation2 = new DateTime().withZone((DateTimeZone.forID("Europe/Rome"))).toString("yyyyMMddHHmmss");
//            System.out.println("rc.soop.testarea.test.main() "+datecreation1);
//            System.out.println("rc.soop.testarea.test.main() "+datecreation2);
//            GeneraFile gf = new GeneraFile();
//            gf.setIs_IT(true);
//            gf.setIs_UK(false);
//            gf.setIs_CZ(false);
//
//            DatabaseCons db = new DatabaseCons(gf);
//
//            DateTime iniziomese = new DateTime().minusDays(5).dayOfMonth().withMinimumValue().withHourOfDay(0)
//                    .withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
//
////        System.out.println("com.fl.upload.GeneraFile.rilasciafile() "+);
////            String mesemysql = iniziomese.toString(patternmonthsql);
//            DateTime ieri = new DateTime().minusDays(2);
//
////            String meseriferimento = iniziomese.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
////            String annoriferimento = iniziomese.year().getAsText(Locale.ITALY).toUpperCase();
//
//            String data1 = iniziomese.toString(patternsql);
//            String data2 = ieri.toString(patternsql);
//
////            String d3 = iniziomese.toString(patternnormdate_filter);
////            String d4 = ieri.toString(patternnormdate_filter);
////            String meseanno_prec = iniziomese.minusMonths(1).toString(patternmonthsql);
////            String anno_rif = iniziomese.minusMonths(1).year().getAsText();
////            String meseriferimento_prec = iniziomese.minusMonths(1).monthOfYear().getAsText(Locale.ITALY).toUpperCase();
//            String path = db.getPath("temp");
//            ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
////            ArrayList<String> filiali_soloROMA = db.list_branch_RM();
//            ArrayList<Branch> allenabledbr = db.list_branch();
//
//            String nomereport = "MANAGEMENT CONTROL - REPORT CHANGE ACCOUNTING N1 DA " + data1 + " A " + data2 + ".xlsx";
//            File Output = new File(path + nomereport);
//            String base64 = ControlloGestione.management_change_accounting1(Output, br1, iniziomese, ieri, allenabledbr, db);
//
//            db.closeDB();
//            Logger log = createLog("Mac2.0_AGG_CENTRAL_" + "000", rb.getString("path.log"), pattern4);
////
//            Central_Branch cb = new Central_Branch("000");
//            cb.updateCentral(log);
            //System.out.println("rc.soop.testarea.test.main() "+Double.valueOf("1.72 "));
//            File txt = new File("C:\\Users\\rcosco\\Desktop\\content.txt");
//            
//            String bs4 = Files.toString(txt, Charset.defaultCharset());
//                
//            FileUtils.writeByteArrayToFile(new File("C:\\Users\\rcosco\\Desktop\\content.pdf"), Base64.decodeBase64(bs4)); 
//        try {
//            Db db = new Db(host_TEST_CZ, false);
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT gruppo_nc,causale_nc,fg_in_out,fg_tipo_transazione_nc,nc_de,de_causale_nc FROM maccorpcz.nc_causali WHERE filiale='000'");
//
////            AtomicInteger i0 = new AtomicInteger(0);
//            while (rs.next()) {
//                String ncde = "00";
//                if (rs.getString(4).equals("1") && rs.getString(3).equals("2")) {
//                    ncde = "01";
//                } else if (rs.getString(4).equals("1") && rs.getString(3).equals("1")) {
//                    ncde = "02";
//                } else if (rs.getString(4).equals("2") && rs.getString(3).equals("3")) {   //STOCK
//                    ncde = "09";
//                } else if (rs.getString(4).equals("2") && rs.getString(3).equals("2")) {   //STOCK
//                    ncde = "10";
//                } else if (rs.getString(4).equals("2") && rs.getString(3).equals("4")) {   //STOCK
//                    ncde = "14";
//                } else if (rs.getString(4).equals("4") && rs.getString(3).equals("1")) {   //NO STOCK
//                    ncde = "15";
//                } else if (rs.getString(4).equals("4") && rs.getString(3).equals("2")) {   //NO STOCK 
//                    ncde = "16";
//                } else {
//                    System.out.println(rs.getString(6) + " : " + rs.getString(4) + ";" + rs.getString(3) + " = " + rs.getString(5) + " ----------------------------");
//                }
//
////                System.out.println(rs.getString(1)+" (-) "+rs.getString(2)+" (-) "+rs.getString(3)+" (-) "+rs.getString(4));
////                String causale_nc = StringUtils.leftPad(String.valueOf(i0.addAndGet(1)), 6, "0");
//                String upd = "UPDATE nc_causali SET nc_de = '" + ncde + "' WHERE filiale = '000' AND gruppo_nc = '"
//                        + rs.getString(1) + "' AND causale_nc = '" + rs.getString(2) + "'";
//                boolean es = db.getC().createStatement().executeUpdate(upd) > 0;
//                System.out.println(upd + " --> " + es);
//            }
//
//            db.closeDB();
//        } catch (SQLException ex) {
//            ex.printStackTrace();
//        }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
;

}
