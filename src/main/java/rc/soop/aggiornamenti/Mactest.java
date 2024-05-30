/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.aggiornamenti;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import static rc.soop.aggiornamenti.Utility.patternnormdate;
import static rc.soop.aggiornamenti.Utility.patternsqldate;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Mactest {

    public static void main(String[] args) {

//        VerificaAggiornamenti.verifica_Aggiornamenti22();
//        VerificaAggiornamenti.ultimo_Aggiornamento22();
////        agg(null);
        //1    
//    updateSQL("ALTER TABLE `sito_servizi_agg` ADD COLUMN `buysell` ENUM('B','S','A') NOT NULL DEFAULT 'S' AFTER `attivo`;", false);
//    updateSQL("ALTER TABLE `sito_agevolazioni_varie` ADD COLUMN `buysell` ENUM('B','S','A') NOT NULL DEFAULT 'S' AFTER `coupon`;", true);
        //2
//        updateSQLTEST("INSERT INTO conf VALUES ('sito.editwt.2023','https://b2badmintest.forexchange.it/booking/ext/edit-booking')", false);
//        updateSQL("INSERT INTO pages VALUES ('web_tran_buy.jsp','0123')", false);

//        updateSQLCZ("ALTER TABLE `selectlevelrate` ADD COLUMN `percent` VARCHAR(10) NOT NULL DEFAULT '0.00' AFTER `descrizione`;", false);
//        updateSQLTESTCZ("ALTER TABLE `selectlevelrate` ADD COLUMN `percent` VARCHAR(10) NOT NULL DEFAULT '0.00' AFTER `descrizione`;", true);
//        try {
//            Db db = new Db(host_TEST, false);
//            ResultSet rs = db.getC().createStatement().executeQuery("show create table unlockrate_justify");
//            if (rs.next()) {
//                String cr = StringUtils.replace(rs.getString(2), "CREATE TABLE", "CREATE TABLE IF NOT EXISTS") ;
//                System.out.println("Table Name: " + rs.getString(1));
//                System.out.println("CREATE: " + cr);
//                updateSQL(rs.getString(2), false);
//                updateSQLCZ(rs.getString(2), false);
//                updateSQLTEST(rs.getString(2), true);
//                updateSQLTESTCZ(rs.getString(2), false);
//            }
//            rs.close();
//            db.closeDB();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        File f = new File("C:\\mnt\\PRENOTAZIONI DA INSERIRE.txt");
//        try (Stream<String> stream = Files.lines(Paths.get(f.getPath()), Charsets.UTF_8)) {
//            stream.forEach(line -> {
//                try {
//                    String f1 = StringUtils.substring(line, 0, 4).replaceAll("'", "");
//                    String sql = "INSERT INTO sito_prenotazioni VALUES (" + new String(line.getBytes(), Charsets.UTF_8) + ")";
//                    Db db1 = new Db(host_PROD, false);
////                    System.out.println(sql);
//
//                    db1.getC().createStatement().executeUpdate(sql);
////                String dtoper = new DateTime().toString(patternsqldate);
////                String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
////                db1.insertValue_agg(null, sql, "000", dt_val, "setaser", false);
////                db1.insertValue_agg(null, sql, f1, dt_val, "setaser", false);
//                    db1.closeDB();
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        updateSQLTEST(host_TEST, false);
//        updateSQLTESTUK("UPDATE selectdoctrans SET descrizione = 'Heavy Transaction Form And Sanction List' WHERE codice ='_heavy';", false);
//        updateSQLUK("UPDATE selectdoctrans SET descrizione = 'Heavy Transaction Form And Sanction List' WHERE codice ='_heavy';", false);
//        Db db = new Db(host_PROD, false);
//        ArrayList<String[]> ip = db.getIpFiliale();
//
//        db.closeDB();
////
//        for (int x = 0; x < ip.size(); x++) {
//            ripristinaIDtransazioniNOChange(ip.get(x)[0], ip.get(x)[1]);
//        }
//        ripristinaIDtransazioniNOChange("161", "192.168.112.2");s
//        PER TICKET 1752
//        updateSQL("ALTER TABLE `valute_tagli` ADD COLUMN `fg_stato` VARCHAR(1) NOT NULL DEFAULT '1' AFTER `ip_taglio`", false); //2
//        updateSQLCZ("ALTER TABLE `valute_tagli` ADD COLUMN `fg_stato` VARCHAR(1) NOT NULL DEFAULT '1' AFTER `ip_taglio`", false); //2
//        updateSQLUK("ALTER TABLE `valute_tagli` ADD COLUMN `fg_stato` VARCHAR(1) NOT NULL DEFAULT '1' AFTER `ip_taglio`", false); //2
//        updateSQL("ALTER TABLE `ch_transaction_temp` CHANGE COLUMN `bl_motiv` `bl_motiv` LONGTEXT NULL DEFAULT NULL COMMENT 'motivazione (- se ok)' AFTER `bl_status`;", false); //2
//        updateSQL("ALTER TABLE `ch_transaction_story` CHANGE COLUMN `bl_motiv` `bl_motiv` LONGTEXT NULL DEFAULT NULL COMMENT 'motivazione (- se ok)' AFTER `bl_status`;", false); //2
//        updateSQL("INSERT INTO path VALUES ('kyc_fa', '5;10;15;')", false); //2
//        updateSQL("INSERT INTO path VALUES ('kyc_listok', '007;011;013;084;088;103;114;046;037;147;069;078;071')", false); //2
//        updateSQL("INSERT INTO path VALUES ('kyc_listro', '002;274;074;159;039;038;136;065;132;121;042')", false); //2
//        updateSQL("INSERT INTO path VALUES ('kyc_va', '10000.00;20000.00;')", false); //2
//        updateSQL("INSERT INTO path VALUES ('kyc_vro', '2000.00')", false); //2
//        DA FARE PER IL SITO - 04/11
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `stato_crm` VARCHAR(2) NOT NULL DEFAULT '0' AFTER `timestamp`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cod_pagamento` VARCHAR(100) NOT NULL DEFAULT '0' AFTER `stato_crm`;", false); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` CHANGE COLUMN `note` `note` VARCHAR(900) NOT NULL DEFAULT '-' AFTER `dt_ritiro`;", false); //2
//        updateSQL("ALTER TABLE `sito_agevolazioni_varie` ADD COLUMN `coupon` ENUM('Y','N') NOT NULL DEFAULT 'N' AFTER `attivo`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_sesso` VARCHAR(10) NOT NULL DEFAULT 'M' AFTER `cod_pagamento`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_dtnascita` VARCHAR(10) NOT NULL DEFAULT '01/01/1901' AFTER `cl_sesso`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_nazione` VARCHAR(50) NOT NULL DEFAULT 'IT' AFTER `cl_dtnascita`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_city` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `cl_nazione`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_prov` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `cl_city`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_codfisc` VARCHAR(16) NOT NULL DEFAULT '-' AFTER `cl_prov`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `crm_note` VARCHAR(900) NOT NULL DEFAULT '-' AFTER `cl_codfisc`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_indirizzo` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `crm_note`, ADD COLUMN `cl_indirizzocity` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `cl_indirizzo`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_indirizzoprov` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `cl_indirizzocity`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_indirizzocap` VARCHAR(10) NOT NULL DEFAULT '00000' AFTER `cl_indirizzoprov`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `cl_indirizzinazione` VARCHAR(50) NOT NULL DEFAULT '-' AFTER `cl_indirizzocap`;", true); //2
//        updateSQL("ALTER TABLE `sito_prenotazioni` ADD COLUMN `pan` VARCHAR(4) NOT NULL DEFAULT '0000' AFTER `cl_indirizzinazione`;", true); //2
//
//    updateSQL("INSERT INTO sito_agevolazioni_varie VALUES ('sconto', 'SCONTO D\'AUTUNNO', '1', '0.00', '5.00', '0.00', '2019-10-31', '2019-11-05', '', '', '', 'Y', 'N')", true);
//    updateSQL("INSERT INTO sito_agevolazioni_varie VALUES ('WBFC19', 'ISCRIZIONE', '0', '1.00', '0.00', '0.00', '2019-10-31', '2019-12-31', '', '', '', 'Y', 'Y')", true);
//    updateSQL("INSERT INTO sito_agevolazioni_varie VALUES ('young', 'Sconto Giovani (Under 26)', '0', '0.70', '0.00', '0.00', '2018-01-01', '2028-01-01', '', '', '', 'Y', 'N')", true);
//    
//    updateSQL("INSERT INTO sito_commissione_fissa VALUES ('01', '0.00', '999999.99', '0.00', '0')", true);
//    
//    updateSQL("UPDATE nc_transaction SET causale_nc='002786' WHERE causale_nc='002211' AND gruppo_nc='OLTA_TRENI' AND data >'2020-03-11'", false);
//    updateSQL("INSERT INTO sito_rate_range VALUES ('01', '1250.01', '2000.00', 'LEV3', 'Y')", true);
//    updateSQL("INSERT INTO sito_rate_range VALUES ('01', '200.01', '500.00', 'LEV1', 'Y')", true);
//    updateSQL("INSERT INTO sito_rate_range VALUES ('01', '2000.01', '2999.99', 'BEST', 'Y')", true);
//    updateSQL("INSERT INTO sito_rate_range VALUES ('01', '500.01', '1250.00', 'LEV2', 'Y')", true);
//    
//    updateSQL("INSERT INTO sito_servizi_agg VALUES ('BUB', 'Servizio Buy Back', '1', '0.00', '5.00', '0.00', '2018-01-01', '2100-01-01', '', '', 'Y')", true);
//    updateSQL("INSERT INTO sito_stato VALUES ('BUB', 'Servizio Buy Back', '1', '0.00', '5.00', '0.00', '2018-01-01', '2100-01-01', '', '', 'Y')", true);
//        try {
//            Db db = new Db(host_TEST, false);
//            String table_name = "sito_spread";
//            String sql1 = "SELECT * FROM " + table_name;
//            ResultSet rs = db.getC().createStatement().executeQuery(sql1);
//            String sql0 = "INSERT INTO " + table_name + " VALUES (";
//            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
//                sql0 += "?,";
//            }
//            sql0 = StringUtils.substring(sql0, 0, sql0.length() - 1) + ")";
//            while (rs.next()) {
//                PreparedStatement ps = db.getC().prepareStatement(sql0);
//                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
//                    ps.setString(i, rs.getString(i));
//                }
//                updateSQLPS(ps);
//                System.out.println("mactest.Mactest.main() " + ps.toString());
//            }
//            db.closeDB();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        try {
//            Db db = new Db(host_TEST, false);
//            ResultSet rs = db.getC().createStatement().executeQuery("show create table sito_spread");
//            if (rs.next()) {
//                System.out.println("Table Name: " + rs.getString(1));
//                updateSQL(rs.getString(2), true);
//            }
//            rs.close();
//            db.closeDB();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        //FINE SITO
//                agg(null);
//
//        try {
//            File file = new File("/mnt/mac/Heavy Transaction.pdf");
//            updateSQLTESTUK("INSERT INTO `conf` (`id`, `des`) VALUES ('path.heavyuk.pdf','" + Base64.encodeBase64String(FileUtils.readFileToByteArray(file)) + "');", false);
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        }
//        updateSQLCZ("UPDATE users SET username = 'jbabic0097' WHERE cod='0097'", false); //1
//        updateSQLTESTCZ("UPDATE users SET username = 'jbabic0097' WHERE cod='0097'", false); //1
//        updateSQLCZ("UPDATE ch_transaction_valori SET pos = '600' where pos = '574'", false); //1
//        
//        updateSQLUK("UPDATE ch_transaction SET pos = '600' where pos = '502'", false); //1
//        updateSQLUK("UPDATE ch_transaction_valori SET pos = '600' where pos = '502'", false); //1
//        
//        updateSQLUK("UPDATE carte_credito SET carta_credito = '600' where carta_credito='502'", false); //1
//        updateSQLUK("UPDATE et_change SET cod_dest = '600' WHERE cod_dest = '502' AND cod IN (SELECT distinct(cod) FROM et_change_valori WHERE kind='04')", false); //2
//SELECT * FROM maccorpukprod.ch_transaction WHERE pos = '502';
//SELECT * FROM maccorpukprod.ch_transaction_valori WHERE pos = '502';
//
//
//SELECT * FROM maccorpczprod.ch_transaction WHERE pos = '574';
//SELECT * FROM maccorpczprod.ch_transaction_valori WHERE pos = '574';
//        updateSQLTESTCZ("UPDATE nc_causali SET fg_in_out = '1' WHERE fg_tipo_transazione_nc = '7' AND nc_de='17'", false);    
//        updateSQLTESTCZ("UPDATE nc_causali SET fg_in_out = '2' WHERE fg_tipo_transazione_nc = '7' AND nc_de='18'", false);
//        updateSQLCZ("UPDATE nc_causali SET fg_in_out = '1' WHERE fg_tipo_transazione_nc = '7' AND nc_de='17'", false);    
//        updateSQLCZ("UPDATE nc_causali SET fg_in_out = '2' WHERE fg_tipo_transazione_nc = '7' AND nc_de='18'", false);
//        updateSQLCZ("INSERT INTO selectncde VALUES ('3', '03', 'CZK Refund', '1', '2')", false);
//        updateSQLCZ("INSERT INTO selectncde VALUES ('3', '04', 'Currency Refund', '1', '2')", false);
//        updateSQLCZ("ALTER TABLE `branch` CHANGE COLUMN `pay_terminale` `pay_terminale` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `pay_token`;", false);
//        updateSQLCZ("DELETE FROM valute_tagli WHERE valuta ='AED' OR valuta = 'SAR' OR valuta ='TRY';" , false);
//        updateSQLCZ("DELETE FROM supporti_valuta WHERE valuta ='AED' OR valuta = 'SAR' OR valuta ='TRY';" , false);
//        updateSQLUK("ALTER TABLE `client_uk` ADD COLUMN `occupation` VARCHAR(100) NOT NULL DEFAULT '-' AFTER `date`;" , false);
//        updateSQL("ALTER TABLE `ch_transaction_client` CHANGE COLUMN `cap` `cap` VARCHAR(255) NULL DEFAULT '-' AFTER `indirizzo`;" , true);        
//        updateSQL("ALTER TABLE `ch_transaction` CHANGE COLUMN `bl_motiv` `bl_motiv` LONGTEXT NULL COLLATE 'latin1_swedish_ci' AFTER `bl_status`;" , false);
//        updateSQL("UPDATE nc_transaction SET fg_tipo_transazione_nc = '4' WHERE fg_tipo_transazione_nc = '8'" , false);
//        excelsetPSWOLTA();
//        Db db = new Db(host_PROD, false);
//        ArrayList<String[]> ip = db.getIpFiliale();
//        db.closeDB();
//
//        for (int i = 0; i < ip.size(); i++) {
//            String[] f1 = ip.get(i);
//            Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
//            if (dbfil.getC() != null) {
////                String sql = "SELECT * FROM tr WHERE descr LIKE '%deadlock%'";
//                
//                String sql = "SELECT * FROM inv_list where transaction not in (SELECT cod FROM ch_transaction)";
//                ResultSet rs1;
//                try {
//                    rs1 = dbfil.getC().createStatement().executeQuery(sql);
//                    while (rs1.next()) {
//                        System.out.println(f1[0] + ": " + rs1.getString("dt"));
//                    }
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//                dbfil.closeDB();
//            }
//        }
//        ripristinaIDtransazioniNOChange("172", "192.168.128.5");
//        excelPAYMAT();
//        resetLOY();
//        CODICI SBLOCCO - FARE IN PRODUZIONE
//        updateSQL("ALTER TABLE `it_nochange_valori` CHANGE COLUMN `causale_nc` `causale_nc` VARCHAR(45) NOT NULL DEFAULT '000000' AFTER `cod`;", false);
//        updateSQLTEST("ALTER TABLE stock_report MODIFY cod_value VARCHAR(45) NOT NULL DEFAULT 'EUR'", false);
//        updateSQLTEST("ALTER TABLE stock_story MODIFY cod_value VARCHAR(45) NOT NULL DEFAULT 'EUR'", false);
//        updateSQL("ALTER TABLE it_nochange_valori MODIFY causale_nc VARCHAR(45) NOT NULL DEFAULT '000000'", false);
//        excelsetPSWOLTA();
//         agg(null);
//        agg("109");
//        agg("170");
//        agg("171");
//        updateSQLTEST("INSERT INTO province (filiale,provincia,de_provincia) VALUES ('000','','')", false);
//        updateSQL("UPDATE valute SET cambio_acquisto='1'", false);
//        updateSQL("ALTER TABLE it_nochange_valori MODIFY causale_nc VARCHAR(45) NOT NULL DEFAULT '000000'", false);
//        updateSQLTEST("ALTER TABLE it_nochange_valori MODIFY causale_nc VARCHAR(45) NOT NULL DEFAULT '000000'", false);
//        try {
//            Db db = new Db(host_PROD, false);
//            ResultSet rs = db.getC().createStatement().executeQuery("CHECK TABLE agenzie;");
//            while(rs.next()){
//                System.out.println(rs.getString(1));
//                System.out.println(rs.getString(2));
//                System.out.println(rs.getString(3));
//                System.out.println(rs.getString(4));
//            }
//            db.closeDB();
//        updateSQLTEST("TRUNCATE TABLE temppaymat", true);
//        updateSQLTEST("ALTER TABLE temppaymat ADD causal VARCHAR(200) NOT NULL DEFAULT '-';", true);
//        updateSQL("UPDATE nc_tipologia SET fg_registratore = '0'", false);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }       
////CREARE NUOVA TABELLA
//        try {
//            Db db = new Db(host_TEST, false);
//            ResultSet rs = db.getC().createStatement().executeQuery("show create table sito_supporti");
//            if (rs.next()) {
//                System.out.println("Table Name: " + rs.getString(1));
//                updateSQLTEST(rs.getString(2), true);
//            }
//            rs.close();
//            db.closeDB();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }

    private static void excelPAYMAT() {
        updateSQLTEST("TRUNCATE TABLE temppaymat", false);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
        }
        ArrayList<String[]> complete = new ArrayList<>();
        try {
            File f = new File("C:\\Maccorp\\Elenco_Paymat_v4 (Test).xlsx");
            FileInputStream fis = new FileInputStream(f);
            XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
            XSSFSheet mySheet = myWorkBook.getSheetAt(0);
            Iterator<Row> rowIterator = mySheet.iterator();
            rowIterator.next();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                // For each row, iterate through each columns
                Iterator<Cell> cellIterator = row.cellIterator();
                String val[] = new String[3];
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int index = cell.getColumnIndex();
                    switch (cell.getCellType()) {
                        case STRING:
                            val[index] = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            val[index] = NumberToTextConverter.toText(cell.getNumericCellValue());
                            break;
                        case BOOLEAN:
                            val[index] = String.valueOf(cell.getBooleanCellValue());
                            break;
                        default:
                    }
                }
                complete.add(val);

            }
            fis.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        for (int c = 0; c < complete.size(); c++) {
            String[] val = complete.get(c);
            if (!val[1].contains("Non")) {
                String insert = "INSERT INTO temppaymat VALUES ('" + val[0] + "','1','" + val[1] + "','" + val[2] + "')";
                updateSQLTEST(insert, false);
                System.out.println(insert);
            }
        }

    }

//    private void () {
//        try {
//            File f = new File("C:\\Maccorp\\olta\\olta credenziali.xlsx");
//            FileInputStream fis = new FileInputStream(f);
//            XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
//            XSSFSheet mySheet = myWorkBook.getSheetAt(0);
//            Iterator<Row> rowIterator = mySheet.iterator();
//            rowIterator.next();
//            while (rowIterator.hasNext()) {
//                Row row = rowIterator.next();
//                int fine = row.getLastCellNum();
//                for (int x = 0; x < fine; x++) {
//                    Cell c1 = row.getCell(x);
//                    if (c1 == null) {
//                        c1 = row.createCell(x);
//                    }
//                    
//                    c1
//                    
//                }
//            }
//            
//            //SCRIVI
//            
//            myWorkBook.close();
//        } catch (Exception e) {
//
//        }
//
//    }
    private static void excelsetPSWOLTA() {
        ArrayList<String[]> complete = new ArrayList<>();
        try {
            File f = new File("C:\\Maccorp\\olta\\olta credenziali.xlsx");
            FileInputStream fis = new FileInputStream(f);
            XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
            XSSFSheet mySheet = myWorkBook.getSheetAt(0);
            Iterator<Row> rowIterator = mySheet.iterator();
            rowIterator.next();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                String val[] = new String[15];
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int index = cell.getColumnIndex();
                    switch (cell.getCellType()) {
                        case STRING:
                            val[index] = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            val[index] = String.valueOf(parseIntR(String.valueOf(cell.getNumericCellValue())));
                            break;
                        case BOOLEAN:
                            val[index] = String.valueOf(cell.getBooleanCellValue());
                            break;
                        default:
                    }
                }
                complete.add(val);
            }
            fis.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        for (int c = 0; c < complete.size(); c++) {
            String[] valori = complete.get(c);
            Db dbq = new Db();
            boolean es = dbq.updatePSWOLTA(valori);
            dbq.closeDB();
            if (es) {
                System.out.println(valori[0] + " OK");
            } else {
                System.err.println(valori[0] + " KO");
            }
        }

    }

    public static final String host_TEST = rb.getString("db.ip") + "/maccorp";
    public static final String host_PROD = rb.getString("db.ip") + "/maccorpita";

    public static final String host_PROD_CZ = rb.getString("db.ip") + "/maccorpczprod";
    public static final String host_TEST_CZ = rb.getString("db.ip") + "/maccorpcz";

    public static final String host_PROD_UK = rb.getString("db.ip") + "/maccorpukprod";
    public static final String host_TEST_UK = rb.getString("db.ip") + "/maccorpuk";

    public static void updateSQLPS(PreparedStatement ps) {
        Db db1 = new Db(host_PROD, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(ps, null, null, dt_val, "setaser", false);
        db1.closeDB();
    }

    public static void updateSQL(String sql, boolean nocentr) {
        Db db1 = new Db(host_PROD, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void updateSQLUK(String sql, boolean nocentr) {
        Db db1 = new Db(host_PROD_UK, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void updateSQLCZ(String sql, boolean nocentr) {
        Db db1 = new Db(host_PROD_CZ, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void updateSQLTEST(String sql, boolean nocentr) {
        Db db1 = new Db(host_TEST, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void updateSQLTESTUK(String sql, boolean nocentr) {
        Db db1 = new Db(host_TEST_UK, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void updateSQLTESTCZ(String sql, boolean nocentr) {
        Db db1 = new Db(host_TEST_CZ, false);
        String dtoper = new DateTime().toString(patternsqldate);
        String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        db1.insertValue_agg(null, sql, null, dt_val, "setaser", nocentr);
        db1.closeDB();
    }

    public static void resetLOY() {
//        Db_Loy db1 = new Db_Loy();
//        if (db1.getC() != null) {
//            try {
//                db1.getC().createStatement().executeUpdate("UPDATE codici SET stato='0';");
//                db1.getC().createStatement().executeUpdate("UPDATE mac_associate SET stato='0';");
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            db1.closeDB();
//        }
//        updateSQLTEST("UPDATE loyalty_ch SET codcl = '-';", false);
    }

    public static void excelspread() {
        Db db = new Db();
        ArrayList<String> li = db.list_cod_branch_enabled();
//                boolean es = db.updateCurrency(val);
        db.closeDB();

        System.out.println(db.getH());

        ArrayList<String[]> complete = new ArrayList<>();

        try {

            // TODO code application logic here
//            File f = new File("C:\\Maccorp\\Currency - Tabella Spread (OK 02_01_2018) per Raffaele.xlsx");
//            File f = new File("C:\\Maccorp\\Currency - Tabella Spread (OK 08_01_2018) per Raffaele.xlsx");
            File f = new File("C:\\Maccorp\\Currency - Tabella Spread Filiali 176 e 177 (aggiornamento 28_02).xlsx");
//            File f = new File("C:\\Maccorp\\Currency - Spread.xlsx");
            FileInputStream fis = new FileInputStream(f);
            // Finds the workbook instance for XLSX file
            XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
            // Return first sheet from the XLSX workbook
            XSSFSheet mySheet = myWorkBook.getSheetAt(0);
            // Get iterator to all the rows in current sheet
            Iterator<Row> rowIterator = mySheet.iterator();
            // Traversing over each row of XLSX file
            rowIterator.next();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                // For each row, iterate through each columns
                Iterator<Cell> cellIterator = row.cellIterator();
                String val[] = new String[15];
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int index = cell.getColumnIndex();

                    switch (cell.getCellType()) {
                        case STRING:
                            val[index] = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            val[index] = String.valueOf(parseIntR(String.valueOf(cell.getNumericCellValue())));
                            break;
                        case BOOLEAN:
                            val[index] = String.valueOf(cell.getBooleanCellValue());
                            break;
                        default:
                    }
                }
                complete.add(val);
//                System.out.println("BR "+val[0]+" sss "+val[1]+val[2]+val[3]+val[4]+val[5]+val[6]+val[7]+val[8]+val[9]+val[10]+val[11]+val[12]+val[13]
//                        +val[14] +" OK");

//                Db db = new Db();
//                boolean es = db.updateCurrency(val);
//                db.closeDB();
//                if (es) {
//                    System.out.println("BR " + val[0] + " VAL " + val[1] + " OK");
//                } else {
//                    System.err.println("BR " + val[0] + " VAL " + val[1] + " KO");
//                }
            }
            fis.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

//        System.out.println(li.size());
//        System.out.println(complete.size());
        for (int x = 0; x < li.size(); x++) {
            String filialedamod = li.get(x);
            for (int c = 0; c < complete.size(); c++) {
                String[] val = complete.get(c);
                if (val[0].equals(filialedamod)) {
                    Db dbq = new Db();
                    boolean es = dbq.updateCurrency(val);
//                    boolean es = dbq.updateCurrencyNOcolumn(val);
                    dbq.closeDB();
                    if (es) {
                        System.out.println("BR " + val[0] + " sss " + val[1] + val[2] + val[3] + val[4] + val[5] + val[6] + val[7] + val[8] + val[9] + val[10] + val[11] + val[12] + val[13]
                                + val[14] + " OK");
                    }
                }
            }

        }
    }

    public static ArrayList<String[]> leggiexcel() {

        ArrayList<String[]> complete = new ArrayList<>();

        try {
            File f = new File("C:\\Users\\rcosco\\Desktop\\Cartel1.xlsx");
            FileInputStream fis = new FileInputStream(f);
            XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
            XSSFSheet mySheet = myWorkBook.getSheetAt(0);
            Iterator<Row> rowIterator = mySheet.iterator();
            //rowIterator.next();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                String val[] = new String[15];
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int index = cell.getColumnIndex();
                    switch (cell.getCellType()) {
                        case STRING:
                            val[index] = cell.getStringCellValue();
                            break;
                        case NUMERIC:
                            val[index] = String.valueOf(parseIntR(String.valueOf(cell.getNumericCellValue())));
                            break;
                        case BOOLEAN:
                            val[index] = String.valueOf(cell.getBooleanCellValue());
                            break;
                        default:
                    }
                }
                complete.add(val);
            }
            fis.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        Db db = new Db(host_PROD, false);
        ArrayList<String[]> ip = db.getIpFiliale();
        db.closeDB();

        for (int i = 0; i < complete.size(); i++) {
            String[] f1 = complete.get(i);
            String fil = StringUtils.leftPad(f1[0], 3, "0");
            String id = StringUtils.leftPad(f1[1], 15, "0");
            String ipval = formatAL(fil, ip, 1);

            Db dbfil = new Db("//" + ipval + ":3306/maccorp");
            if (dbfil.getC() != null) {
                try {
                    ResultSet rs1 = dbfil.getC().createStatement().executeQuery("SELECT cod FROM nc_transaction WHERE filiale = '" + fil + "' AND id = '" + id + "'");
                    if (rs1.next()) {
                        String upd = "UPDATE nc_transaction SET ricevuta = '1' WHERE cod = '" + rs1.getString(1) + "'";
                        int x = dbfil.getC().createStatement().executeUpdate(upd);
                        if (x > 0) {
                            Db db1 = new Db(host_PROD, false);
                            int y = db1.getC().createStatement().executeUpdate(upd);
                            db1.closeDB();
                            if (y > 0) {
                                System.out.println(fil + " - " + id + " - " + ipval + " - " + rs1.getString(1));
                            }
                        }
                    }
                    dbfil.closeDB();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }

        return complete;
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
    }

    public static List<StatusBranch> agg(String filialesingola) {

        ArrayList<StatusBranch> liout = new ArrayList<>();

        Db db = new Db(host_PROD, false);
        ArrayList<String[]> ip = db.getIpFiliale();
        if (filialesingola != null) {
            ip = db.getIpFiliale(filialesingola);
        }
        ArrayList<String[]> aggfiliali = db.aggfiliali();
        db.closeDB();

        for (int i = 0; i < ip.size(); i++) {

            String[] f1 = ip.get(i);
            String fil = f1[0];
            String aggfil = formatAL(fil, aggfiliali, 1);
            int aggfrom = 0;
            int aggto = parseIntR(aggfil);
            StatusBranch sb = new StatusBranch();
            sb.setCod(fil);
            sb.setIp(f1[1]);
            if (fil.equals("000")) {
                sb.setRagg(true);
            } else {
                Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
                if (dbfil.getC() != null) {
                    aggfrom = dbfil.countA();
                    dbfil.closeDB();
                    sb.setRagg(true);
                } else {
                    sb.setRagg(false);
                }
            }
            sb.setAggto(aggto);
            sb.setAggfrom(aggfrom);
            liout.add(sb);
            System.out.println(sb.toString());
        }
        return liout;
    }

    public static void updateoneshot() {
        try {
            Db db = new Db(host_TEST, false);
            ArrayList<String[]> ip = db.getIpFiliale();
            db.closeDB();

            String ipfil = Utility.formatAL("043", ip, 1);
            Db dbfil = new Db("//" + ipfil + ":3306/maccorp", true);
            String profcl = dbfil.getConf("path.profcl");
            dbfil.closeDB();

            Db db1 = new Db(host_PROD, false);

            String upd = "UPDATE conf SET des = ? WHERE id = ?";
            PreparedStatement ps1 = db1.getC().prepareStatement(upd);
            ps1.setString(1, profcl);
            ps1.setString(2, "path.profcl");

            String dtoper = new DateTime().toString(patternsqldate);
            String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
            db1.insertValue_agg(ps1, null, null, dt_val, "setaser", false);

            db1.closeDB();

//            System.out.println( profcl);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }

    public static int parseIntR(String value) {
//        value = value.replaceAll("-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
        }
        return 0;
    }

    private static void ripristinaIDtransazioniNOChange(String filiale, String ip) {
        try {
//            ArrayList<String> li_temp0 = new ArrayList<>();
            ArrayList<String[]> li_temp1 = new ArrayList<>();
            ArrayList<String[]> li_temp2 = new ArrayList<>();
//            ArrayList<String> li_update = new ArrayList<>();

            String sql = "select cod,id FROM nc_transaction WHERE filiale='" + filiale + "' ORDER BY data";
            Db db = new Db(host_PROD, false);
            ResultSet rs1 = db.getC().createStatement().executeQuery(sql);
            while (rs1.next()) {
                String cod = rs1.getString("cod").trim();
                String id = rs1.getString("id").trim();
                String[] v = {cod, id};
//                li_temp0.add(cod);
                li_temp1.add(v);
            }

            Db dbfil = new Db("//" + ip + ":3306/maccorp");
            if (dbfil.getC() != null) {
                ResultSet rs2 = dbfil.getC().createStatement().executeQuery(sql);
                while (rs2.next()) {
                    String cod = rs2.getString("cod").trim();
                    String id = rs2.getString("id").trim();
                    String[] v = {cod, id};
                    li_temp2.add(v);

//                    li_update.add("UPDATE nc_transaction SET id = '" + id + "' WHERE cod='" + cod + "'");
//                    if (!li_temp0.contains(cod)) {
//                        String del1 = "DELETE FROM nc_transaction WHERE cod ='" + cod + "'";
//                        System.out.println(del1);
//                    }
                }
                dbfil.closeDB();
            }

            for (int x = 0; x < li_temp1.size(); x++) {
                String[] ce = li_temp1.get(x);
                for (int y = 0; y < li_temp2.size(); y++) {
                    String[] fi = li_temp2.get(y);
                    if (ce[0].equals(fi[0])) {
                        if (!ce[1].equals(fi[1])) {
                            String upd = "UPDATE nc_transaction SET id = '" + fi[1] + "' WHERE cod='" + ce[0] + "'";

//                            System.out.println(upd);
                            System.out.println(upd + " : " + (db.getC().createStatement().executeUpdate(upd) > 0));

                        }
                    }
                }
            }
            db.closeDB();
            System.out.println(li_temp1.size());
            System.out.println(li_temp2.size());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void ripristinaIDtransazioniChange(String filiale, String ip) {
        try {

            ArrayList<String> li_temp0 = new ArrayList<>();
            ArrayList<String> li_update = new ArrayList<>();
            String sql = "select cod,id,data FROM ch_transaction_temp WHERE filiale='" + filiale + "'";
            String sql1 = "select cod,id,data FROM ch_transaction WHERE filiale='" + filiale + "'";

            Db dbfil = new Db("//" + ip + ":3306/maccorp");
            if (dbfil.getC() != null) {
                ResultSet rs2 = dbfil.getC().createStatement().executeQuery(sql);
                while (rs2.next()) {
                    String cod = rs2.getString("cod");
                    String id = rs2.getString("id");
//                    String[] v = {cod, id};
                    li_update.add("UPDATE ch_transaction_temp SET id = '" + id + "' WHERE cod='" + cod + "'");
                    li_temp0.add(cod);
                }
                ResultSet rs3 = dbfil.getC().createStatement().executeQuery(sql1);
                while (rs3.next()) {
                    String cod = rs3.getString("cod");
                    String id = rs3.getString("id");
                    li_update.add("UPDATE ch_transaction SET id = '" + id + "' WHERE cod='" + cod + "'");
                }
                dbfil.closeDB();
            }
            Db db = new Db(host_PROD, false);
            ResultSet rs1 = db.getC().createStatement().executeQuery(sql);
            while (rs1.next()) {
                String cod = rs1.getString("cod");
                String id = rs1.getString("id");
//                String[] v = {cod, id};
                if (!li_temp0.contains(cod)) {
                    String del1 = "DELETE FROM ch_transaction_temp WHERE cod ='" + cod + "'";
                    System.out.println(del1 + " : " + (db.getC().createStatement().executeUpdate(del1) > 0));
                }
            }
            for (int t = 0; t < li_update.size(); t++) {
                System.out.println(li_update.get(t) + " : " + (db.getC().createStatement().executeUpdate(li_update.get(t)) > 0));
            }
            db.closeDB();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
