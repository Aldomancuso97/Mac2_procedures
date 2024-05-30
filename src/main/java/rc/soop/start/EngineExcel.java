/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import static rc.soop.start.Central_Branch.host_h;
import static rc.soop.start.Utility.createLog;
import static rc.soop.start.Utility.fd;
import static rc.soop.start.Utility.formatMysqltoDisplay;
import static rc.soop.start.Utility.generaId;
import static rc.soop.start.Utility.parseStringDate;
import static rc.soop.start.Utility.pattern4;
import static rc.soop.start.Utility.patternsqldate;
import static rc.soop.start.Utility.rb;
import static rc.soop.start.Utility.roundDoubleandFormat;

/**
 *
 * @author rcosco
 */
public class EngineExcel {

    private static final String user_h = rb.getString("user_h");
    private static final String pwd_h = rb.getString("pwd_h");
    private static final String host_h = rb.getString("host_h");

    private static final Logger log = createLog("Mac2.0_EXCEL_", rb.getString("path.log"), pattern4);

    public static void insert_value_excel() {
        try {
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            List<WaitingExcel> ing = db.getExceldaElaborare();
            db.closeDB();
            if (!ing.isEmpty()) {
                log.log(Level.WARNING, "TROVATI {0} EXCEL DA ELABORARE", ing.size());
            }
            ing.forEach(input -> {
                AtomicInteger error = new AtomicInteger(0);
                List<ValueExcel> listcurr = getReadEXCELCURRENCY(Base64.decodeBase64(input.getFileout()));
                if (listcurr != null) {
                    DBHost dbm0 = new DBHost(host_h, user_h, pwd_h, log);
                    ArrayList<String> libr = dbm0.list_branch_enabled();
                    dbm0.closeDB();
                    DBHost dbm1 = new DBHost(host_h, user_h, pwd_h, log);
                    listcurr.forEach(cu1 -> {
                        libr.forEach(br1 -> {
                            String[] valorivaluta = dbm1.get_currency_filiale(br1, cu1.getV1());
                            boolean pres = dbm1.getPresenzaValuta(br1, cu1.getV1(), cu1.getV2(), input.getDt_start(), input.getUser());
                            if (pres && valorivaluta != null) {
                                boolean es = dbm1.update_change_BCE(br1, cu1.getV1(), cu1.getV2(), input.getDt_start(), input.getUser());
                                if (es) {
                                    double d_rifbce = fd(cu1.getV2());
                                    String buy_perc = valorivaluta[2];
                                    String sel_perc = valorivaluta[4];
                                    double d_standard_b = fd(buy_perc);
                                    double d_standard_s = fd(sel_perc);
                                    String tot_st_b = formatMysqltoDisplay(roundDoubleandFormat((d_rifbce * (100.0D + d_standard_b) / 100.0D), 8));
                                    String tot_st_s = formatMysqltoDisplay(roundDoubleandFormat((d_rifbce * (100.0D + d_standard_s) / 100.0D), 8));
                                    String msg_rh = "Upload excel bce. <br>BCE value " + formatMysqltoDisplay(cu1.getV2())
                                            + "<br>Buy Std: " + tot_st_b + "<br>Sell Std: " + tot_st_s + "<br>Date validity: " + input.getDt_start();
                                    Rate_history rh = new Rate_history(generaId(50), br1, cu1.getV1(), "0", msg_rh, input.getUser(), input.getData());
                                    dbm1.insert_ratehistory(rh);
                                } else {
                                    error.addAndGet(1);
                                }
                            }
                        });
                    });
                    dbm1.closeDB();
                } else {
                    error.addAndGet(1);
                }
                if (error.get() == 0) {
                    DBHost dbz = new DBHost(host_h, user_h, pwd_h, log);
                    boolean o = dbz.update_stato_excel(input.getCod(), "1");
                    dbz.closeDB();
                    if (!o) {
                        log.severe("ERRORE DURANTE LA LETTURA E IL CARICAMENTO DEI DATI DELL'EXCEL DEI TASSI. CONTROLLARE.");
                    } else {
                        log.warning("DATI DALL'EXCEL CARICATI CON SUCCESSO");
                    }
                } else {
                    //DELETE AGGIORNAMENTI E ALTRO
                    log.severe("ERRORE DURANTE LA LETTURA E IL CARICAMENTO DEI DATI DELL'EXCEL DEI TASSI. CONTROLLARE.");
                }
            });
        } catch (Exception e) {
            log.log(Level.SEVERE, "ERRORE: {0}", e.getMessage());
        }
    }
    

//    public static void main(String[] args) {
////        new EngineExcel().test_excel();
//        new EngineExcel().insert_value_excel();
//    }

    private static List<ValueExcel> getReadEXCELCURRENCY(byte[] ing) {
        try {
            List<ValueExcel> al;
            try (ByteArrayInputStream in = new ByteArrayInputStream(ing); XSSFWorkbook workbook = new XSSFWorkbook(in)) {
                XSSFSheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                al = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Iterator<Cell> cellIterator = row.cellIterator();
                    String v[] = new String[2];
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();
                        if (cell.getColumnIndex() == 0) {
                            v[0] = getValueCell(cell);
                        }
                        if (cell.getColumnIndex() == 1) {
                            v[1] = getValueCell(cell);
                        }
                    }
                    if (v[0] != null) {
                        if (!v[0].trim().equals("")) {
                            al.add(new ValueExcel(v[0], v[1]));
                        }
                    }
                }              }
            return al;
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return null;
    }

    private static String getValueCell(Cell c1) {
        switch (c1.getCellType()) {
            case BOOLEAN:
                return String.valueOf(c1.getBooleanCellValue()).trim();
            case NUMERIC:
                return String.valueOf(c1.getNumericCellValue()).trim();
            case STRING:
                return c1.getStringCellValue().trim();
        }
        return "";
    }

    private void test_excel() {
        try {
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            List<WaitingExcel> ing = db.getExceldaElaborare();
            db.closeDB();

            if (!ing.isEmpty()) {
                log.log(Level.WARNING, "TROVATI {0} EXCEL DA ELABORARE", ing.size());
            }

//            for (int x = 0; x < ing.size(); x++) {
            ing.forEach(input -> {

                AtomicInteger error = new AtomicInteger(0);
                List<ValueExcel> listcurr = getReadEXCELCURRENCY(Base64.decodeBase64(input.getFileout()));
                if (listcurr != null) {
                    DBHost dbm0 = new DBHost(host_h, user_h, pwd_h, log);
                    ArrayList<String> libr = dbm0.list_branch_enabled();
                    dbm0.closeDB();
                    DBHost dbm1 = new DBHost(host_h, user_h, pwd_h, log);
                    listcurr.forEach(cu1 -> {
//                    for (int i = 0; i < listcurr.size(); i++) {
//                        for (int j = 0; j < libr.size(); j++) {
                        libr.forEach(br1 -> {
                            String[] valorivaluta = dbm1.get_currency_filiale(br1, cu1.getV1());

                            double d_rifbce = fd(cu1.getV2());
                            String buy_perc = valorivaluta[2];
                            String sel_perc = valorivaluta[4];
                            double d_standard_b = fd(buy_perc);
                            double d_standard_s = fd(sel_perc);
                            String tot_st_b = formatMysqltoDisplay(roundDoubleandFormat((d_rifbce
                                    * (100.0D + d_standard_b) / 100.0D), 8));
                            String tot_st_s = formatMysqltoDisplay(roundDoubleandFormat((d_rifbce
                                    * (100.0D + d_standard_s) / 100.0D), 8));
                            String msg_rh = "Upload excel bce. <br>BCE value " + formatMysqltoDisplay(cu1.getV2())
                                    + "<br>Buy Std: " + tot_st_b + "<br>Sell Std: " + tot_st_s + "<br>Date validity: " + input.getDt_start();
                            System.out.println(br1 + " (-) " + cu1.getV1() + " (-) " + msg_rh);
                        });
                    });
                    dbm1.closeDB();
                } else {
                    error.addAndGet(1);
                }

            });
        } catch (Exception e) {
            log.log(Level.SEVERE, "ERRORE: {0}", e.getMessage());
        }
    }
    
}

class ValueExcel {

    String v1, v2;

    public ValueExcel(String v1, String v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public String getV1() {
        return v1;
    }

    public void setV1(String v1) {
        this.v1 = v1;
    }

    public String getV2() {
        return v2;
    }

    public void setV2(String v2) {
        this.v2 = v2;
    }

}

class WaitingExcel {

    String cod, fileout, user, dt_start, data;

    public WaitingExcel(String cod, String fileout, String user, String dt_start, String data) {
        this.cod = cod;
        this.fileout = fileout;
        this.user = user;
        this.dt_start = dt_start;
        if (host_h.contains("uk")) {
            this.data = parseStringDate(data, patternsqldate).minusHours(1).toString(patternsqldate);
        } else {
            this.data = data;
        }
        System.out.println("update.WaitingExcel.<init>() " + this.data);
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getFileout() {
        return fileout;
    }

    public void setFileout(String fileout) {
        this.fileout = fileout;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDt_start() {
        return dt_start;
    }

    public void setDt_start(String dt_start) {
        this.dt_start = dt_start;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

}
