/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.spreadexcel;

import static rc.soop.spreadexcel.Utility.createLog;
import static rc.soop.spreadexcel.Utility.pattern4;
import static rc.soop.spreadexcel.Utility.rb;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author rcosco
 */
public class SpreadExcel {

    private static final Logger log = createLog("Mac2.0_EXCELSPREAD_", rb.getString("path.log"), pattern4);

    private static final String host = Utility.rb.getString("db.ip") + "/" + rb.getString("db.name");

    public static void engine() {
        Db db = new Db(host, false);
        ArrayList<String> li = db.list_cod_branch_enabled();
        ArrayList<String[]> ing = db.getExceldaElaborare();
        db.closeDB();

        ArrayList<String[]> complete = new ArrayList<>();
        try {
            for (int y = 0; y < ing.size(); y++) {
                String[] input = ing.get(y);
                String cod = input[0];
                String base64 = input[1];
                String user = input[2];
                String dt_start = input[3];
                ByteArrayInputStream in = new ByteArrayInputStream(Base64.decodeBase64(base64));
                XSSFWorkbook myWorkBook = new XSSFWorkbook(in);
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
                                if (!cell.getStringCellValue().trim().equals("")) {
                                    val[index] = cell.getStringCellValue();

                                }
                                break;
                            case NUMERIC:
                                val[index] = String.valueOf(parseIntR(String.valueOf(cell.getNumericCellValue())));
                                break;
                            case BOOLEAN:
                                val[index] = String.valueOf(cell.getBooleanCellValue());
                                break;
                            case BLANK:
                                break;
                            default:
                                break;
                        }
                    }
                    if (val[0] != null) {
                        complete.add(val);
                    }
                }
                in.close();
                boolean ok = false;
                for (int x = 0; x < li.size(); x++) {
                    String filialedamod = li.get(x);
                    for (int c = 0; c < complete.size(); c++) {
                        String[] val = complete.get(c);
                        if (val[0].equals(filialedamod)) {
                            Db dbq = new Db(host, false);
                            boolean es = dbq.updateCurrency(val, user, dt_start);
                            dbq.closeDB();
                            if (!es) {
                                ok = false;
                                break;
                            } else {
                                ok = true;
                            }
                        }
                    }
                }

                if (ok) {
                    Db dbz = new Db(host, false);
                    boolean o = dbz.update_stato_excel(cod, "4");
                    dbz.closeDB();
                    if (!o) {
                        log.severe("ERRORE DURANTE LA LETTURA E IL CARICAMENTO DEI DATI DELL'EXCEL DEI TASSI. CONTROLLARE.");
                    } else {
                        log.warning("DATI DALL'EXCEL CARICATI CON SUCCESSO");
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static int parseIntR(String value) {
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        int d1;
        try {
            d1 = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            d1 = 0;
        }
        return d1;
    }

//    public static void main(String[] args) {
//        excelspread(host);
//    }

}
