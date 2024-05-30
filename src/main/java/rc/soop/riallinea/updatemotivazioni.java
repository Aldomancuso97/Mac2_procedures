/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.ss.usermodel.Cell;
import static org.apache.poi.ss.usermodel.CellType.NUMERIC;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class updatemotivazioni {

//    public static void main(String[] args) {
//        try {
//            File myFile = new File("E:\\mnt\\C_DailyError_18_19_COMPLETO.xlsx");
//            FileInputStream fis = new FileInputStream(myFile);
//            XSSFWorkbook workbook = new XSSFWorkbook(fis);
//
//            int numerofogli = workbook.getNumberOfSheets();
//
//            List<Rep> repfiliale = new ArrayList<>();
//
//            for (int i = 0; i < numerofogli; i++) {
//                XSSFSheet mySheet = workbook.getSheetAt(i);
//                XSSFRow row1 = mySheet.getRow(0);
//
//                String filiale = row1.getCell(1).getStringCellValue().split(" ")[1].trim();
//
//                Iterator<Row> rowIterator = mySheet.iterator();
//
//                while (rowIterator.hasNext()) {
//
//                    XSSFRow row = (XSSFRow) rowIterator.next();
//                    if (row.getRowNum() > 2) {
//                        Rep r1 = new Rep();
//                        r1.setFiliale(filiale);
////                        Rep
//                        Iterator<Cell> cellIterator = row.cellIterator();
//                        while (cellIterator.hasNext()) {
//                            XSSFCell cell = (XSSFCell)cellIterator.next();
//                            String value = "";
//                            switch (cell.getCellType()) {
//                                case NUMERIC:
//                                    if (DateUtil.isCellDateFormatted(cell)) {
//                                        value = new DateTime(cell.getDateCellValue().getTime()).toString("dd/MM/yyyy");
//                                    } else {
//                                        value = String.valueOf(cell.getNumericCellValue());
//                                    }
//                                    break;
//                                case STRING:
//                                    value = cell.getStringCellValue();
//                                    break;
//                            }
//
//                            switch (cell.getColumnIndex()) {
//                                case 1:
//                                    r1.setData(value);
//                                case 5:
//                                    r1.setTipo(value);
//                                case 6:
//                                    r1.setNote(value);
//                            }
//
//////                            switch (cell.getCellType()) {
//////                                case Cell.CELL_TYPE_FORMULA:
//////                                    switch (cell.getCachedFormulaResultType()) {
//////                                        case Cell.CELL_TYPE_STRING:
//////                                            System.out.println(cell.getRichStringCellValue().getString());
//////                                            break;
//////                                        case Cell.CELL_TYPE_NUMERIC:
//////                                            if (DateUtil.isCellDateFormatted(cell)) {
//////                                                System.out.println(cell.getDateCellValue() + "");
//////                                            } else {
//////                                                System.out.println(cell.getNumericCellValue());
//////                                            }
//////                                            break;
//////                                    }
//////                                    break;
//////                                default:
//////                            }
//                        }
//                        repfiliale.add(r1);
////                        System.out.println(r1.toString());
//                    }
//                }
//            }
//
//            repfiliale.forEach(r1 -> {
//                if (r1.getNote() == null || r1.getNote().equals("")) {
//
//                } else {
//                    String upd = "UPDATE macreport.dailyerror SET tipo = '"
//                            + r1.getTipo() + "', note='"
//                            + r1.getNote() + "' WHERE filiale='" 
//                            + r1.getFiliale() + "' AND data = '" 
//                            + r1.getNote() + "'";
//                    System.out.println(upd);
//                }
//            });
//
//            workbook.close();
//            fis.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
}
