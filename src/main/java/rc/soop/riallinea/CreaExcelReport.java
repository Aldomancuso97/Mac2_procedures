/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package rc.soop.riallinea;

import com.google.common.util.concurrent.AtomicDouble;
import rc.soop.riallinea.Db_Master;
import rc.soop.riallinea.Util;
import static rc.soop.riallinea.Util.fd;
import java.io.FileOutputStream;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FontUnderline;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author rcosco
 */
public class CreaExcelReport {
    
    
    
//    public static void main(String[] args) {
//        List<Rep> out = new ArrayList<>();
//        try {
//            Db_Master db1 = new Db_Master();
//            Statement st1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//            String sql1 = "SELECT * FROM macreport.dailyerror ORDER BY filiale,STR_TO_DATE(DATA, '%d/%c/%Y')";
//            ResultSet rs1 = st1.executeQuery(sql1);
//            while (rs1.next()) {
//                out.add(new Rep(rs1.getString(1), rs1.getString(2), rs1.getString(3), rs1.getString(4), rs1.getString(5), rs1.getString(6), rs1.getString(7)));
//            }
//            rs1.close();
//            st1.close();
//            db1.closeDB();
//        } catch (Exception e) {
//            System.out.println("ERR: "+ExceptionUtils.getStackTrace(e));
//        }
//
//        List<String> solofiliali = out.stream().map(r -> r.getFiliale()).distinct().collect(Collectors.toList());
//
//        if (!solofiliali.isEmpty()) {
//            XSSFWorkbook workbook = new XSSFWorkbook();
//
//            XSSFFont font1 = workbook.createFont();
//            font1.setFontName("Calibri");
//            font1.setFontHeight(11.00);
//            font1.setBold(true);
//            font1.setUnderline(FontUnderline.SINGLE);
//            
//            XSSFFont font2 = workbook.createFont();
//            font2.setFontName("Calibri");
//            font2.setFontHeight(11.00);
//            font2.setBold(false);
//
//            XSSFCellStyle style1 = workbook.createCellStyle();
//            style1.setFont(font1);
//            style1.setAlignment(HorizontalAlignment.LEFT);
//            style1.setBorderTop(BorderStyle.THIN);
//            style1.setBorderRight(BorderStyle.THIN);
//            style1.setBorderBottom(BorderStyle.THIN);
//            style1.setBorderLeft(BorderStyle.THIN);
//
//            XSSFCellStyle style2 = workbook.createCellStyle();
//            style2.setAlignment(HorizontalAlignment.LEFT);
//            style2.setBorderTop(BorderStyle.THIN);
//            style2.setBorderRight(BorderStyle.THIN);
//            style2.setBorderBottom(BorderStyle.THIN);
//            style2.setBorderLeft(BorderStyle.THIN);
//            style2.setFont(font2);
//            XSSFCellStyle style3 = workbook.createCellStyle();
//            XSSFDataFormat hssfDataFormat = workbook.createDataFormat();
//            style3.setDataFormat(hssfDataFormat.getFormat("#,#.00"));
//            style3.setAlignment(HorizontalAlignment.RIGHT);
//            style3.setBorderTop(BorderStyle.THIN);
//            style3.setBorderRight(BorderStyle.THIN);
//            style3.setBorderBottom(BorderStyle.THIN);
//            style3.setBorderLeft(BorderStyle.THIN);
//            style3.setFont(font2);
//
//            solofiliali.forEach(f -> {
//
//                List<Rep> content = out.stream().filter(r -> r.getFiliale().equals(f)).collect(Collectors.toList());
//                if (!content.isEmpty()) {
//                    
//                    AtomicDouble ad1 = new AtomicDouble(0.0);
//                    
//                    XSSFSheet sheet = workbook.createSheet(f);
//                    //INDICE
//
//                    XSSFRow row0 = sheet.createRow(0);
//                    XSSFCell c0 = row0.createCell(1);
//                    c0.setCellStyle(style1);
//                    c0.setCellValue("FILIALE: " + f);
//
//                    XSSFRow rowP = sheet.createRow(2);
//                    XSSFCell c1 = rowP.createCell(1);
//                    c1.setCellStyle(style1);
//                    c1.setCellValue("DATA");
//                    XSSFCell c2 = rowP.createCell(2);
//                    c2.setCellValue("REPORT BSI");
//                    c2.setCellStyle(style1);
//                    XSSFCell c3 = rowP.createCell(3);
//                    c3.setCellValue("REPORT DAILY");
//                    c3.setCellStyle(style1);
//                    XSSFCell c4 = rowP.createCell(4);
//                    c4.setCellValue("DIFFERENZA €");
//                    c4.setCellStyle(style1);
//                    XSSFCell c5 = rowP.createCell(5);
//                    c5.setCellValue("TIPO TR");
//                    c5.setCellStyle(style1);
//                    XSSFCell c6 = rowP.createCell(6);
//                    c6.setCellValue("NOTE");
//                    c6.setCellStyle(style1);
//                    AtomicInteger int1 = new AtomicInteger(3);
//                    content.forEach(c -> {
//                        XSSFRow rowD = sheet.createRow(int1.get());
//                        int1.addAndGet(1);
//                        
//                        if(!c.getData().contains("/2020")){
//                            ad1.addAndGet(fd(c.getDiff()));
//                        }
//                        
//                        XSSFCell ca1 = rowD.createCell(1);
//                        ca1.setCellStyle(style2);
//                        ca1.setCellValue(c.getData());
//                        XSSFCell ca2 = rowD.createCell(2);
//                        ca2.setCellStyle(style3);
//                        ca2.setCellValue(fd(c.getBsi()));
//                        XSSFCell ca3 = rowD.createCell(3);
//                        ca3.setCellStyle(style3);
//                        ca3.setCellValue(fd(c.getDai()));
//                        XSSFCell ca4 = rowD.createCell(4);
//                        ca4.setCellStyle(style3);
//                        ca4.setCellValue(fd(c.getDiff()));
//                        XSSFCell ca5 = rowD.createCell(5);
//                        ca5.setCellStyle(style2);
//                        ca5.setCellValue("");
//                        XSSFCell ca6 = rowD.createCell(6);
//                        ca6.setCellStyle(style2);
//                        ca6.setCellValue("");
//                    });
//                    
//                    XSSFCell cd0 = row0.createCell(3);
//                    cd0.setCellStyle(style1);
//                    cd0.setCellValue("ERRORE 01/01/2020:");
//                    XSSFCell cd1 = row0.createCell(4);
//                    cd1.setCellStyle(style3);
//                    cd1.setCellValue(fd(Util.roundDoubleandFormat(ad1.get(), 2)));
//                    
//                    
//                    sheet.autoSizeColumn(1);
//                    sheet.autoSizeColumn(2);
//                    sheet.autoSizeColumn(3);
//                    sheet.autoSizeColumn(4);
//                    sheet.autoSizeColumn(5);
//                    sheet.autoSizeColumn(6);
//                }
//            });
//            try {
//
//                FileOutputStream xls = new FileOutputStream("C:\\mnt\\temp\\Error Recap.xlsx");
//                workbook.write(xls);
//                xls.close();
//                
//            } catch (Exception e) {
//                System.out.println("ERR: "+ExceptionUtils.getStackTrace(e));
//            }
//
////        XSSFWorkbook workbook = new XSSFWorkbook();
////        XSSFSheet sheet = workbook.createSheet("TillTransactionList");
//        }
//    }
}

class Rep {

    String filiale, data, bsi, dai, diff, tipo, note;   

    public Rep() {
    }
    
    
    
    public Rep(String filiale, String data, String bsi, String dai, String diff, String tipo, String note) {
        this.filiale = filiale;
        this.data = data;
        this.bsi = bsi;
        this.dai = dai;
        this.diff = diff;
        this.tipo = tipo;
        this.note = note;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getBsi() {
        return bsi;
    }

    public void setBsi(String bsi) {
        this.bsi = bsi;
    }

    public String getDai() {
        return dai;
    }

    public void setDai(String dai) {
        this.dai = dai;
    }

    public String getDiff() {
        return diff;
    }

    public void setDiff(String diff) {
        this.diff = diff;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
    }
    
}
