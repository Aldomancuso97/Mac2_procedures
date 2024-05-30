/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import com.google.common.collect.Iterators;
import rc.soop.esolver.Branch;
import rc.soop.esolver.NC_category;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.formatAL;
import static rc.soop.esolver.Util.formatStringtoStringDate;
import static rc.soop.esolver.Util.parseIntR;
import static rc.soop.esolver.Util.patternsqldate;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import static rc.soop.maintenance.Db_Master.roundDouble;
import static rc.soop.rilasciofile.Utility.divisione_controllozero;
import static rc.soop.rilasciofile.Utility.formatALN;
import static rc.soop.rilasciofile.Utility.formatALNC_category;
import static rc.soop.rilasciofile.Utility.formatALNC_causal;
import static rc.soop.rilasciofile.Utility.formatALNC_causal_ncde;
import static rc.soop.rilasciofile.Utility.formatBankBranch;
import static rc.soop.rilasciofile.Utility.formatCountry;
import static rc.soop.rilasciofile.Utility.formatDoubleforMysql;
import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
import static rc.soop.rilasciofile.Utility.formatSex;
import static rc.soop.rilasciofile.Utility.getStringCurrency;
import static rc.soop.rilasciofile.Utility.getStringFigures;
import static rc.soop.rilasciofile.Utility.get_Branch;
import static rc.soop.rilasciofile.Utility.parseDoubleR;
import static rc.soop.rilasciofile.Utility.patternnormdate;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFClientAnchor;
import org.apache.poi.hssf.usermodel.HSSFFont;
import static org.apache.poi.hssf.usermodel.HSSFFont.FONT_ARIAL;
import org.apache.poi.hssf.usermodel.HSSFPatriarch;
import org.apache.poi.hssf.usermodel.HSSFPicture;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import static org.apache.poi.ss.usermodel.BorderStyle.THICK;
import static org.apache.poi.ss.usermodel.BorderStyle.THIN;
import static org.apache.poi.ss.usermodel.CellType.NUMERIC;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.IndexedColorMap;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 *
 * @author rcosco
 */
public class Excel {

    private static final String formatdataCelINT = "0";

    // private static final float[] columnWidths2 = new float[]{4f, 3f, 3f,6f, 8f,
    // 8f, 3f, 6f, 6f, 6f, 6f, 6.5f, 3f, 5f, 5f, 11f, 5f, 7f};
    // private static final float[] columnWidths4 = new float[]{3f, 6f, 6f, 5f,
    // 3.5f, 6f, 6f, 6f, 3.5f, 6f, 6f, 6f, 3.5f, 8f, 8f, 4f, 5f, 3.5f, 5f, 5f, 5f};
    //
    public static String excel_openclose(GeneraFile gf, File outputfile, ArrayList<Openclose> result,
            ArrayList<Till> listTill_complete) {

        try {
            try (Workbook wb = new XSSFWorkbook()) {
                Sheet sheet = wb.createSheet("OpenClose_list");
                XSSFCellStyle style = (XSSFCellStyle) wb.createCellStyle();
                style.setBorderBottom(BorderStyle.THIN);
                style.setBorderTop(BorderStyle.THIN);
                style.setBorderRight(BorderStyle.THIN);
                style.setBorderLeft(BorderStyle.THIN);
                XSSFCellStyle style1 = (XSSFCellStyle) wb.createCellStyle();
                style1.setBorderBottom(BorderStyle.THIN);
                style1.setBorderTop(BorderStyle.THIN);
                style1.setBorderRight(BorderStyle.THIN);
                style1.setBorderLeft(BorderStyle.THIN);
                style1.setAlignment(HorizontalAlignment.CENTER);
                XSSFFont font = (XSSFFont) wb.createFont();
                font.setFontName(HSSFFont.FONT_ARIAL);
                font.setFontHeightInPoints((short) 12);
                font.setBold(true);
                style1.setFont(font);
                XSSFCellStyle style2 = (XSSFCellStyle) wb.createCellStyle();
                style2.setAlignment(HorizontalAlignment.CENTER);
                style2.setBorderBottom(BorderStyle.THIN);
                style2.setBorderTop(BorderStyle.THIN);
                style2.setBorderRight(BorderStyle.THIN);
                style2.setBorderLeft(BorderStyle.THIN);
                XSSFFont font2 = (XSSFFont) wb.createFont();
                font2.setFontName(HSSFFont.FONT_ARIAL);
                font2.setFontHeightInPoints((short) 14);
                font2.setBold(true);
                style2.setFont(font2);
                Row row0 = sheet.createRow(0);
                Cell cell0 = row0.createCell(1);
                cell0.setCellStyle(style2);
                sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 7));
                cell0.setCellValue("OPEN/CLOSE List");

                XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
                XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
                cellStylenum.setDataFormat(hssfDataFormat.getFormat("#,#.00"));
                // private static final String formatdataCell = "#.#0,0";
                cellStylenum.setBorderBottom(BorderStyle.THIN);
                cellStylenum.setBorderTop(BorderStyle.THIN);
                cellStylenum.setBorderRight(BorderStyle.THIN);
                cellStylenum.setBorderLeft(BorderStyle.THIN);

                XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
                cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
                cellStyleint.setBorderBottom(BorderStyle.THIN);
                cellStyleint.setBorderTop(BorderStyle.THIN);
                cellStyleint.setBorderRight(BorderStyle.THIN);
                cellStyleint.setBorderLeft(BorderStyle.THIN);

                Row row = sheet.createRow(1);
                Cell cell11 = row.createCell(1);
                cell11.setCellValue("Branch ID");
                cell11.setCellStyle(style1);
                Cell cell12 = row.createCell(2);
                cell12.setCellValue("Code");
                cell12.setCellStyle(style1);
                Cell cell13 = row.createCell(3);
                cell13.setCellValue("Date");
                cell13.setCellStyle(style1);
                Cell cell14 = row.createCell(4);
                cell14.setCellValue("Operator");
                cell14.setCellStyle(style1);
                Cell cell15 = row.createCell(5);
                cell15.setCellValue("Till");
                cell15.setCellStyle(style1);
                Cell cell16 = row.createCell(6);
                cell16.setCellValue("Type");
                cell16.setCellStyle(style1);
                Cell cell17 = row.createCell(7);
                cell17.setCellValue("Error");
                cell17.setCellStyle(style1);

                for (int i = 0; i < result.size(); i++) {
                    row = sheet.createRow(i + 3);
                    Openclose res = result.get(i);
                    cell11 = row.createCell(1);
                    cell11.setCellValue(res.getFiliale());
                    cell11.setCellStyle(style);
                    cell12 = row.createCell(2);
                    cell12.setCellValue(res.getId());
                    cell12.setCellStyle(style);
                    cell13 = row.createCell(3);
                    cell13.setCellValue(formatStringtoStringDate(res.getData(), patternsqldate, patternnormdate));
                    cell13.setCellStyle(style);
                    cell14 = row.createCell(4);
                    cell14.setCellValue(res.getUser());
                    cell14.setCellStyle(style);
                    cell15 = row.createCell(5);
                    cell15.setCellValue(listTill_complete.stream().filter(t1 -> t1.getFiliale().equals(res.getFiliale())
                            && t1.getCod().equals(res.getTill())).findFirst().orElse(new Till("-", "-", "-"))
                            .getDe_till());
                    cell15.setCellStyle(style);
                    cell16 = row.createCell(6);
                    cell16.setCellValue(res.formatType_cru(res.getFg_tipo()));
                    cell16.setCellStyle(style);
                    cell17 = row.createCell(7);
                    cell17.setCellValue(res.getErrors());
                    cell17.setCellStyle(style);
                }

                sheet.autoSizeColumn(1);
                sheet.autoSizeColumn(2);
                sheet.autoSizeColumn(3);
                sheet.autoSizeColumn(4);
                sheet.autoSizeColumn(5);
                sheet.autoSizeColumn(6);
                sheet.autoSizeColumn(7);

                try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                    wb.write(fileOut);
                }
            }

            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static String excel_transaction_listEVO(GeneraFile gf, File outputfile, ArrayList<Ch_transaction> result) {
        try {
            try ( // File out = new File(pathout + outputfile);
                    Workbook wb = new XSSFWorkbook()) {
                Sheet sheet = wb.createSheet("Transaction_list_E");
                XSSFCellStyle style = (XSSFCellStyle) wb.createCellStyle();
                style.setBorderBottom(BorderStyle.THIN);
                style.setBorderTop(BorderStyle.THIN);
                style.setBorderRight(BorderStyle.THIN);
                style.setBorderLeft(BorderStyle.THIN);
                XSSFCellStyle style1 = (XSSFCellStyle) wb.createCellStyle();
                style1.setBorderBottom(BorderStyle.THIN);
                style1.setBorderTop(BorderStyle.THIN);
                style1.setBorderRight(BorderStyle.THIN);
                style1.setBorderLeft(BorderStyle.THIN);
                style1.setAlignment(HorizontalAlignment.CENTER);
                XSSFFont font = (XSSFFont) wb.createFont();
                font.setFontName(HSSFFont.FONT_ARIAL);
                font.setFontHeightInPoints((short) 12);
                font.setBold(true);
                // font.setColor(HSSFColor.BLUE.index);
                style1.setFont(font);
                XSSFCellStyle style2 = (XSSFCellStyle) wb.createCellStyle();
                style2.setAlignment(HorizontalAlignment.CENTER);
                XSSFFont font2 = (XSSFFont) wb.createFont();
                font2.setFontName(HSSFFont.FONT_ARIAL);
                font2.setFontHeightInPoints((short) 14);
                font2.setBold(true);
                style2.setFont(font2);
                Row row0 = sheet.createRow(0);
                Cell cell0 = row0.createCell(1);
                cell0.setCellStyle(style2);
                sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 9));
                cell0.setCellValue("Transaction List");

                XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
                XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
                cellStylenum.setDataFormat(hssfDataFormat.getFormat("#,#.00"));
                // private static final String formatdataCell = "#.#0,0";
                cellStylenum.setBorderBottom(BorderStyle.THIN);
                cellStylenum.setBorderTop(BorderStyle.THIN);
                cellStylenum.setBorderRight(BorderStyle.THIN);
                cellStylenum.setBorderLeft(BorderStyle.THIN);

                XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
                cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
                cellStyleint.setBorderBottom(BorderStyle.THIN);
                cellStyleint.setBorderTop(BorderStyle.THIN);
                cellStyleint.setBorderRight(BorderStyle.THIN);
                cellStyleint.setBorderLeft(BorderStyle.THIN);

                Row row = sheet.createRow(1);
                Cell cell14 = row.createCell(1);
                cell14.setCellValue("Status");
                cell14.setCellStyle(style1);
                Cell cell12 = row.createCell(2);
                cell12.setCellValue("Branch ID");
                cell12.setCellStyle(style1);
                Cell cell = row.createCell(3);
                cell.setCellValue("Code");
                cell.setCellStyle(style1);
                Cell cell2 = row.createCell(4);
                cell2.setCellValue("Date");
                cell2.setCellStyle(style1);
                Cell cell3 = row.createCell(5);
                cell3.setCellValue("Till");
                cell3.setCellStyle(style1);
                Cell cell4 = row.createCell(6);
                cell4.setCellValue("Operator");
                cell4.setCellStyle(style1);
                Cell cell6 = row.createCell(7);
                cell6.setCellValue("Type");
                cell6.setCellStyle(style1);
                Cell cell7 = row.createCell(8);
                cell7.setCellValue("Total");
                cell7.setCellStyle(style1);
                Cell cell8 = row.createCell(9);
                cell8.setCellValue("Net");
                cell8.setCellStyle(style1);
                Cell cell9 = row.createCell(10);
                cell9.setCellValue("Commission");
                cell9.setCellStyle(style1);
                Cell cell10 = row.createCell(11);
                cell10.setCellValue("Spread");
                cell10.setCellStyle(style1);
                Cell cell11 = row.createCell(12);
                cell11.setCellValue("Invoice");
                cell11.setCellStyle(style1);
                Cell cell122 = row.createCell(13);
                cell122.setCellValue("Credit Note");
                cell122.setCellStyle(style1);
                Cell cell13 = row.createCell(14);
                cell13.setCellValue("Date Deleted");
                cell13.setCellStyle(style1);
                Cell cell131 = row.createCell(15);
                cell131.setCellValue("Client Surname");
                cell131.setCellStyle(style1);
                Cell cell132 = row.createCell(16);
                cell132.setCellValue("Client Name");
                cell132.setCellStyle(style1);
                Cell cell133 = row.createCell(17);
                cell133.setCellValue("Client Tax Code");
                cell133.setCellStyle(style1);
                Cell cell133a = row.createCell(18);
                cell133a.setCellValue("Pos/Bank Account");
                cell133a.setCellStyle(style1);

                Cell celln1 = row.createCell(19);
                celln1.setCellValue("Currency");
                celln1.setCellStyle(style1);
                Cell celln2 = row.createCell(20);
                celln2.setCellValue("Figures");
                celln2.setCellStyle(style1);
                Cell celln3 = row.createCell(21);
                celln3.setCellValue("Nation of Birth");
                celln3.setCellStyle(style1);
                Cell celln4 = row.createCell(22);
                celln4.setCellValue("Nation of Residence");
                celln4.setCellStyle(style1);
                Cell celln5 = row.createCell(23);
                celln5.setCellValue("Address");
                celln5.setCellStyle(style1);
                Cell celln6 = row.createCell(24);
                celln6.setCellValue("Internet Booking");
                celln6.setCellStyle(style1);

                Cell celln_0 = row.createCell(25);
                celln_0.setCellValue("Date Of Birth");
                celln_0.setCellStyle(style1);
                Cell celln_1 = row.createCell(26);
                celln_1.setCellValue("Sex");
                celln_1.setCellStyle(style1);
                Cell celln_2 = row.createCell(27);
                celln_2.setCellValue("Phone Number");
                celln_2.setCellStyle(style1);
                Cell celln_3 = row.createCell(28);
                celln_3.setCellValue("Email");
                celln_3.setCellStyle(style1);
                Cell celln_4 = row.createCell(29);
                celln_4.setCellValue("Authorization to the processing of personal data");
                celln_4.setCellStyle(style1);
                Cell celln_5 = row.createCell(30);
                celln_5.setCellValue("Loyalty Code");
                celln_5.setCellStyle(style1);
                Cell celln_6 = row.createCell(31);
                celln_6.setCellValue("% Com.");
                celln_6.setCellStyle(style1);
                Cell celln_7 = row.createCell(32);
                celln_7.setCellValue("Zip Code");
                celln_7.setCellStyle(style1);

                DatabaseCons db = new DatabaseCons(gf);
                ArrayList<Figures> figu = db.list_all_figures();
                ArrayList<Currency> curr = db.list_currency("000");
                ArrayList<String[]> city = db.city_Italy_APM();
                ArrayList<String[]> country = db.country();
                db.closeDB();

                DatabaseCons db2 = new DatabaseCons(gf);
                for (int i = 0; i < result.size(); i++) {
                    Row row2 = sheet.createRow(i + 3);
                    Ch_transaction res = (Ch_transaction) result.get(i);

                    Client cl = db2.query_Client_transaction(res.getCod(), res.getCl_cod());
                    String loy = db2.query_LOY_transaction(res.getCod());
                    if (loy == null) {
                        loy = "";
                    }

                    String dt_del = "";
                    if (res.getDel_fg().equals("1")) {
                        dt_del = Utility.formatStringtoStringDate(res.getDel_dt().split("\\.")[0],
                                "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss");
                    }
                    String pos = res.getPos();
                    String ca = "";
                    ArrayList<String> cur = new ArrayList<>();
                    ArrayList<String> fig = new ArrayList<>();
                    ArrayList<Ch_transaction_value> va = db2.query_transaction_value(res.getCod());
                    for (int f = 0; f < va.size(); f++) {
                        cur.add(va.get(f).getValuta());
                        fig.add(va.get(f).getSupporto());
                        if (va.get(f).getSupporto().equals("04")) {
                            pos = va.get(f).getPos();
                            ca = " CA ";
                        }
                    }

                    Cell cell140 = row2.createCell(1);
                    cell140.setCellValue(res.formatStatus_cru(res.getDel_fg()));
                    cell140.setCellStyle(style);
                    Cell cell1b = row2.createCell(2);
                    cell1b.setCellValue(res.getFiliale());
                    cell1b.setCellStyle(style);
                    Cell cell1 = row2.createCell(3);
                    cell1.setCellValue(res.getId());
                    cell1.setCellStyle(style);
                    Cell cell22 = row2.createCell(4);
                    cell22.setCellValue(Utility.formatStringtoStringDate(res.getData().split("\\.")[0],
                            "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss"));
                    cell22.setCellStyle(style);
                    Cell cell33 = row2.createCell(5);
                    cell33.setCellValue(res.getTill());
                    cell33.setCellStyle(style);
                    Cell cell44 = row2.createCell(6);
                    cell44.setCellValue(res.getUser());
                    cell44.setCellStyle(style);
                    Cell cell66 = row2.createCell(7);
                    cell66.setCellValue(Ch_transaction.formatType(res.getTipotr()) + ca);
                    cell66.setCellStyle(style);
                    Cell cell77 = row2.createCell(8);
                    cell77.setCellType(CellType.NUMERIC);
                    cell77.setCellValue(fd(res.getTotal()));
                    cell77.setCellStyle(cellStylenum);

                    Cell cell88 = row2.createCell(9);
                    cell88.setCellValue(fd(res.getPay()));
                    cell88.setCellType(CellType.NUMERIC);
                    cell88.setCellStyle(cellStylenum);
                    Cell cell99 = row2.createCell(10);
                    cell99.setCellValue(fd(res.getCommission()));
                    cell99.setCellType(CellType.NUMERIC);
                    cell99.setCellStyle(cellStylenum);

                    Cell cell100 = row2.createCell(11);
                    cell100.setCellValue(fd(res.getSpread_total()));
                    cell100.setCellType(CellType.NUMERIC);
                    cell100.setCellStyle(cellStylenum);

                    Cell cell110 = row2.createCell(12);
                    cell110.setCellValue(res.getFa_number());
                    cell110.setCellStyle(style);
                    Cell cell120 = row2.createCell(13);
                    cell120.setCellValue(res.getCn_number());
                    cell120.setCellStyle(style);
                    Cell cell130 = row2.createCell(14);
                    cell130.setCellValue(dt_del);
                    cell130.setCellStyle(style);
                    Cell cell1301 = row2.createCell(15);
                    cell1301.setCellValue(cl.getCognome().toUpperCase());
                    cell1301.setCellStyle(style);
                    Cell cell1302 = row2.createCell(16);
                    cell1302.setCellValue(cl.getNome().toUpperCase());
                    cell1302.setCellStyle(style);
                    Cell cell1303 = row2.createCell(17);
                    cell1303.setCellValue(cl.getCodfisc().toUpperCase());
                    cell1303.setCellStyle(style);
                    Cell cell1303a = row2.createCell(18);

                    cell1303a.setCellValue(pos);
                    cell1303a.setCellStyle(style);

                    Cell celln11 = row2.createCell(19);
                    celln11.setCellValue(getStringCurrency(cur, curr));
                    celln11.setCellStyle(style);

                    Cell celln12 = row2.createCell(20);
                    celln12.setCellValue(getStringFigures(fig, figu));
                    celln12.setCellStyle(style);

                    Cell celln13 = row2.createCell(21);
                    celln13.setCellValue(formatCountry(cl.getNazione_nascita(), country).toUpperCase());
                    celln13.setCellStyle(style);
                    Cell celln14 = row2.createCell(22);
                    celln14.setCellValue(formatCountry(cl.getNazione(), country).toUpperCase());
                    celln14.setCellStyle(style);

                    Cell celln15 = row2.createCell(23);
                    celln15.setCellValue(cl.getIndirizzo() + " - " + formatALN(cl.getCitta(), city, 1).toUpperCase());
                    celln15.setCellStyle(style);

                    Cell celln16 = row2.createCell(24);
                    if (res.getIntbook().equals("0")) {
                        celln16.setCellValue("NO");
                    } else {
                        celln16.setCellValue("SI");
                    }
                    celln16.setCellStyle(style);

                    Cell celln1_0 = row2.createCell(25);
                    celln1_0.setCellValue(cl.getDt_nascita());
                    celln1_0.setCellStyle(style);
                    Cell celln1_1 = row2.createCell(26);
                    celln1_1.setCellValue(formatSex(cl.getSesso()).toUpperCase());
                    celln1_1.setCellStyle(style);
                    Cell celln1_2 = row2.createCell(27);
                    celln1_2.setCellValue(cl.getTelefono());
                    celln1_2.setCellStyle(style);
                    Cell celln1_3 = row2.createCell(28);
                    celln1_3.setCellValue(cl.getEmail());
                    celln1_3.setCellStyle(style);
                    Cell celln1_4 = row2.createCell(29);
                    celln1_4.setCellValue("");
                    celln1_4.setCellStyle(style);
                    Cell celln1_5 = row2.createCell(30);
                    celln1_5.setCellValue(loy);
                    celln1_5.setCellStyle(style);
                    Cell celln1_6 = row2.createCell(31);
                    celln1_6.setCellValue(fd(res.getCom()));
                    celln1_6.setCellType(CellType.NUMERIC);
                    celln1_6.setCellStyle(cellStylenum);
                    Cell celln1_7 = row2.createCell(32);
                    celln1_7.setCellValue(cl.getCap());
                    celln1_7.setCellStyle(style);

                }
                db2.closeDB();
                sheet.autoSizeColumn(1);
                sheet.autoSizeColumn(2);
                sheet.autoSizeColumn(3);
                sheet.autoSizeColumn(4);
                sheet.autoSizeColumn(5);
                sheet.autoSizeColumn(6);
                sheet.autoSizeColumn(7);
                sheet.autoSizeColumn(8);
                sheet.autoSizeColumn(9);
                sheet.autoSizeColumn(10);
                sheet.autoSizeColumn(11);
                sheet.autoSizeColumn(12);
                sheet.autoSizeColumn(13);
                sheet.autoSizeColumn(14);
                sheet.autoSizeColumn(15);
                sheet.autoSizeColumn(16);
                sheet.autoSizeColumn(17);
                sheet.autoSizeColumn(18);
                sheet.autoSizeColumn(19);
                sheet.autoSizeColumn(20);
                sheet.autoSizeColumn(21);
                sheet.autoSizeColumn(22);
                sheet.autoSizeColumn(23);
                sheet.autoSizeColumn(24);

                sheet.autoSizeColumn(25);
                sheet.autoSizeColumn(26);
                sheet.autoSizeColumn(27);
                sheet.autoSizeColumn(28);
                sheet.autoSizeColumn(29);
                sheet.autoSizeColumn(30);
                sheet.autoSizeColumn(31);
                sheet.autoSizeColumn(32);

                try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                    wb.write(fileOut);
                }
            }

            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String excel_transactionnc_list(GeneraFile gf, File outputfile, ArrayList<NC_transaction> result) {
        try {

            DatabaseCons db = new DatabaseCons(gf);
            ArrayList<Branch> libr = db.list_branch();
            ArrayList<String[]> country = db.country();
            ArrayList<NC_category> array_nc_cat = db.list_ALL_nc_category("000");
            ArrayList<NC_causal> array_nc_caus = db.list_nc_causal_all("000");
            ArrayList<String[]> array_nc_descr = db.list_nc_descr();

            Workbook wb = new XSSFWorkbook();
            Sheet sheet = wb.createSheet("TransactionNC_list");

            XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
            XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
            cellStylenum.setDataFormat(hssfDataFormat.getFormat("#,#.00"));
            cellStylenum.setBorderBottom(BorderStyle.THIN);
            cellStylenum.setBorderTop(BorderStyle.THIN);
            cellStylenum.setBorderRight(BorderStyle.THIN);
            cellStylenum.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
            cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleint.setBorderBottom(BorderStyle.THIN);
            cellStyleint.setBorderTop(BorderStyle.THIN);
            cellStyleint.setBorderRight(BorderStyle.THIN);
            cellStyleint.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle style = (XSSFCellStyle) wb.createCellStyle();
            style.setBorderBottom(BorderStyle.THIN);
            style.setBorderTop(BorderStyle.THIN);
            style.setBorderRight(BorderStyle.THIN);
            style.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle style1 = (XSSFCellStyle) wb.createCellStyle();
            style1.setBorderBottom(BorderStyle.THIN);
            style1.setBorderTop(BorderStyle.THIN);
            style1.setBorderRight(BorderStyle.THIN);
            style1.setBorderLeft(BorderStyle.THIN);
            style1.setAlignment(HorizontalAlignment.CENTER);
            XSSFFont font = (XSSFFont) wb.createFont();
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 12);
            font.setBold(true);
            // font.setColor(HSSFColor.BLUE.index);
            style1.setFont(font);
            XSSFCellStyle style2 = (XSSFCellStyle) wb.createCellStyle();
            style2.setAlignment(HorizontalAlignment.CENTER);
            XSSFFont font2 = (XSSFFont) wb.createFont();
            font2.setFontName(HSSFFont.FONT_ARIAL);
            font2.setFontHeightInPoints((short) 14);
            font2.setBold(true);
            style2.setFont(font2);
            Row row0 = sheet.createRow(0);
            Cell cell0 = row0.createCell(1);
            cell0.setCellStyle(style2);
            sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 9));
            cell0.setCellValue("Transaction No Change List");
            Row row = sheet.createRow(1);

            Cell cell13 = row.createCell(1);
            cell13.setCellValue("Status");
            cell13.setCellStyle(style1);
            Cell cellb = row.createCell(2);
            cellb.setCellValue("Branch ID");
            cellb.setCellStyle(style1);
            Cell cell = row.createCell(3);
            cell.setCellValue("Code");
            cell.setCellStyle(style1);
            Cell cell2 = row.createCell(4);
            cell2.setCellValue("Date");
            cell2.setCellStyle(style1);
            Cell cell3 = row.createCell(5);
            cell3.setCellValue("Till");
            cell3.setCellStyle(style1);
            Cell cell4 = row.createCell(6);
            cell4.setCellValue("User");
            cell4.setCellStyle(style1);

            Cell cell6 = row.createCell(7);
            cell6.setCellValue("Total");
            cell6.setCellStyle(style1);
            Cell cell7 = row.createCell(8);
            cell7.setCellValue("Quantity");
            cell7.setCellStyle(style1);
            Cell cell8 = row.createCell(9);
            cell8.setCellValue("Price");
            cell8.setCellStyle(style1);
            Cell cell9 = row.createCell(10);
            cell9.setCellValue("Fee");
            cell9.setCellStyle(style1);
            Cell cell9a = row.createCell(11);
            cell9a.setCellValue("Category");
            cell9a.setCellStyle(style1);
            Cell cell10 = row.createCell(12);
            cell10.setCellValue("Causal");
            cell10.setCellStyle(style1);
            Cell cell11 = row.createCell(13);
            cell11.setCellValue("Client");
            cell11.setCellStyle(style1);
            Cell cell12 = row.createCell(14);
            cell12.setCellValue("Country");
            cell12.setCellStyle(style1);
            Cell cell121 = row.createCell(15);
            cell121.setCellValue("Type");
            cell121.setCellStyle(style1);
            Cell cell111 = row.createCell(16);
            cell111.setCellValue("Pos/Bank Account");
            cell111.setCellStyle(style1);

            cell111 = row.createCell(17);
            cell111.setCellValue("Fix Com");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(18);
            cell111.setCellValue("perc Com");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(19);
            cell111.setCellValue("Round");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(20);
            cell111.setCellValue("Total Com");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(21);
            cell111.setCellValue("Estimate Commissions");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(22);
            cell111.setCellValue("GM");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(23);
            cell111.setCellValue("Branch Description");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(24);
            cell111.setCellValue("Loyalty Code");
            cell111.setCellStyle(style1);

            cell111 = row.createCell(25);
            cell111.setCellValue("Phone number");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(26);
            cell111.setCellValue("Email");
            cell111.setCellStyle(style1);
            cell111 = row.createCell(27);
            cell111.setCellValue("Authorization to the processing of personal data");
            cell111.setCellStyle(style1);

            for (int i = 0; i < result.size(); i++) {
                Row row2 = sheet.createRow(i + 3);
                NC_transaction res = (NC_transaction) result.get(i);

                String q1 = (roundDoubleandFormat(fd(res.getQuantita()), 0));
                String p1 = (res.getPrezzo());
                String f1 = ("0.00");
                String pos = res.getPos();
                if (res.getFg_tipo_transazione_nc().equals("1")) {
                    q1 = "1";
                    p1 = (res.getNetto());
                    f1 = (res.getCommissione());
                } else if (res.getFg_tipo_transazione_nc().equals("3")) {
                    q1 = (roundDoubleandFormat(fd(res.getRicevuta()), 0));
                    p1 = (res.getQuantita());
                } else if (res.getFg_tipo_transazione_nc().equals("21")) {
                    String comm;
                    if (fd(res.getCommissione()) > 0) {
                        comm = res.getCommissione();
                    } else {
                        comm = res.getTi_ticket_fee();
                    }
                    if (res.getTotal().contains("-")) {
                        comm = "-" + comm;
                    }
                    f1 = (comm);
                }

                Cell cell130 = row2.createCell(1);
                cell130.setCellValue(res.formatStatus_cru(res.getDel_fg()));
                cell130.setCellStyle(style);
                Cell cell1b = row2.createCell(2);
                cell1b.setCellValue(res.getFiliale());
                cell1b.setCellStyle(style);
                Cell cell1 = row2.createCell(3);
                cell1.setCellValue(res.getId());
                cell1.setCellStyle(style);
                Cell cell22 = row2.createCell(4);
                cell22.setCellValue(formatStringtoStringDate(res.getData(), patternsqldate, patternnormdate));
                cell22.setCellStyle(style);
                Cell cell33 = row2.createCell(5);
                cell33.setCellValue(res.getTill());
                cell33.setCellStyle(style);
                Cell cell44 = row2.createCell(6);
                cell44.setCellValue(res.getUser());
                cell44.setCellStyle(style);
                Cell cell66 = row2.createCell(7);
                cell66.setCellType(CellType.NUMERIC);
                cell66.setCellValue(fd(res.getTotal()));

                // cell66.setCellValue(formatMysqltoDisplay(res.getTotal()));
                cell66.setCellStyle(cellStylenum);

                Cell cell77 = row2.createCell(8);
                cell77.setCellValue(fd(q1));
                cell77.setCellType(CellType.NUMERIC);
                cell77.setCellStyle(cellStyleint);

                Cell cell88 = row2.createCell(9);
                cell88.setCellValue(fd(p1));
                cell88.setCellType(CellType.NUMERIC);
                // cell66.setCellValue(formatMysqltoDisplay(res.getTotal()));
                cell88.setCellStyle(cellStylenum);

                Cell cell88a = row2.createCell(10);
                cell88a.setCellValue(fd(f1));
                cell88a.setCellType(CellType.NUMERIC);
                cell88a.setCellStyle(cellStylenum);

                Cell cell99 = row2.createCell(11);
                cell99.setCellValue(res.getGruppo_nc() + " - " + formatALNC_category(res.getGruppo_nc(), array_nc_cat));
                cell99.setCellStyle(style);
                Cell cell100 = row2.createCell(12);
                cell100.setCellValue(
                        res.getCausale_nc() + " - " + formatALNC_causal(res.getCausale_nc(), array_nc_caus));
                cell100.setCellStyle(style);
                Cell cell110 = row2.createCell(13);
                cell110.setCellValue(res.getCl_cognome() + " " + res.getCl_nome());
                cell110.setCellStyle(style);
                Cell cell120 = row2.createCell(14);
                cell120.setCellValue(formatAL(res.getCl_nazione(), country, 1));
                cell120.setCellStyle(style);
                Cell cell1201 = row2.createCell(15);
                cell1201.setCellValue(formatALNC_causal_ncde(res.getCausale_nc(), array_nc_caus, array_nc_descr));
                cell1201.setCellStyle(style);
                Cell cell1202 = row2.createCell(16);
                cell1202.setCellValue(pos);
                cell1202.setCellStyle(style);

                Ch_transaction cht = null;
                if (!res.getCh_transaction().equals("-")) {
                    try {
                        if (db.getC() == null || db.getC().isClosed()) {
                            db = new DatabaseCons(gf);
                        }
                        cht = db.query_transaction_ch_reportNC(res.getCh_transaction());
                    } catch (Exception e1) {
                        cht = null;
                    }
                }
                if (cht != null) {

                    double GM = fd(cht.getCommission()) + parseDoubleR(gf, cht.getRound()) + fd(cht.getSpread_total());
                    double stimaCO = parseDoubleR(gf, res.getTotal())
                            * (parseDoubleR(gf, cht.getCommission()) + parseDoubleR(gf, cht.getRound()))
                            / parseDoubleR(gf, cht.getPay());
                    double stimaGM = (GM * parseDoubleR(gf, res.getTotal())) / parseDoubleR(gf, cht.getPay());
                    cell1202 = row2.createCell(17);
                    cell1202.setCellValue(fd(cht.getFix()));
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                    cell1202 = row2.createCell(18);
                    cell1202.setCellValue(fd(cht.getCom()));
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                    cell1202 = row2.createCell(19);
                    cell1202.setCellValue(fd(cht.getRound()));
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                    cell1202 = row2.createCell(20);
                    cell1202.setCellValue(fd(cht.getCommission()));
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                    cell1202 = row2.createCell(21);
                    cell1202.setCellValue(stimaCO);
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                    cell1202 = row2.createCell(22);
                    cell1202.setCellValue(stimaGM);
                    cell1202.setCellType(CellType.NUMERIC);
                    cell1202.setCellStyle(cellStylenum);

                } else {
                    cell1202 = row2.createCell(17);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);
                    cell1202 = row2.createCell(18);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);
                    cell1202 = row2.createCell(19);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);
                    cell1202 = row2.createCell(20);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);
                    cell1202 = row2.createCell(21);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);
                    cell1202 = row2.createCell(22);
                    cell1202.setCellValue("");
                    cell1202.setCellStyle(style);

                }

                cell1202 = row2.createCell(23);
                cell1202.setCellValue(get_Branch(res.getFiliale(), libr).getDe_branch().toUpperCase());
                cell1202.setCellStyle(style);

                String loy;
                try {
                    if (db.getC() == null || db.getC().isClosed()) {
                        db = new DatabaseCons(gf);
                    }
                    loy = db.query_LOY_transaction(res.getCod());
                    if (loy == null) {
                        loy = "";
                    }
                } catch (Exception e1) {
                    loy = "";
                }
                cell1202 = row2.createCell(24);
                cell1202.setCellValue(loy);
                cell1202.setCellStyle(style);

                cell1202 = row2.createCell(25);
                cell1202.setCellValue(res.getCl_telefono());
                cell1202.setCellStyle(style);
                cell1202 = row2.createCell(26);
                cell1202.setCellValue(res.getCl_email());
                cell1202.setCellStyle(style);
                cell1202 = row2.createCell(27);
                cell1202.setCellValue("");
                cell1202.setCellStyle(style);

            }
            sheet.autoSizeColumn(1);
            sheet.autoSizeColumn(2);
            sheet.autoSizeColumn(3);
            sheet.autoSizeColumn(4);
            sheet.autoSizeColumn(5);
            sheet.autoSizeColumn(6);
            sheet.autoSizeColumn(7);
            sheet.autoSizeColumn(8);
            sheet.autoSizeColumn(9);
            sheet.autoSizeColumn(10);
            sheet.autoSizeColumn(11);
            sheet.autoSizeColumn(12);
            sheet.autoSizeColumn(13);
            sheet.autoSizeColumn(14);
            sheet.autoSizeColumn(15);
            sheet.autoSizeColumn(16);
            sheet.autoSizeColumn(17);
            sheet.autoSizeColumn(18);
            sheet.autoSizeColumn(19);
            sheet.autoSizeColumn(20);
            sheet.autoSizeColumn(21);
            sheet.autoSizeColumn(22);
            sheet.autoSizeColumn(23);
            sheet.autoSizeColumn(24);

            sheet.autoSizeColumn(25);
            sheet.autoSizeColumn(26);
            sheet.autoSizeColumn(27);

            try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                wb.write(fileOut);
                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
                db.closeDB();
                return base64;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return null;

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String BB_receiptexcel(File outputfile, TillTransactionListBB_value siq, String datereport1,
            String datereport2, String intestazionePdf) {

        float[] columnWidths2 = new float[] { 4f, 3f, 3f, 6f, 8f, 8f, 3f, 6f, 6f, 6f, 6f, 6.5f, 3f, 5f, 5f, 11f, 5f,
                7f };
        float[] columnWidths4 = new float[] { 3f, 6f, 6f, 5f, 3.5f, 6f, 6f, 6f, 3.5f, 6f, 6f, 6f, 3.5f, 8f, 8f, 4f, 5f,
                3.5f, 5f, 5f, 5f };
        //

        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("TillTransactionList");
        // CREAZIONE FONT
        XSSFFont font = workbook.createFont();
        font.setFontName(HSSFFont.FONT_ARIAL);
        font.setFontHeightInPoints((short) 12);
        font.setBold(true);

        XSSFCellStyle style1 = (XSSFCellStyle) workbook.createCellStyle();
        style1.setFont(font);

        XSSFFont font2 = workbook.createFont();
        font2.setFontName(HSSFFont.FONT_ARIAL);
        font2.setFontHeightInPoints((short) 12);

        XSSFCellStyle style2 = (XSSFCellStyle) workbook.createCellStyle();
        style2.setFont(font2);

        XSSFFont font3 = workbook.createFont();
        font3.setFontName(HSSFFont.FONT_ARIAL);
        font3.setFontHeightInPoints((short) 10);
        font3.setBold(true);

        XSSFCellStyle style3 = (XSSFCellStyle) workbook.createCellStyle();
        style3.setFont(font3);
        style3.setAlignment(HorizontalAlignment.RIGHT);
        style3.setBorderTop(BorderStyle.THIN);
        style3.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle style3left = (XSSFCellStyle) workbook.createCellStyle();
        style3left.setFont(font3);
        style3left.setAlignment(HorizontalAlignment.LEFT);
        style3left.setBorderTop(BorderStyle.THIN);
        style3left.setBorderBottom(BorderStyle.THIN);

        XSSFFont font4 = workbook.createFont();
        font4.setFontName(HSSFFont.FONT_ARIAL);
        font4.setFontHeightInPoints((short) 10);

        XSSFCellStyle style4 = (XSSFCellStyle) workbook.createCellStyle();
        style4.setAlignment(HorizontalAlignment.RIGHT);
        style4.setBorderTop(BorderStyle.THIN);
        style4.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle style4left = (XSSFCellStyle) workbook.createCellStyle();
        style4left.setAlignment(HorizontalAlignment.LEFT);
        style4left.setBorderTop(BorderStyle.THIN);
        style4left.setBorderBottom(BorderStyle.THIN);

        Row rowP = sheet.createRow(1);

        Cell cl = rowP.createCell(1);
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 1, 6));
        cl.setCellStyle(style1);

        cl.setCellValue(intestazionePdf + " From " + datereport1 + " To " + datereport2);

        // Cell cl2 = rowP.createCell(6);
        // cl2.setCellStyle(style2);
        // cl2.setCellValue(datereport1);
        Row row = sheet.createRow(3);
        row.createCell(1).setCellValue(siq.getId_filiale() + " " + siq.getDe_filiale());

        ArrayList dati = siq.getDati();

        Row row6 = sheet.createRow(5);

        Cell f2 = row6.createCell(1);
        f2.setCellStyle(style3left);
        f2.setCellValue("Type");

        Cell f3 = row6.createCell(2);
        f3.setCellStyle(style3);
        f3.setCellValue("Till");

        Cell f4 = row6.createCell(3);
        f4.setCellStyle(style3);
        f4.setCellValue("User");

        Cell f4bb = row6.createCell(4);
        f4bb.setCellStyle(style3);
        f4bb.setCellValue("Branch ID");

        Cell f5 = row6.createCell(5);
        f5.setCellStyle(style3);
        f5.setCellValue("No.Tr.");

        Cell f6 = row6.createCell(6);
        f6.setCellStyle(style3left);
        f6.setCellValue("Date / Time");

        Cell f7 = row6.createCell(7);
        f7.setCellStyle(style3left);
        f7.setCellValue("Cur");

        Cell f8 = row6.createCell(8);
        f8.setCellStyle(style3left);
        f8.setCellValue("Kind");

        Cell f9 = row6.createCell(9);
        f9.setCellStyle(style3);
        f9.setCellValue("Amount");

        Cell f10 = row6.createCell(10);
        f10.setCellStyle(style3);
        f10.setCellValue("Rate");

        Cell f11 = row6.createCell(11);
        f11.setCellStyle(style3);
        f11.setCellValue("Total");

        Cell f12 = row6.createCell(12);
        f12.setCellStyle(style3);
        f12.setCellValue("%");

        Cell f13 = row6.createCell(13);
        f13.setCellStyle(style3);
        f13.setCellValue("Comm.Fee");

        Cell f13bis = row6.createCell(14);
        f13bis.setCellStyle(style3);
        f13bis.setCellValue("Round");

        Cell f14 = row6.createCell(15);
        f14.setCellStyle(style3);
        f14.setCellValue("Pay In / Pay Out");

        Cell f15 = row6.createCell(16);
        f15.setCellStyle(style3left);
        f15.setCellValue("Customer");

        Cell f16 = row6.createCell(17);
        f16.setCellStyle(style3);
        f16.setCellValue("Spread");

        Cell f17 = row6.createCell(18);
        f17.setCellStyle(style3);
        f17.setCellValue("Pos / Bank Acc");

        double totaleresidentbuy = 0;
        double totalenonresidentbuy = 0;
        double totaleresidentcommfreebuy = 0;
        double totalenonresidentcommfreebuy = 0;

        double totaleresidentsell = 0;
        double totalenonresidentsell = 0;
        double totaleresidentcommfreesell = 0;
        double totalenonresidentcommfreesell = 0;

        double totalepayinoutgeneral = 0;

        double totround = 0;
        double totcommfee = 0;

        int cntriga = 7;

        for (int i = 0; i < dati.size(); i++) {

            cntriga++;
            XSSFRow row7 = sheet.createRow(cntriga);

            TillTransactionListBB_value actual = (TillTransactionListBB_value) dati.get(i);
            TillTransactionListBB_value prossimo = (TillTransactionListBB_value) dati.get(i);

            Cell f18 = row7.createCell(1);
            f18.setCellStyle(style4left);
            f18.setCellValue(actual.getType());

            Cell f19 = row7.createCell(2);
            f19.setCellStyle(style4);
            f19.setCellValue(actual.getTill());

            Cell f20 = row7.createCell(3);
            f20.setCellStyle(style4);
            f20.setCellValue(actual.getUser());

            Cell f204 = row7.createCell(4);
            f204.setCellStyle(style4);
            f204.setCellValue(actual.getId_filiale());

            Cell f21 = row7.createCell(5);
            f21.setCellStyle(style4);
            f21.setCellValue(actual.getNotr());

            Cell f22 = row7.createCell(6);
            f22.setCellStyle(style4left);
            f22.setCellValue(formatStringtoStringDate(actual.getTime(), patternsqldate, patternnormdate));

            Cell f23 = row7.createCell(7);
            f23.setCellStyle(style4left);
            f23.setCellValue(actual.getCur());

            Cell f24 = row7.createCell(8);
            f24.setCellStyle(style4left);
            f24.setCellValue(actual.getKind());

            if (actual.getKind().contains("Buy")) {
                if (actual.getResidentnonresident().equalsIgnoreCase("Resident")) {
                    totaleresidentbuy += fd(actual.getTotalSenzaFormattazione());
                    totaleresidentcommfreebuy += fd(actual.getComfreeSenzaFormattazione());

                } else {
                    totalenonresidentcommfreebuy += fd(actual.getComfreeSenzaFormattazione());
                    totalenonresidentbuy += fd(actual.getTotalSenzaFormattazione());
                }
            }
            if (actual.getKind().contains("Sell")) {
                if (actual.getResidentnonresident().equalsIgnoreCase("Resident")) {

                    totaleresidentsell += fd(actual.getTotalSenzaFormattazione());
                    totaleresidentcommfreesell += fd(actual.getComfreeSenzaFormattazione());
                } else {
                    totalenonresidentsell += fd(actual.getTotalSenzaFormattazione());

                    totalenonresidentcommfreesell += fd(actual.getComfreeSenzaFormattazione());
                }
            }

            Cell f25 = row7.createCell(9);
            f25.setCellStyle(style4);
            f25.setCellValue(formatMysqltoDisplay(actual.getAmount()));

            Cell f26 = row7.createCell(10);
            f26.setCellStyle(style4);
            f26.setCellValue(formatMysqltoDisplay(actual.getRate()));

            Cell f27 = row7.createCell(11);
            f27.setCellStyle(style4);
            f27.setCellValue(formatMysqltoDisplay(actual.getTotal()));

            Cell f28 = row7.createCell(12);
            f28.setCellStyle(style4);
            f28.setCellValue(formatMysqltoDisplay(actual.getPerc()));

            Cell f29 = row7.createCell(13);
            f29.setCellStyle(style4);
            f29.setCellValue(formatMysqltoDisplay(actual.getComfree()));

            totcommfee = totcommfee + ((fd(actual.getComfree())));

            Cell f29bis = row7.createCell(14);
            f29bis.setCellStyle(style4);
            f29bis.setCellValue(actual.getRound());

            totround = totround + ((fd(actual.getRound())));
            totround = roundDouble(totround, 2);

            Cell f30 = row7.createCell(15);
            f30.setCellStyle(style4);

            if (actual.getKind().contains("S")) {
                f30.setCellValue("+" + formatMysqltoDisplay(actual.getPayinpayout()));

                totalepayinoutgeneral += fd(actual.getPayinpayoutSenzaFormattazione());
            } else {

                f30.setCellValue("-" + formatMysqltoDisplay(actual.getPayinpayout()));

                totalepayinoutgeneral -= fd(actual.getPayinpayoutSenzaFormattazione());
            }

            Cell f31 = row7.createCell(16);
            f31.setCellStyle(style4left);
            f31.setCellValue(actual.getCustomer());

            Cell f32 = row7.createCell(17);
            f32.setCellStyle(style4);
            f32.setCellValue(formatMysqltoDisplay(actual.getSpread()));

            Cell f33 = row7.createCell(18);
            f33.setCellStyle(style4);
            f33.setCellValue(actual.getPos());

            if (intestazionePdf.contains("Delete")) {
                cntriga++;
                Row row8 = sheet.createRow(cntriga);
                Cell f331 = row8.createCell(16);
                f331.setCellStyle(style4left);
                f331.setCellValue(actual.getDelete1());

                Cell f332 = row8.createCell(17);
                f332.setCellStyle(style4left);
                f332.setCellValue(actual.getDelete2());

            }

            cntriga++;
            cntriga++;

        }

        cntriga++;
        XSSFRow row8 = sheet.createRow(cntriga);

        if (dati.size() > 0) {

            for (int v = 0; v < columnWidths2.length; v++) {
                if (v == 9) {
                    double totalegenerale = totaleresidentbuy + totaleresidentsell + totalenonresidentbuy
                            + totalenonresidentsell;

                    Cell f34 = row8.createCell(9 + 1);
                    f34.setCellStyle(style3);
                    f34.setCellValue((formatMysqltoDisplay(roundDoubleandFormat(totalegenerale, 2))));

                } else if (v == 12 + 1) {

                    Cell f35 = row8.createCell(12);
                    f35.setCellStyle(style3);
                    f35.setCellValue((formatMysqltoDisplay(roundDoubleandFormat(totcommfee, 2))));

                } else if (v == 13 + 1) {

                    Cell f35 = row8.createCell(13);
                    f35.setCellStyle(style3);
                    f35.setCellValue((formatMysqltoDisplay(roundDoubleandFormat(totround, 2))));

                } else if (v == 14 + 1) {

                    Cell f36 = row8.createCell(14);
                    f36.setCellStyle(style3);
                    f36.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalepayinoutgeneral, 2)));

                } else {

                    Cell f37 = row8.createCell(v + 1);
                    f37.setCellStyle(style3);
                    f37.setCellValue("");

                }

            }
        }
        cntriga++;
        cntriga++;
        cntriga++;
        cntriga++;
        Row row9 = sheet.createRow(cntriga);

        Cell f38 = row9.createCell(3);
        f38.setCellStyle(style3);
        f38.setCellValue("Transaction value");

        Cell f41 = row9.createCell(7);
        f41.setCellStyle(style3);
        f41.setCellValue("Commission Value");

        Cell f42 = row9.createCell(14);
        f42.setCellStyle(style3);
        f42.setCellValue("Transacion Number");

        Cell f421 = row9.createCell(11);
        f421.setCellStyle(style3);
        f421.setCellValue("Sell / Internet Booking");

        Cell f422 = row9.createCell(17);
        f422.setCellStyle(style3);
        f422.setCellValue("POS");

        Cell f4221 = row9.createCell(21);
        f4221.setCellStyle(style3);
        f4221.setCellValue("Bank Account");

        cntriga++;
        Row row10 = sheet.createRow(cntriga);

        Cell f43 = row10.createCell(2);
        f43.setCellStyle(style3);
        f43.setCellValue("Resident");

        Cell f44 = row10.createCell(3);
        f44.setCellStyle(style3);
        f44.setCellValue("Non resident");

        Cell f45 = row10.createCell(4);
        f45.setCellStyle(style3);
        f45.setCellValue("Total");

        Cell f46 = row10.createCell(6);
        f46.setCellStyle(style3);
        f46.setCellValue("Resident");

        Cell f47 = row10.createCell(7);
        f47.setCellStyle(style3);
        f47.setCellValue("Non resident");

        Cell f48 = row10.createCell(8);
        f48.setCellStyle(style3);
        f48.setCellValue("Total");

        Cell f49 = row10.createCell(10);
        f49.setCellStyle(style3);
        f49.setCellValue("Resident");

        Cell f50 = row10.createCell(11);
        f50.setCellStyle(style3);
        f50.setCellValue("Non resident");

        Cell f51 = row10.createCell(12);
        f51.setCellStyle(style3);
        f51.setCellValue("Total");

        Cell f52 = row10.createCell(14);
        f52.setCellStyle(style3);
        f52.setCellValue("Amount");

        Cell f53 = row10.createCell(15);
        f53.setCellStyle(style3);
        f53.setCellValue("#");

        Cell f54 = row10.createCell(17);
        f54.setCellStyle(style3);
        f54.setCellValue("Amount");

        Cell f55 = row10.createCell(18);
        f55.setCellStyle(style3);
        f55.setCellValue("#");

        Cell f541 = row10.createCell(20);
        f541.setCellStyle(style3);
        f541.setCellValue("Amount");

        Cell f551 = row10.createCell(21);
        f551.setCellStyle(style3);
        f551.setCellValue("#");

        cntriga++;
        Row row11 = sheet.createRow(cntriga);

        int cntvalue = 0;

        ArrayList datifooter = siq.getFooterdati();

        ArrayList totaleresidentnonresperbuy = new ArrayList();
        ArrayList totaleresidentnonrespersell = new ArrayList();
        ArrayList totaledeitotalibuy = new ArrayList();
        ArrayList totaledeitotalisell = new ArrayList();

        int cnt = 0;

        for (int n = 0; n < columnWidths4.length; n++) {

            if (n == 4 || n == 8) {

                //
                // phraset = new Phrase();
                // phraset.add(new Chunk("", f5_bold));
                // cellt = new PdfPCell(phraset);
                // cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                // cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                // cellt.setBorder(Rectangle.BOX);
                // table3.addCell(cellt);
            } else if (n == 0) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("buy");

            } else if (n == 12) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("yes");

            } else if (n == 15 || n == 18) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("buy");

            } // else (cntvalue % 2 == 1) {
            else if (n == 11) {

                double s2 = fd(datifooter.get(cntvalue - 2).toString());
                double s1 = fd(datifooter.get(cntvalue - 1).toString());
                double tot = s1 + s2;

                totaledeitotalibuy.add(tot);
                // totaleresidentnonresperbuy.add(s2);
                // totaleresidentnonresperbuy.add(s1);

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(tot, 0)));

            } else if (n == 1) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getTransvalueresidentbuy())));

            } else if (n == 2) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getTransvaluenonresidentbuy())));

            } else if (n == 3) {

                double totalebuytrans = fd(siq.getTransvaluenonresidentbuy()) + fd(siq.getTransvalueresidentbuy());

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalebuytrans, 2)));

            } else if (n == 5) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getCommisionvaluetresidentbuy())));

            } else if (n == 6) {

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getCommisionvaluenonresidentbuy())));

            } else if (n == 7) {
                double totalebuycommfee = fd(siq.getCommisionvaluenonresidentbuy())
                        + fd(siq.getCommisionvaluetresidentbuy());

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalebuycommfee, 2)));

            } else {

                totaleresidentnonresperbuy.add(fd(datifooter.get(cntvalue).toString()));

                Cell f56 = row11.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(datifooter.get(cntvalue).toString()));

                cntvalue++;
            }

        }

        cntriga++;
        Row row12 = sheet.createRow(cntriga);

        for (int n = 0; n < columnWidths4.length; n++) {

            if (n == 4 || n == 8) {
                // phraset = new Phrase();
                // phraset.add(new Chunk("", f5_bold));
                // cellt = new PdfPCell(phraset);
                // cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                // cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                // cellt.setBorder(Rectangle.BOX);
                // table3.addCell(cellt);
            } else if (n == 0) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("sell");

            } else if (n == 12) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("no");

            } else if (n == 15 || n == 18) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("sell");

            } else if (n == 11) {
                double s2 = fd(datifooter.get(cntvalue - 2).toString());
                double s1 = fd(datifooter.get(cntvalue - 1).toString());
                double tot = s1 + s2;

                totaledeitotalisell.add(tot);
                // totaleresidentnonrespersell.add(s2);
                // totaleresidentnonrespersell.add(s1);

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(tot, 0)));

            } else if (n == 1) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getTransvalueresidentsell())));

            } else if (n == 2) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getTransvaluenonresidentsell())));

            } else if (n == 3) {

                double totaleselltrans = fd(siq.getCommisionvaluetresidentsell())
                        + fd(siq.getCommisionvaluenonresidentsell());
                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totaleselltrans, 2)));

            } else if (n == 5) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getCommisionvaluetresidentsell())));

            } else if (n == 6) {

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(String.valueOf(siq.getCommisionvaluenonresidentsell())));

            } else if (n == 7) {
                double totalesellcommfee = fd(siq.getCommisionvaluenonresidentsell())
                        + fd(siq.getCommisionvaluetresidentsell());

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalesellcommfee, 2)));

            } else {
                totaleresidentnonrespersell.add(fd(datifooter.get(cntvalue).toString()));

                Cell f56 = row12.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(datifooter.get(cntvalue).toString()));

                cntvalue++;
            }

        }

        cntriga++;
        Row row13 = sheet.createRow(cntriga);

        int cnttotali = 0;
        int cntresnonres = 0;

        for (int n = 0; n < columnWidths4.length; n++) {

            if (n == 4 || n == 8) {
                // phraset = new Phrase();
                // phraset.add(new Chunk("", f5_bold));
                // cellt = new PdfPCell(phraset);
                // cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                // cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                // cellt.setBorder(Rectangle.BOX);
                // table3.addCell(cellt);

            } else if (n == 0) {

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("total");

            } else if (n == 12) {

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("total");

            } else if (n == 15 || n == 18) {

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style3);
                f56.setCellValue("total");

            } else if (n == 11) {

                double tot1 = (double) totaledeitotalibuy.get(cnttotali);
                double tot2 = (double) totaledeitotalisell.get(cnttotali);

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(tot1 + tot2, 0)));

                cnttotali++;
            } else if (n == 1) {

                double totaleresidentsellbuy = fd(siq.getTransvalueresidentbuy())
                        + fd(siq.getTransvalueresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totaleresidentsellbuy, 2)));

            } else if (n == 2) {

                double totalenonresidentbuysell = fd(siq.getTransvaluenonresidentbuy())
                        + fd(siq.getTransvaluenonresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalenonresidentbuysell, 2)));

            } else if (n == 3) {

                double totalebuysellgenerale = fd(siq.getTransvaluenonresidentbuy())
                        + fd(siq.getTransvaluenonresidentsell()) + fd(siq.getTransvalueresidentbuy())
                        + fd(siq.getTransvalueresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalebuysellgenerale, 2)));

            } else if (n == 5) {

                double totaleresidentcommfeebuysell = fd(siq.getCommisionvaluetresidentbuy())
                        + fd(siq.getCommisionvaluetresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totaleresidentcommfeebuysell, 2)));

            } else if (n == 6) {
                double totalenonresidentcommfeebuysell = fd(siq.getCommisionvaluenonresidentbuy())
                        + fd(siq.getCommisionvaluenonresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalenonresidentcommfeebuysell, 2)));

            } else if (n == 7) {
                double totalegeneralecommfee = fd(siq.getCommisionvaluenonresidentbuy())
                        + fd(siq.getCommisionvaluenonresidentsell()) + fd(siq.getCommisionvaluetresidentbuy())
                        + fd(siq.getCommisionvaluetresidentsell());

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totalegeneralecommfee, 2)));

            } else {
                double tot3 = (double) (totaleresidentnonresperbuy.get(cntresnonres));
                double tot4 = (double) (totaleresidentnonrespersell.get(cntresnonres));

                Cell f56 = row13.createCell(n + 1);
                f56.setCellStyle(style4);
                if (n == 9 || n == 10 || n == 14 || n == 17 || n == 20) {
                    f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(tot3 + tot4, 0)));
                } else {
                    f56.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(tot3 + tot4, 2)));
                }

                cntresnonres++;
            }
        }

        sheet.autoSizeColumn(0);
        sheet.autoSizeColumn(1);
        sheet.autoSizeColumn(2);
        sheet.autoSizeColumn(3);
        sheet.autoSizeColumn(4);
        sheet.autoSizeColumn(5);
        sheet.autoSizeColumn(6);
        sheet.autoSizeColumn(7);
        sheet.autoSizeColumn(8);
        sheet.autoSizeColumn(9);
        sheet.autoSizeColumn(10);
        sheet.autoSizeColumn(11);
        sheet.autoSizeColumn(12);
        sheet.autoSizeColumn(13);
        sheet.autoSizeColumn(14);
        sheet.autoSizeColumn(15);
        sheet.autoSizeColumn(16);
        sheet.autoSizeColumn(17);
        sheet.autoSizeColumn(18);
        sheet.autoSizeColumn(19);
        try (FileOutputStream out = new FileOutputStream(outputfile)) {
            workbook.write(out);
            // Chiudiamo il flusso di output prima di leggere il file per la conversione in
            // Base64
            out.close();
            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;

    }

    public static String CP_mainexcel(File outputfile, String d3, String d4, String data1, String data2, String bss,
            ArrayList<C_CashierPerformance_value> dati, ArrayList<String> alcolonne, ArrayList<String> filiali,
            ArrayList<Branch> br) {
        HSSFWorkbook workbook = null;
        try {
            workbook = new HSSFWorkbook();
            HSSFSheet sheet = workbook.createSheet("C_CashierPerformance");

            // CREAZIONE FONT
            HSSFFont font = workbook.createFont();
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 12);
            font.setBold(true);

            HSSFCellStyle style1 = (HSSFCellStyle) workbook.createCellStyle();
            style1.setFont(font);

            HSSFFont font2 = workbook.createFont();
            font2.setFontName(HSSFFont.FONT_ARIAL);
            font2.setFontHeightInPoints((short) 12);

            HSSFCellStyle style2 = (HSSFCellStyle) workbook.createCellStyle();
            style2.setFont(font2);

            HSSFFont font3 = workbook.createFont();
            font3.setFontName(HSSFFont.FONT_ARIAL);
            font3.setFontHeightInPoints((short) 10);
            font3.setBold(true);

            HSSFCellStyle style3 = (HSSFCellStyle) workbook.createCellStyle();
            style3.setFont(font3);
            style3.setAlignment(HorizontalAlignment.RIGHT);
            style3.setBorderTop(BorderStyle.THIN);
            style3.setBorderBottom(BorderStyle.THIN);

            HSSFCellStyle style3left = (HSSFCellStyle) workbook.createCellStyle();
            style3left.setFont(font3);
            style3left.setAlignment(HorizontalAlignment.LEFT);
            style3left.setBorderTop(BorderStyle.THIN);
            style3left.setBorderBottom(BorderStyle.THIN);

            HSSFFont font4 = workbook.createFont();
            font4.setFontName(HSSFFont.FONT_ARIAL);
            font4.setFontHeightInPoints((short) 10);

            HSSFCellStyle style4 = (HSSFCellStyle) workbook.createCellStyle();
            style4.setAlignment(HorizontalAlignment.RIGHT);
            style4.setBorderTop(BorderStyle.THIN);
            style4.setBorderBottom(BorderStyle.THIN);

            HSSFCellStyle style4left = (HSSFCellStyle) workbook.createCellStyle();
            style4left.setAlignment(HorizontalAlignment.LEFT);
            style4left.setBorderTop(BorderStyle.THIN);
            style4left.setBorderBottom(BorderStyle.THIN);

            HSSFCellStyle style1bis = (HSSFCellStyle) workbook.createCellStyle();
            style1bis.setFont(font3);
            style1bis.setAlignment(HorizontalAlignment.RIGHT);
            style1bis.setBorderTop(BorderStyle.THIN);
            style1bis.setBorderBottom(BorderStyle.THIN);
            style1bis.setWrapText(true);

            C_CashierPerformance_value pdf = new C_CashierPerformance_value();
            boolean firstTime = true;
            boolean lastTime = true;

            pdf.setDataDa(d3);
            pdf.setDataA(d4);
            pdf.setDati(dati);

            if (dati.size() > 0) {
                CP_receiptexcel(pdf, alcolonne, firstTime, lastTime, bss, filiali, br, sheet, 1, style1, style2, style3,
                        style4, style1bis, style3left, style4left);
                // chiusura documento

                for (int i = 0; i < 20; i++) {
                    sheet.autoSizeColumn(i);
                }

                try (FileOutputStream out = new FileOutputStream(outputfile)) {
                    workbook.write(out);
                }

                String base64;
                if (alcolonne.size() > 14) {
                    base64 = generagrafico(outputfile, 1);
                } else {
                    base64 = generagrafico(outputfile, 2);
                }
                return base64;
            } else {
                return null;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (workbook != null) {
                try {
                    workbook.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    private static int CP_receiptexcel(C_CashierPerformance_value cmfb, ArrayList<String> colonne, boolean firstTime,
            boolean lastTime, String opType, ArrayList<String> filialisel,
            ArrayList<Branch> filialifull, HSSFSheet sheet, int cntriga,
            HSSFCellStyle style1, HSSFCellStyle style2, HSSFCellStyle style3, HSSFCellStyle style4,
            HSSFCellStyle style1bis, HSSFCellStyle style3left, HSSFCellStyle style4left) {

        ArrayList<String> cpTot = new ArrayList<>();
        String intestazionePdf = "Cashier Performance";

        try {

            if (firstTime) {

                // creo un array di appoggio per sommarmi i totali parametrizzati
                C_CashierPerformance_value t = (C_CashierPerformance_value) cmfb.getDati().get(0);
                int size = t.getDati().size();

                for (int i = 0; i < size; i++) {
                    cpTot.add("0");
                }

                Row rowP = sheet.createRow((short) cntriga);

                Cell cl = rowP.createCell(1);
                cl.setCellStyle(style1);
                cl.setCellValue(intestazionePdf + " From " + cmfb.getDataDa() + " to " + cmfb.getDataA());

                cntriga++;
                cntriga++;

                String opTypeesteso;

                if (opType.equals("BS")) {
                    opTypeesteso = "Buy / Sell";
                } else if (opType.equals("B")) {
                    opTypeesteso = "Buy";
                } else {
                    opTypeesteso = "Sell";
                }

                Row rowP2 = sheet.createRow((short) cntriga);

                Cell cl2 = rowP2.createCell(1);
                cl2.setCellStyle(style1);
                cl2.setCellValue("Operation Type: " + opTypeesteso);

                cntriga++;
                cntriga++;

                String elencofiliali = "";
                for (int v = 0; v < filialisel.size(); v++) {
                    elencofiliali = elencofiliali + " - "
                            + formatBankBranch(filialisel.get(v), "BR", null, filialifull, null);
                }

                Row rowP3 = sheet.createRow((short) cntriga);

                Cell cl3 = rowP3.createCell(1);
                cl3.setCellStyle(style1bis);
                cl3.setCellValue(elencofiliali);

                cntriga++;
                cntriga++;

            }

            // document.add(table2);
            // document.add(sep);
            ArrayList dati = cmfb.getDati();

            int totfull = 0;
            int totNtrans = 0;
            int totNff = 0;
            int totDel = 0;
            double totVolume = 0;
            double totComFix = 0;
            int totErr = 0;
            double totTotErr = 0;
            double totPerc = 0;

            boolean ft = true;

            if (ft) {

                Row row66 = sheet.createRow((short) cntriga);

                // mi scandisco le colonne
                for (int c = 0; c < colonne.size(); c++) {
                    Cell cl8 = row66.createCell(c + 1);
                    cl8.setCellStyle(style3);
                    if (c == 0) {
                        cl8.setCellStyle(style3left);
                    }
                    cl8.setCellValue(colonne.get(c));
                }

            }

            for (int j = 0; j < dati.size(); j++) {

                cntriga++;

                Row row6 = sheet.createRow((short) cntriga);

                C_CashierPerformance_value temp = (C_CashierPerformance_value) dati.get(j);

                Cell f1bis = row6.createCell(1);
                f1bis.setCellStyle(style4left);
                f1bis.setCellType(CellType.STRING);
                f1bis.setCellValue(temp.getUser());

                ArrayList<String> cp = temp.getDati();

                for (int x = 0; x < cp.size(); x++) {

                    Cell fx = row6.createCell(x + 2);
                    fx.setCellStyle(style4);
                    fx.setCellValue(cp.get(x));

                    // cpTot.set(x, (Float.parseFloat(cpTot.get(x)) + Float.parseFloat(cp.get(x))) +
                    // "");
                    cpTot.set(x, (Integer.parseInt(cpTot.get(x)) + Integer.parseInt(cp.get(x))) + "");
                }

                Cell f2 = row6.createCell(cp.size() + 2);
                f2.setCellStyle(style4);
                f2.setCellValue(temp.getFull());

                Cell f3 = row6.createCell(cp.size() + 3);
                f3.setCellStyle(style4);
                f3.setCellValue(temp.getnTrans());

                Cell f4 = row6.createCell(cp.size() + 4);
                f4.setCellStyle(style4);
                f4.setCellValue(temp.getNff());

                Cell f5 = row6.createCell(cp.size() + 5);
                f5.setCellStyle(style4);
                f5.setCellValue(temp.getDel());

                Cell f6 = row6.createCell(cp.size() + 6);
                f6.setCellStyle(style4);
                f6.setCellValue(temp.getVolume());

                Cell f7 = row6.createCell(cp.size() + 7);
                f7.setCellStyle(style4);
                f7.setCellType(CellType.NUMERIC);
                f7.setCellValue(fd(temp.getComFix()));

                Cell f8 = row6.createCell(cp.size() + 8);
                f8.setCellStyle(style4);

                if (fd(temp.getVolume()) == 0) {
                    f8.setCellValue("0.00");
                } else {
                    double a = (fd(temp.getComFix()) / fd(temp.getVolume()) * 100);
                    f8.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(a, 2)));
                }

                Cell f9 = row6.createCell(cp.size() + 9);
                f9.setCellStyle(style4);

                if (fd(temp.getnTrans()) == 0) {
                    f9.setCellValue("0");
                } else {
                    double a = (fd(temp.getVolume()) / fd(temp.getnTrans()));
                    f9.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(a, 2)) + "");
                }

                Cell f10 = row6.createCell(cp.size() + 10);
                f10.setCellStyle(style4);

                if (fd(temp.getnTrans()) == 0) {
                    f10.setCellValue("0");
                } else {
                    double a = (fd(temp.getComFix()) / fd(temp.getnTrans()));
                    f10.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(a, 2)) + "");
                }

                Cell f11 = row6.createCell(cp.size() + 11);
                f11.setCellStyle(style4);
                f11.setCellValue(temp.getErr());

                Cell f12 = row6.createCell(cp.size() + 12);
                f12.setCellStyle(style4);
                f12.setCellValue(formatMysqltoDisplay(temp.getTotErr()));

                totfull += parseIntR(temp.getFull());
                totNtrans += parseIntR(temp.getnTrans());
                totNff += parseIntR(temp.getNff());
                totDel += parseIntR(temp.getDel());
                totVolume += fd(temp.getVolume());
                totComFix += fd(temp.getComFix());
                totErr += parseIntR(temp.getErr());
                totTotErr += fd(temp.getTotErr());

                if (fd(temp.getnTrans()) == 0) {
                    totPerc += 0;
                } else {
                    totPerc += (fd(temp.getComFix()) / fd(temp.getnTrans()));
                }

            }

            totVolume = roundDouble(totVolume, 2);
            totComFix = roundDouble(totComFix, 2);
            totTotErr = roundDouble(totTotErr, 2);
            totPerc = roundDouble(totPerc, 2);

            cntriga++;

            Row row7 = sheet.createRow((short) cntriga);

            Cell f2 = row7.createCell(1);
            f2.setCellStyle(style3left);
            f2.setCellValue("Total");

            // total
            for (int x = 0; x < cpTot.size(); x++) {
                Cell f8 = row7.createCell(x + 2);
                f8.setCellStyle(style3);
                f8.setCellValue(cpTot.get(x));

            }

            Cell f10 = row7.createCell(cpTot.size() + 2);
            f10.setCellStyle(style3);
            f10.setCellValue(roundDoubleandFormat(totfull, 0));

            Cell f11 = row7.createCell(cpTot.size() + 3);
            f11.setCellStyle(style3);
            f11.setCellValue(roundDoubleandFormat(totNtrans, 0));

            Cell f12 = row7.createCell(cpTot.size() + 4);
            f12.setCellStyle(style3);
            f12.setCellValue(roundDoubleandFormat(totNff, 0));

            Cell f13 = row7.createCell(cpTot.size() + 5);
            f13.setCellStyle(style3);
            f13.setCellValue(roundDoubleandFormat(totDel, 0));

            Cell f14 = row7.createCell(cpTot.size() + 6);
            f14.setCellStyle(style3);
            f14.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totVolume, 2)));

            Cell f15 = row7.createCell(cpTot.size() + 7);
            f15.setCellStyle(style3);
            f15.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totComFix, 2)));

            Cell f16 = row7.createCell(cpTot.size() + 9);
            f16.setCellStyle(style3);
            if (totNtrans != 0) {
                double a = ((totVolume) / (totNtrans));
                f16.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(a, 2)));

                Cell f17 = row7.createCell(cpTot.size() + 10);
                f17.setCellStyle(style3);
                double a1 = (totComFix) / (totNtrans);
                f17.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(a1, 2)));

                Cell f18 = row7.createCell(cpTot.size() + 11);
                f18.setCellStyle(style3);
                f18.setCellValue(totErr);

                Cell f19 = row7.createCell(cpTot.size() + 12);
                f19.setCellStyle(style3);
                f19.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(totTotErr, 2)));

                // fine total
                // media
                cntriga++;
                Row row8 = sheet.createRow((short) cntriga);

                Cell f8 = row8.createCell(1);
                f8.setCellStyle(style3left);
                f8.setCellValue("Average");

                // da calcolare float mediaBonus=
                double mediaFull = totfull / dati.size();
                double mediaNtrans = totNtrans / dati.size();
                double mediaNff = totNff / dati.size();
                double mediaDel = totDel / dati.size();
                double mediaVolume = totVolume / dati.size();
                double mediaComFix = totComFix / dati.size();
                double mediaPerc = totPerc / dati.size();
                double mediaErr = totErr / dati.size();
                double mediaTotErr = totTotErr / dati.size();

                mediaFull = roundDouble(mediaFull, 2);
                mediaNtrans = roundDouble(mediaNtrans, 2);
                mediaNff = roundDouble(mediaNff, 2);
                mediaDel = roundDouble(mediaDel, 2);
                mediaVolume = roundDouble(mediaVolume, 2);
                mediaComFix = roundDouble(mediaComFix, 2);
                mediaErr = roundDouble(mediaErr, 2);
                mediaTotErr = roundDouble(mediaTotErr, 2);

                ArrayList<String> cpMedia = new ArrayList<>();

                for (int x = 0; x < cpTot.size(); x++) {

                    Cell fx = row8.createCell(x + 2);
                    fx.setCellStyle(style3);
                    fx.setCellValue((fd(cpTot.get(x)) / dati.size()));

                }

                Cell f70 = row8.createCell(cpTot.size() + 2);
                f70.setCellStyle(style3);
                f70.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaFull, 2)));

                Cell f71 = row8.createCell(cpTot.size() + 3);
                f71.setCellStyle(style3);
                f71.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaNtrans, 2)));

                Cell f72 = row8.createCell(cpTot.size() + 4);
                f72.setCellStyle(style3);
                f72.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaNff, 2)));

                Cell f73 = row8.createCell(cpTot.size() + 5);
                f73.setCellStyle(style3);
                f73.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaDel, 2)));

                Cell f74 = row8.createCell(cpTot.size() + 6);
                f74.setCellStyle(style3);
                f74.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaVolume, 2)));

                Cell f75 = row8.createCell(cpTot.size() + 7);
                f75.setCellStyle(style3);
                f75.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaComFix, 2)));

                Cell f76 = row8.createCell(cpTot.size() + 8);
                f76.setCellStyle(style3);
                double b = ((mediaComFix / mediaVolume) * 100);
                f76.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(b, 2)));

                Cell f77 = row8.createCell(cpTot.size() + 11);
                f77.setCellStyle(style3);
                f77.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaErr, 2)));

                Cell f78 = row8.createCell(cpTot.size() + 12);
                f78.setCellStyle(style3);
                f78.setCellValue(formatMysqltoDisplay(roundDoubleandFormat(mediaTotErr, 2)));
            }
            // fine media
        } catch (Exception ex) {
            ex.printStackTrace();

        }

        cntriga++;
        cntriga++;
        cntriga++;
        cntriga++;
        return cntriga;
    }

    private static String generagrafico(File filename, int type) {
        try (FileInputStream chartFileInput = new FileInputStream(filename);
                HSSFWorkbook myWorkbook = new HSSFWorkbook(chartFileInput);
                ByteArrayOutputStream chartOut = new ByteArrayOutputStream()) {

            HSSFSheet mySheet = myWorkbook.getSheetAt(0);
            DefaultCategoryDataset myBarChartDataset = new DefaultCategoryDataset();

            Iterator<Row> rowIterator = mySheet.iterator();
            int numRow = (int) mySheet.getPhysicalNumberOfRows() + 4;

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                if (row.getRowNum() >= 8 && row.getRowNum() < numRow - 2) {
                    Cell c1 = row.getCell(1);
                    String chartLabel = c1.getStringCellValue();
                    Cell c2 = row.getCell(14);
                    if (type == 2) {
                        c2 = row.getCell(9);
                    }
                    Number chartData = c2.getNumericCellValue();
                    myBarChartDataset.addValue(chartData.doubleValue(), "Cashier", chartLabel);
                }
            }

            JFreeChart barChartObject = ChartFactory.createBarChart("Cashier Performance", "Cashier", "Com+Fix",
                    myBarChartDataset, PlotOrientation.VERTICAL, true, true, false);

            int width = 1000;
            int height = 250;

            ChartUtilities.writeChartAsPNG(chartOut, barChartObject, width, height);
            int myPictureId = myWorkbook.addPicture(chartOut.toByteArray(), Workbook.PICTURE_TYPE_PNG);

            HSSFPatriarch drawing = mySheet.createDrawingPatriarch();
            ClientAnchor myAnchor = new HSSFClientAnchor();
            myAnchor.setCol1(21);
            myAnchor.setRow1(7);
            HSSFPicture myPicture = drawing.createPicture(myAnchor, myPictureId);
            myPicture.resize();

            try (FileOutputStream out = new FileOutputStream(filename)) {
                myWorkbook.write(out);
            }

            return new String(Base64.encodeBase64(FileUtils.readFileToByteArray(filename)));

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String AML_anagrafica(GeneraFile gf, File outputfile, String data_da, String data_a) {
        try {
            DatabaseCons db1 = new DatabaseCons(gf);
            InputStream inp = new ByteArrayInputStream(Base64.decodeBase64(db1.getConf("path.anti.anag")));
            Workbook wb = WorkbookFactory.create(inp);
            Sheet sheet = wb.getSheetAt(0);
            ArrayList<String[]> coddoc = db1.identificationCard();
            ArrayList<String[]> city = db1.city_Italy_APM();
            ResultSet rsCl = db1.getSogliaTipologiaCL();
            int indiceRiga = 1;
            while (rsCl.next()) {
                ResultSet rs = db1.getTransazioni(rsCl.getDouble(2), StringUtils.leftPad(rsCl.getString(1), 3, "0"),
                        data_da, data_a);
                while (rs.next()) {
                    Row row = sheet.getRow(indiceRiga);
                    if (row == null) {
                        row = sheet.createRow(indiceRiga);
                    }
                    Cell cell0 = row.createCell(0);
                    cell0.setCellValue("1" + rs.getString("filiale") + rs.getString("id"));
                    Cell cell = row.createCell(1);
                    if (rs.getString("tipocliente").equals("003")) {
                        cell.setCellValue("PNF");
                    } else {
                        cell.setCellValue("PF");
                    }
                    // String cliente[] = db1.getCliente(rs.getString("cl_cod"));
                    Client cl = db1.query_Client_transaction(rs.getString("cod"), rs.getString("cl_cod"));

                    String cap = cl.getCap();
                    if (!cl.getNazione().equals(gf.getCODNAZ())) {
                        cap = "";
                    }
                    Cell cell2 = row.createCell(2);
                    cell2.setCellValue(cl.getCodfisc().replaceAll("---", ""));
                    Cell cell3 = row.createCell(3);
                    cell3.setCellValue(cl.getCognome() + " " + cl.getNome());
                    Cell cell4 = row.createCell(4);
                    cell4.setCellValue(cl.getIndirizzo());
                    Cell cell5 = row.createCell(5);
                    cell5.setCellValue(cap);
                    Cell cell6 = row.createCell(6);
                    cell6.setCellValue(formatALN(cl.getCitta(), city, 1).toUpperCase());
                    Cell cell7 = row.createCell(7);
                    cell7.setCellValue(cl.getProvincia());
                    Cell cell8 = row.createCell(8);
                    cell8.setCellValue(cl.getNazione());
                    Cell cell9 = row.createCell(9);
                    cell9.setCellValue(cl.getSesso());
                    Cell cell10 = row.createCell(10);
                    cell10.setCellValue(cl.getDt_nascita());
                    Cell cell11 = row.createCell(11);
                    cell11.setCellValue(cl.getCitta_nascita());
                    Cell cell12 = row.createCell(12);
                    String td = formatAL(cl.getTipo_documento(), coddoc, 2);
                    if (td == null) {
                        cell12.setCellValue("");
                    } else {
                        cell12.setCellValue(td);
                    }
                    Cell cell13 = row.createCell(13);
                    cell13.setCellValue(cl.getNumero_documento());
                    Cell cell14 = row.createCell(14);
                    cell14.setCellValue(cl.getDt_rilascio_documento());
                    Cell cell15 = row.createCell(15);
                    cell15.setCellValue(cl.getRilasciato_da_documento());
                    Cell cell16 = row.createCell(16);
                    cell16.setCellValue("0");
                    Cell cell17 = row.createCell(17);
                    cell17.setCellValue("600");
                    Cell cell18 = row.createCell(18);
                    cell18.setCellValue("600");
                    indiceRiga++;
                }
                rs.close();
            }
            rsCl.close();
            db1.closeDB();
            sheet.autoSizeColumn(0);
            sheet.autoSizeColumn(1);
            sheet.autoSizeColumn(2);
            sheet.autoSizeColumn(3);
            sheet.autoSizeColumn(4);
            sheet.autoSizeColumn(5);
            sheet.autoSizeColumn(6);
            sheet.autoSizeColumn(7);
            sheet.autoSizeColumn(8);
            sheet.autoSizeColumn(9);
            sheet.autoSizeColumn(10);
            sheet.autoSizeColumn(11);
            sheet.autoSizeColumn(12);
            sheet.autoSizeColumn(13);
            sheet.autoSizeColumn(14);
            sheet.autoSizeColumn(15);
            sheet.autoSizeColumn(16);
            sheet.autoSizeColumn(17);
            sheet.autoSizeColumn(18);
            sheet.autoSizeColumn(19);
            sheet.autoSizeColumn(20);
            try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                wb.write(fileOut);
            }
            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String AML_registrazione(GeneraFile gf, File outputfile, String data_da, String data_a) {
        try {
            DatabaseCons db1 = new DatabaseCons(gf);
            try (
                    InputStream inp = new ByteArrayInputStream(Base64.decodeBase64(db1.getConf("path.anti.regi")));
                    Workbook wb = WorkbookFactory.create(inp);
                    FileOutputStream fileOut = new FileOutputStream(outputfile)) {

                Sheet sheet = wb.getSheetAt(0);
                ResultSet rsCl = db1.getSogliaTipologiaCLRegistrazione();
                int indiceRiga = 1;
                while (rsCl.next()) {
                    ResultSet rs = db1.getTransazioni(rsCl.getDouble(2), StringUtils.leftPad(rsCl.getString(1), 3, "0"),
                            data_da, data_a);
                    while (rs.next()) {
                        ResultSet rsCommission = db1.getCommissione(rs.getString("cod"));
                        while (rsCommission.next()) {
                            Row row = sheet.getRow(indiceRiga);
                            if (row == null) {
                                row = sheet.createRow(indiceRiga);
                            }
                            Cell cell0 = row.createCell(0);
                            cell0.setCellValue("");
                            Cell cell = row.createCell(1);
                            cell.setCellValue(
                                    formatStringtoStringDate(rs.getString("data"), "yyyy-MM-dd HH:mm:ss",
                                            "dd/MM/yyyy"));
                            Cell cell2 = row.createCell(2);
                            cell2.setCellValue("10");
                            Cell cell3 = row.createCell(3);
                            Cell cell4 = row.createCell(4);
                            if (rs.getString("tipotr").equalsIgnoreCase("S")) {
                                cell3.setCellValue("DB");
                                cell4.setCellValue("A");
                            } else { // per il BUY
                                cell3.setCellValue("DC");
                                cell4.setCellValue("D");
                            }
                            Cell cell5 = row.createCell(5);
                            cell5.setCellValue(""); // VOCE ==> SEMPRE VUOTOP
                            Cell cell6 = row.createCell(6);
                            cell6.setCellValue(db1.getCodiceValuta(rsCommission.getString("valuta"))); // CODICE VALUTA
                            Cell cell7 = row.createCell(7);
                            cell7.setCellValue(rsCommission.getString("net").replaceAll("\\.", ","));
                            Cell cell8 = row.createCell(8);
                            cell8.setCellValue(rsCommission.getString("net").replaceAll("\\.", ","));
                            Cell cell9 = row.createCell(9);
                            cell9.setCellValue("");
                            Cell cell10 = row.createCell(10);
                            cell10.setCellValue("");
                            Cell cell11 = row.createCell(11);
                            Cell cell12 = row.createCell(12);
                            if (rs.getString("tipocliente").equals("003")) {
                                cell11.setCellValue(db1.getNDGSocieta(rs.getString("cl_cod")));
                                cell12.setCellValue(rs.getString("cl_cod"));
                            } else {
                                cell11.setCellValue("1" + rs.getString("filiale") + rs.getString("id"));
                                cell12.setCellValue("");
                            }
                            Cell cell13 = row.createCell(13);
                            cell13.setCellValue("0");
                            Cell cell14 = row.createCell(14);
                            cell14.setCellValue("0");
                            Cell cell15 = row.createCell(15);
                            cell15.setCellValue("");
                            Cell cell16 = row.createCell(16);
                            cell16.setCellValue("");
                            Cell cell17 = row.createCell(17);
                            cell17.setCellValue("");
                            Cell cell18 = row.createCell(18);
                            cell18.setCellValue("");
                            Cell cell19 = row.createCell(19);
                            cell19.setCellValue("");
                            Cell cell20 = row.createCell(20);
                            cell20.setCellValue("");
                            Cell cell21 = row.createCell(21);
                            cell21.setCellValue("");
                            Cell cell22 = row.createCell(22);
                            cell22.setCellValue("");
                            Cell cell23 = row.createCell(23);
                            cell23.setCellValue("");
                            Cell cell24 = row.createCell(24);
                            cell24.setCellValue("");
                            Cell cell25 = row.createCell(25);
                            cell25.setCellValue("");
                            Cell cell26 = row.createCell(26);
                            cell26.setCellValue("");
                            Cell cell27 = row.createCell(27);
                            cell27.setCellValue("");
                            Cell cell28 = row.createCell(28);
                            cell28.setCellValue("");
                            Cell cell29 = row.createCell(29);
                            cell29.setCellValue("");
                            Cell cell30 = row.createCell(30);
                            cell30.setCellValue("");
                            Cell cell31 = row.createCell(31);
                            cell31.setCellValue("");
                            Cell cell32 = row.createCell(32);
                            cell32.setCellValue("");
                            Cell cell33 = row.createCell(33);
                            cell33.setCellValue("");
                            Cell cell34 = row.createCell(34);
                            cell34.setCellValue("");
                            Cell cell35 = row.createCell(35);
                            cell35.setCellValue("1" + rs.getString("filiale"));
                            indiceRiga++;
                        }
                        rsCommission.close();
                    }
                    rs.close();
                }
                rsCl.close();
                db1.closeDB();

                for (int i = 0; i <= 35; i++) {
                    sheet.autoSizeColumn(i);
                }

                wb.write(fileOut);

                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
                return base64;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static Row getRow(Sheet sheet, int index) {
        Row r = sheet.getRow(index);
        if (r == null) {
            r = sheet.createRow(index);
        }
        return r;
    }

    public static Cell getCell(Row row, int index) {
        Cell cell1 = row.getCell(index);
        if (cell1 == null) {
            cell1 = row.createCell(index);
        }
        return cell1;
    }

    public static Cell getCell(Row row, int index, CellType ct) {
        Cell cell1 = row.getCell(index);
        if (cell1 == null || ct.equals(CellType.NUMERIC)) {
            cell1 = row.createCell(index, ct);
        }
        return cell1;
    }

    public static String giornalieroCG(GeneraFile gf, File outputfile, int numgiornimese, int nummese,
            ArrayList<Colonna> elencovalori,
            ArrayList<Riga> elencovaloririgafoglio2,
            ArrayList<Riga> elencovaloririgafoglio3,
            ArrayList<Riga> elencovaloririgafoglio4,
            ArrayList<Riga> elencovaloririgafoglio5,
            ArrayList<Riga> elencovaloririgafoglio6,
            ArrayList<Riga> elencovaloririgafoglio7) {

        String formatdataCell = "#,#.00";
        String formatdataCellRATE = "#,##0.00000000";

        try {
            DatabaseCons db1 = new DatabaseCons(gf);
            InputStream is = new ByteArrayInputStream(Base64.decodeBase64(db1.getConf("path.giorn")));
            db1.closeDB();

            XSSFWorkbook wb = new XSSFWorkbook(is);
            // Sheet sheet = wb.createSheet("RC");
            Sheet sheet = wb.getSheetAt(0);
            XSSFFont font = (XSSFFont) wb.createFont();
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 12);
            font.setBold(true);
            XSSFFont font2 = (XSSFFont) wb.createFont();
            font2.setFontName(HSSFFont.FONT_ARIAL);
            font2.setFontHeightInPoints((short) 10);

            XSSFDataFormat hssfDataFormat = wb.createDataFormat();

            XSSFCellStyle cellStyle = wb.createCellStyle();
            cellStyle.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStyle.setFont(font2);

            XSSFCellStyle cellStyleRATE = wb.createCellStyle();
            cellStyleRATE.setDataFormat(hssfDataFormat.getFormat(formatdataCellRATE));
            cellStyleRATE.setFont(font2);

            XSSFCellStyle stylesottosinistra = wb.createCellStyle();
            stylesottosinistra.setBorderBottom(BorderStyle.DOUBLE);
            stylesottosinistra.setBorderLeft(BorderStyle.DOUBLE);
            stylesottosinistra.setFont(font);

            XSSFCellStyle stylesinsitraaltrodestra = wb.createCellStyle();
            stylesinsitraaltrodestra.setBorderTop(BorderStyle.DOUBLE);
            stylesinsitraaltrodestra.setBorderLeft(BorderStyle.DOUBLE);
            stylesinsitraaltrodestra.setBorderRight(BorderStyle.DOUBLE);
            stylesinsitraaltrodestra.setFont(font);

            XSSFCellStyle stylealtosinistra = wb.createCellStyle();
            stylealtosinistra.setBorderTop(BorderStyle.DOUBLE);
            stylealtosinistra.setBorderLeft(BorderStyle.DOUBLE);
            stylealtosinistra.setFont(font2);

            XSSFCellStyle stylesinistra = wb.createCellStyle();
            stylesinistra.setBorderLeft(BorderStyle.DOUBLE);
            stylesinistra.setFont(font2);

            XSSFCellStyle styledestra = wb.createCellStyle();
            styledestra.setBorderRight(BorderStyle.DOUBLE);
            styledestra.setFont(font2);
            styledestra.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylevuoto = wb.createCellStyle();
            stylevuoto.setFont(font2);
            stylevuoto.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylevuotonoformat = wb.createCellStyle();
            stylevuotonoformat.setFont(font2);

            XSSFCellStyle stylepercent = wb.createCellStyle();
            stylepercent.setFont(font2);
            stylepercent.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylealto = wb.createCellStyle();
            stylealto.setBorderTop(BorderStyle.DOUBLE);
            stylealto.setFont(font2);

            XSSFCellStyle stylebasso = wb.createCellStyle();
            stylebasso.setBorderBottom(BorderStyle.DOUBLE);
            stylebasso.setFont(font2);
            stylebasso.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylebassoaltosinistra = wb.createCellStyle();
            stylebassoaltosinistra.setBorderBottom(BorderStyle.DOUBLE);
            stylebassoaltosinistra.setBorderTop(BorderStyle.DOUBLE);
            stylebassoaltosinistra.setBorderLeft(BorderStyle.DOUBLE);
            stylebassoaltosinistra.setFont(font2);
            stylebassoaltosinistra.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylebassoalto = wb.createCellStyle();
            stylebassoalto.setBorderBottom(BorderStyle.DOUBLE);
            stylebassoalto.setBorderTop(BorderStyle.DOUBLE);
            stylebassoalto.setFont(font2);
            stylebassoalto.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylebassodestra = wb.createCellStyle();
            stylebassodestra.setBorderBottom(BorderStyle.DOUBLE);
            stylebassodestra.setBorderRight(BorderStyle.DOUBLE);
            stylebassodestra.setFont(font2);
            stylebassodestra.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle stylebassosinistra = wb.createCellStyle();
            stylebassosinistra.setBorderBottom(BorderStyle.DOUBLE);
            stylebassosinistra.setBorderLeft(BorderStyle.DOUBLE);
            stylebassosinistra.setFont(font2);

            XSSFCellStyle stylealtodestra = wb.createCellStyle();
            stylealtodestra.setBorderTop(BorderStyle.DOUBLE);
            stylealtodestra.setBorderRight(BorderStyle.DOUBLE);
            stylealtodestra.setFont(font2);

            int totind = 35;
            int indicecolonnacambio = -1;
            int indicecolonnatotalegenerale = -1;

            Row row = sheet.createRow(0);
            Cell cell = row.createCell(0);
            cell.setCellValue(numgiornimese);

            ArrayList elencoindicinoformule = new ArrayList();

            for (int a = 0; a < elencovalori.size(); a++) {

                Colonna temp = elencovalori.get(a);
                boolean rate = false;
                if (temp.getFormula().equals("N")) {
                    elencoindicinoformule.add(a + 1);
                }
                if (temp.getDesc().equalsIgnoreCase("CAMBIO")) {
                    indicecolonnacambio = a + 1;

                    rate = true;

                }

                if (temp.getDesc().equalsIgnoreCase("TOTALE GENERALE")) {
                    indicecolonnatotalegenerale = a + 1;
                }

                ArrayList<String> elencovaloricolonna = temp.getValori();

                for (int x = 0; x < elencovaloricolonna.size(); x++) {

                    Row row1 = sheet.getRow(x);
                    if (a == 0) {
                        row1 = sheet.createRow(x);
                    }
                    if (x > 2 && a > 0) {
                        Cell cell1 = getCell(row1, a + 1);
                        if (!elencovaloricolonna.get(x).equals("")) {
                            if (rate) {
                                cell1.setCellStyle(cellStyleRATE);
                            } else {
                                cell1.setCellStyle(cellStyle);

                            }
                            cell1.setCellValue(fd(elencovaloricolonna.get(x)));
                            cell1.setCellType(CellType.NUMERIC);
                        } else {
                            cell1.setCellValue("");
                        }

                        if (a == elencovalori.size() - 1 || a == indicecolonnatotalegenerale - 1) {
                            cell1.setCellStyle(styledestra);
                        }

                        if (temp.getDesc().startsWith("%")) {
                            cell1.setCellStyle(stylepercent);
                            cell1.setCellValue(fd(
                                    formatDoubleforMysql(gf, elencovaloricolonna.get(x).replaceAll("%", "").trim())));

                            cell1.setCellType(CellType.NUMERIC);
                        }

                        if (elencovaloricolonna.get(x).equals("Totale")) {
                            cell1.setCellStyle(stylebassoaltosinistra);
                        }

                        if (x == totind - 1) {
                            cell1.setCellStyle(stylebassoalto);
                        }

                    } else {
                        Cell cell1 = getCell(row1, a + 1);
                        cell1.setCellValue(elencovaloricolonna.get(x));
                        // cell1.setCellStyle(stylebasso);
                        if (a == 0) {
                            cell1.setCellStyle(stylesinistra);
                            if (x == totind || x == totind + 4 || x == totind + 6 || x == totind + 9 || x == totind + 13
                                    || x == totind + 16) {
                                cell1.setCellStyle(stylebassosinistra);
                            }

                            if (elencovaloricolonna.get(x).equals("Totale")) {
                                cell1.setCellStyle(stylebassoaltosinistra);
                            }

                        }
                        if (a == elencovalori.size() - 1 || a == indicecolonnatotalegenerale - 1) {
                            cell1.setCellStyle(styledestra);
                        }

                        if (x == totind - 1) {
                            cell1.setCellStyle(stylebassoalto);
                        }

                    }
                }
            }

            // faccio la parte dei totali in basso
            Row row3 = sheet.getRow(totind);
            // for (int y = 0; y < elencovalori.size(); y++) {
            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row3, y + 2);

                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "SUM(" + colletter + "4:" + colletter + "34)";

                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebassoalto);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }

            }

            totind++;
            Row row4 = sheet.getRow(totind);
            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);

                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "36/" + colletter + "42*100%";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                    cell1.setCellStyle(stylevuoto);
                }
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            Row row44 = sheet.getRow(totind);
            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row44, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "36/" + colletter + "43*100%";
                // cell1.setCellFormula(formula);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "37-" + colletter + "38";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                    cell1.setCellStyle(stylevuoto);
                }
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = colletter + "39/" + colletter + "38";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebasso);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "44/" + colletter + "42";
                // cell1.setCellFormula(formula);
                // cell1.setCellStyle(stylebasso);

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "43/" + colletter + "41";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebasso);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "36-" + colletter + "44";
                // cell1.setCellFormula(formula);

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "36-" + colletter + "43";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                    cell1.setCellStyle(stylevuoto);
                }

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = colletter + "44/" + colletter + "43";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebasso);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "47-" + colletter + "48";
                // cell1.setCellFormula(formula);

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "47-" + colletter + "48";
                // cell1.setCellFormula(formula);

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "46-" + colletter + "47";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                    cell1.setCellStyle(stylevuoto);
                }

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = colletter + "48/" + colletter + "47";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebasso);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {

                Cell cell1 = getCell(row4, y + 2);
                // int colindex = cell1.getColumnInde);
                // String colletter = CellReference.convertNumToColString(colindex);
                // String formula = "+" + colletter + "47-" + colletter + "51";
                // cell1.setCellFormula(formula);

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {
                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = "+" + colletter + "46-" + colletter + "50";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                    cell1.setCellStyle(stylevuoto);
                }

                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(styledestra);
                }

            }

            totind++;
            row4 = sheet.getRow(totind);

            for (int y = 0; y < indicecolonnacambio; y++) {
                Cell cell1 = getCell(row4, y + 2);
                int colindex = cell1.getColumnIndex();
                String colletter = CellReference.convertNumToColString(colindex);
                String formula = colletter + "51/" + colletter + "50";
                if (!elencoindicinoformule.contains(y + 2)) {
                    cell1.setCellFormula(formula);
                }
                cell1.setCellStyle(stylebasso);
                if (y == indicecolonnacambio - 1) {
                    cell1.setCellStyle(stylebassodestra);
                }
            }

            Row startrow = sheet.getRow(0);
            Cell startcell = startrow.createCell(0);
            startcell.setCellValue(numgiornimese);
            Row startrow2 = sheet.getRow(1);
            Cell startcell2 = startrow2.createCell(0);
            startcell2.setCellValue(nummese);

            // for (int r = 0; r < elencovalori.size() + 5; r++) {
            //
            // sheet.autoSizeColumn(r);
            // }
            // ---------------- FOGLIO 2 ------------ //////////////////
            // DATI CAMBIO MTD
            // Sheet sheet2 = wb.createSheet("DATI CAMBIO MTD");
            // Sheet sheet2 = wb.createSheet("DATI CAMBIO MTD");
            Sheet sheet2 = wb.getSheetAt(1);

            for (int a = 0; a < elencovaloririgafoglio2.size(); a++) {
                Row rw = sheet2.getRow(a);
                if (rw == null) {
                    rw = sheet2.createRow(a);
                }
                Riga temp = elencovaloririgafoglio2.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();

                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }

                    if (a > 3 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }

                    // if(!haveLetter(elencovalorisingolariga.get(x))){
                    // cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                    // cell1.setCellType(CellType.NUMERIC);
                    // cell1.setCellStyle(cellStyle);
                    // }else{
                    // cell1.setCellValue(elencovalorisingolariga.get(x));
                    // }
                    // cell1.setCellType(CellType.NUMERIC);
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet2.autoSizeColumn(r1);
            // }
            // ---------------- FOGLIO 3 ------------ //////////////////
            // DATI CAMBIO YTD
            Sheet sheet3 = wb.getSheetAt(2);

            for (int a = 0; a < elencovaloririgafoglio3.size(); a++) {
                Row rw = sheet3.getRow(a);
                if (rw == null) {
                    rw = sheet3.createRow(a);
                }
                Riga temp = elencovaloririgafoglio3.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();

                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }
                    if (a > 3 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }
                    // cell1.setCellValue(elencovalorisingolariga.get(x));
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet3.autoSizeColumn(r1);
            // }
            // ---------------- FOGLIO 4 ------------ //////////////////
            // DATI WU MTD
            Sheet sheet4 = wb.getSheetAt(3);

            for (int a = 0; a < elencovaloririgafoglio4.size(); a++) {
                Row rw = sheet4.getRow(a);
                if (rw == null) {
                    rw = sheet4.createRow(a);
                }
                Riga temp = elencovaloririgafoglio4.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();

                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }
                    if (a > 3 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet4.autoSizeColumn(r1);
            // }
            // ---------------- FOGLIO 5 ------------ //////////////////
            // DATI WU YTD
            Sheet sheet5 = wb.getSheetAt(4);

            for (int a = 0; a < elencovaloririgafoglio5.size(); a++) {
                Row rw = sheet5.getRow(a);
                if (rw == null) {
                    rw = sheet5.createRow(a);
                }
                Riga temp = elencovaloririgafoglio5.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();

                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }
                    if (a > 3 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet5.autoSizeColumn(r1);
            // }
            // ---------------- FOGLIO 6 ------------ //////////////////
            // SHEET NO CHANGE MTD
            Sheet sheet6 = wb.getSheetAt(5);
            for (int a = 0; a < elencovaloririgafoglio6.size(); a++) {
                Row rw = sheet6.getRow(a);
                if (rw == null) {
                    rw = sheet6.createRow(a);
                }
                Riga temp = elencovaloririgafoglio6.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();
                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }
                    if (a > 4 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet6.autoSizeColumn(r1);
            // }
            // ---------------- FOGLIO 7 ------------ //////////////////
            // SHEET NO CHANGE YTD
            Sheet sheet7 = wb.getSheetAt(6);
            for (int a = 0; a < elencovaloririgafoglio7.size(); a++) {
                Row rw = sheet7.getRow(a);
                if (rw == null) {
                    rw = sheet7.createRow(a);
                }
                Riga temp = elencovaloririgafoglio7.get(a);
                ArrayList<String> elencovalorisingolariga = temp.getValori();

                for (int x = 0; x < elencovalorisingolariga.size(); x++) {
                    Cell cell1 = rw.getCell(x);
                    if (cell1 == null) {
                        cell1 = rw.createCell(x);
                    }
                    if (a > 4 && x > 1) {
                        cell1.setCellValue(fd(elencovalorisingolariga.get(x)));
                        cell1.setCellType(CellType.NUMERIC);
                        cell1.setCellStyle(cellStyle);
                    } else {
                        cell1.setCellValue(elencovalorisingolariga.get(x));
                    }
                }
            }

            // for (int r1 = 0; r1 < 500; r1++) {
            //
            // sheet7.autoSizeColumn(r1);
            // }
            // Write the output to a file
            try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {

                wb.write(fileOut);
                fileOut.close();
                is.close();
                wb.close();

                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
                // out.delete();
                return base64;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String create_cdc(File outputfile, ArrayList<DailyChange_CG> output, DatabaseCons db) {
        String formatdataCell = "#,#.00";
        String formatdataCellRATE = "#,#.00000000";
        try (InputStream is = new ByteArrayInputStream(Base64.decodeBase64(db.getConf("path.rep1cdc.23")));
                XSSFWorkbook wb = new XSSFWorkbook(is);
                FileOutputStream fileOut = new FileOutputStream(outputfile)) {

            XSSFCellStyle cellStylenum = wb.createCellStyle();
            XSSFDataFormat hssfDataFormat = wb.createDataFormat();
            cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenum.setBorderBottom(BorderStyle.THIN);
            cellStylenum.setBorderTop(BorderStyle.THIN);
            cellStylenum.setBorderRight(BorderStyle.THIN);
            cellStylenum.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle cellStyle = wb.createCellStyle();
            cellStyle.setBorderBottom(BorderStyle.THIN);
            cellStyle.setBorderTop(BorderStyle.THIN);
            cellStyle.setBorderRight(BorderStyle.THIN);
            cellStyle.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle cellStylenumrate = wb.createCellStyle();
            cellStylenumrate.setDataFormat(hssfDataFormat.getFormat(formatdataCellRATE));
            cellStylenumrate.setBorderBottom(BorderStyle.THIN);
            cellStylenumrate.setBorderTop(BorderStyle.THIN);
            cellStylenumrate.setBorderRight(BorderStyle.THIN);
            cellStylenumrate.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle cellStyleint = wb.createCellStyle();
            cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleint.setBorderBottom(BorderStyle.THIN);
            cellStyleint.setBorderTop(BorderStyle.THIN);
            cellStyleint.setBorderRight(BorderStyle.THIN);
            cellStyleint.setBorderLeft(BorderStyle.THIN);

            Sheet sheet = wb.getSheetAt(0);

            CellStyle backgroundStyle = wb.createCellStyle();
            IndexedColorMap colorMap = wb.getStylesSource().getIndexedColors();
            XSSFColor color = new XSSFColor(new java.awt.Color(146, 208, 80), colorMap); // accepts a short value

            XSSFFont font = wb.createFont();
            font.setFontHeightInPoints((short) 11);
            font.setFontName("Calibri");
            font.setColor(IndexedColors.BLACK.getIndex());
            font.setBold(true);
            font.setItalic(false);
            ((XSSFCellStyle) backgroundStyle).setFillForegroundColor(color);
            backgroundStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            backgroundStyle.setBorderBottom(BorderStyle.THIN);
            backgroundStyle.setBorderTop(BorderStyle.THIN);
            backgroundStyle.setBorderRight(BorderStyle.THIN);
            backgroundStyle.setBorderLeft(BorderStyle.THIN);
            backgroundStyle.setVerticalAlignment(VerticalAlignment.CENTER);
            backgroundStyle.setAlignment(HorizontalAlignment.CENTER);

            backgroundStyle.setFont(font);

            int st = 2;
            for (int i = 0; i < output.size(); i++) {
                DailyChange_CG dc = output.get(i);
                Row row = getRow(sheet, st + i);
                Cell c1 = getCell(row, 1);
                c1.setCellValue(dc.getCDC());
                c1 = getCell(row, 2);
                c1.setCellValue(dc.getSPORTELLO());
                c1 = getCell(row, 3);
                c1.setCellValue(dc.getID());
                c1 = getCell(row, 4);
                c1.setCellValue(dc.getDELETE());
                c1 = getCell(row, 5);
                c1.setCellValue(dc.getAREA());
                c1 = getCell(row, 6);
                c1.setCellValue(dc.getCITTA());
                c1 = getCell(row, 7);
                c1.setCellValue(dc.getUBICAZIONE());
                c1 = getCell(row, 8);
                c1.setCellValue(dc.getGRUPPO());
                c1 = getCell(row, 9);
                c1.setCellValue(dc.getDATA());
                c1 = getCell(row, 10);
                c1.setCellValue(dc.getORA());
                c1 = getCell(row, 11);
                c1.setCellValue(dc.getMESE());
                c1 = getCell(row, 12);
                c1.setCellValue(dc.getANNO());
                c1 = getCell(row, 13);
                c1.setCellValue(dc.getCODUSER());
                c1 = getCell(row, 14);
                c1.setCellValue(dc.getUSERNOME());
                c1 = getCell(row, 15);
                c1.setCellValue(dc.getUSERCOGNOME());
                c1 = getCell(row, 16);
                c1.setCellValue(dc.getMETODOPAGAMENTO());
                c1 = getCell(row, 17);
                c1.setCellValue(dc.getRESIDENZACLIENTE());
                c1 = getCell(row, 18);
                c1.setCellValue(dc.getNAZIONALITACLIENTE());
                c1 = getCell(row, 19);
                c1.setCellValue(dc.getCOMMENTI());
                c1 = getCell(row, 20);
                c1.setCellValue(dc.getACQUISTOVENDITA());
                c1 = getCell(row, 21);
                c1.setCellValue(dc.getTIPOLOGIAACQOVEND());
                c1 = getCell(row, 22);
                c1.setCellValue(dc.getVALUTA());
                c1 = getCell(row, 23, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getQUANTITA()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 24, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getTASSODICAMBIO()));
                c1.setCellStyle(cellStylenumrate);

                c1 = getCell(row, 25, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getCONTROVALORE()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 26, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getCOMMVARIABILE()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 27, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getCOMMFISSA()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 28, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getSPREADBRANCH()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 29, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getSPREADBANK()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 30, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getSPREADVEND()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 31, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getTOTGM()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 32, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getPERCCOMM()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 33, CellType.NUMERIC);
                c1.setCellValue(fd(dc.getPERCSPREADVENDITA()));
                c1.setCellStyle(cellStylenum);

                if (dc.getVENDITABUYBACK().equals("") || dc.getVENDITABUYBACK().startsWith("F")) {
                    c1 = getCell(row, 34);
                    c1.setCellValue(dc.getVENDITABUYBACK());
                } else {
                    c1 = getCell(row, 34, CellType.NUMERIC);
                    c1.setCellValue(fd(dc.getVENDITABUYBACK()));
                    c1.setCellStyle(cellStylenum);
                }
                if (dc.getVENDITASELLBACK().equals("") || dc.getVENDITASELLBACK().startsWith("F")) {
                    c1 = getCell(row, 35);
                    c1.setCellValue(dc.getVENDITASELLBACK());
                } else {
                    c1 = getCell(row, 35, CellType.NUMERIC);
                    c1.setCellValue(fd(dc.getVENDITASELLBACK()));
                    c1.setCellStyle(cellStylenum);
                }

                c1 = getCell(row, 36);
                c1.setCellValue(dc.getCODICEINTERNETBOOKING());
                c1 = getCell(row, 37);
                c1.setCellValue(dc.getMOTIVOPERRIDUZIONEDELLACOMM());
                c1 = getCell(row, 38);
                c1.setCellValue(dc.getMOTIVOPERRIDUZIONEDELLACOMMFISSA());

                c1 = getCell(row, 39);
                c1.setCellValue(dc.getCODICESBLOCCO());

                c1 = getCell(row, 40);
                c1.setCellValue(dc.getLOYALTYCODE());
                c1.setCellStyle(cellStyle);

                c1 = getCell(row, 41);
                c1.setCellValue(dc.getAGCODE());
                c1.setCellStyle(cellStyle);
                c1 = getCell(row, 42);
                c1.setCellValue(dc.getAGNUMBER());
                c1.setCellStyle(cellStyle);

                c1 = getCell(row, 43);
                c1.setCellValue(dc.getIBCHAN());
                c1.setCellStyle(cellStyle);

                if (dc.getCBCOMM().trim().equals("")) {
                    c1 = getCell(row, 44);
                    c1.setCellValue("");
                } else {
                    c1 = getCell(row, 44, CellType.NUMERIC);
                    c1.setCellValue(fd(dc.getCBCOMM()));
                    c1.setCellStyle(cellStylenum);
                }

            }

            for (int r = 0; r < 45; r++) {
                sheet.autoSizeColumn(r);
            }

            wb.write(fileOut);

            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    // public static String create_cdcOLD(File outputfile, ArrayList<DailyChange_CG>
    // output, DatabaseCons db) {
    // String formatdataCell = "#,#.00";
    // String formatdataCellRATE = "#,#.00000000";
    // try {
    //// InputStream is = new FileInputStream(new File("C:\\Maccorp\\Report 1 CDC 2
    // .xlsx"));
    // InputStream is = new
    // ByteArrayInputStream(Base64.decodeBase64(db.getConf("path.rep1cdc")));
    // XSSFWorkbook wb = new XSSFWorkbook(is);
    // XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
    // XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
    // cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
    // cellStylenum.setBorderBottom(BorderStyle.THIN);
    // cellStylenum.setBorderTop(BorderStyle.THIN);
    // cellStylenum.setBorderRight(BorderStyle.THIN);
    // cellStylenum.setBorderLeft(BorderStyle.THIN);
    //
    // XSSFCellStyle cellStyle = (XSSFCellStyle) wb.createCellStyle();
    // cellStyle.setBorderBottom(BorderStyle.THIN);
    // cellStyle.setBorderTop(BorderStyle.THIN);
    // cellStyle.setBorderRight(BorderStyle.THIN);
    // cellStyle.setBorderLeft(BorderStyle.THIN);
    //
    // XSSFCellStyle cellStylenumrate = (XSSFCellStyle) wb.createCellStyle();
    // cellStylenumrate.setDataFormat(hssfDataFormat.getFormat(formatdataCellRATE));
    // cellStylenumrate.setBorderBottom(BorderStyle.THIN);
    // cellStylenumrate.setBorderTop(BorderStyle.THIN);
    // cellStylenumrate.setBorderRight(BorderStyle.THIN);
    // cellStylenumrate.setBorderLeft(BorderStyle.THIN);
    //
    // XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
    // cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
    // cellStyleint.setBorderBottom(BorderStyle.THIN);
    // cellStyleint.setBorderTop(BorderStyle.THIN);
    // cellStyleint.setBorderRight(BorderStyle.THIN);
    // cellStyleint.setBorderLeft(BorderStyle.THIN);
    //
    // Sheet sheet = wb.getSheetAt(0);
    //
    // CellStyle backgroundStyle = wb.createCellStyle();
    // byte[] rgb = {(byte) 146, (byte) 208, (byte) 80};
    // XSSFColor color = new XSSFColor(rgb, new DefaultIndexedColorMap());
    //
    // XSSFFont font = wb.createFont();
    // font.setFontHeightInPoints((short) 11);
    // font.setFontName("Calibri");
    // font.setColor(IndexedColors.BLACK.getIndex());
    // font.setBold(true);
    // font.setItalic(false);
    // ((XSSFCellStyle) backgroundStyle).setFillForegroundColor(color);
    // backgroundStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
    // backgroundStyle.setBorderBottom(BorderStyle.THIN);
    // backgroundStyle.setBorderTop(BorderStyle.THIN);
    // backgroundStyle.setBorderRight(BorderStyle.THIN);
    // backgroundStyle.setBorderLeft(BorderStyle.THIN);
    // backgroundStyle.setVerticalAlignment(VerticalAlignment.CENTER);
    // backgroundStyle.setAlignment(HorizontalAlignment.CENTER);
    // backgroundStyle.setFont(font);
    //
    // Row row0 = getRow(sheet, 1);
    // Cell c10 = getCell(row0, 40);
    // c10.setCellValue("LOYALTY CODE");
    // c10.setCellStyle(backgroundStyle);
    //
    // int st = 2;
    // for (int i = 0; i < output.size(); i++) {
    // DailyChange_CG dc = output.get(i);
    // Row row = getRow(sheet, st + i);
    // Cell c1 = getCell(row, 1);
    // c1.setCellValue(dc.getCDC());
    // c1 = getCell(row, 2);
    // c1.setCellValue(dc.getSPORTELLO());
    // c1 = getCell(row, 3);
    // c1.setCellValue(dc.getID());
    // c1 = getCell(row, 4);
    // c1.setCellValue(dc.getDELETE());
    // c1 = getCell(row, 5);
    // c1.setCellValue(dc.getAREA());
    // c1 = getCell(row, 6);
    // c1.setCellValue(dc.getCITTA());
    // c1 = getCell(row, 7);
    // c1.setCellValue(dc.getUBICAZIONE());
    // c1 = getCell(row, 8);
    // c1.setCellValue(dc.getGRUPPO());
    // c1 = getCell(row, 9);
    // c1.setCellValue(dc.getDATA());
    // c1 = getCell(row, 10);
    // c1.setCellValue(dc.getORA());
    // c1 = getCell(row, 11);
    // c1.setCellValue(dc.getMESE());
    // c1 = getCell(row, 12);
    // c1.setCellValue(dc.getANNO());
    // c1 = getCell(row, 13);
    // c1.setCellValue(dc.getCODUSER());
    // c1 = getCell(row, 14);
    // c1.setCellValue(dc.getUSERNOME());
    // c1 = getCell(row, 15);
    // c1.setCellValue(dc.getUSERCOGNOME());
    // c1 = getCell(row, 16);
    // c1.setCellValue(dc.getMETODOPAGAMENTO());
    // c1 = getCell(row, 17);
    // c1.setCellValue(dc.getRESIDENZACLIENTE());
    // c1 = getCell(row, 18);
    // c1.setCellValue(dc.getNAZIONALITACLIENTE());
    // c1 = getCell(row, 19);
    // c1.setCellValue(dc.getCOMMENTI());
    // c1 = getCell(row, 20);
    // c1.setCellValue(dc.getACQUISTOVENDITA());
    // c1 = getCell(row, 21);
    // c1.setCellValue(dc.getTIPOLOGIAACQOVEND());
    // c1 = getCell(row, 22);
    // c1.setCellValue(dc.getVALUTA());
    // c1 = getCell(row, 23);
    // c1.setCellValue(fd(dc.getQUANTITA()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    //
    // c1 = getCell(row, 24);
    // c1.setCellValue(fd(dc.getTASSODICAMBIO()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenumrate);
    //
    // c1 = getCell(row, 25);
    // c1.setCellValue(fd(dc.getCONTROVALORE()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 26);
    // c1.setCellValue(fd(dc.getCOMMVARIABILE()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 27);
    // c1.setCellValue(fd(dc.getCOMMFISSA()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 28);
    // c1.setCellValue(fd(dc.getSPREADBRANCH()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 29);
    // c1.setCellValue(fd(dc.getSPREADBANK()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 30);
    // c1.setCellValue(fd(dc.getSPREADVEND()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 31);
    // c1.setCellValue(fd(dc.getTOTGM()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 32);
    // c1.setCellValue(fd(dc.getPERCCOMM()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 33);
    // c1.setCellValue(fd(dc.getPERCSPREADVENDITA()));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 34);
    // c1.setCellValue(fd(formatDoubleforMysql(db.getGf(),
    // dc.getVENDITABUYBACK())));
    // c1.setCellType(CellType.NUMERIC);
    // c1.setCellStyle(cellStylenum);
    // c1 = getCell(row, 35);
    // c1.setCellValue(dc.getCODICEINTERNETBOOKING());
    // c1 = getCell(row, 36);
    // c1.setCellValue(dc.getFASCEIMPORTO());
    // c1 = getCell(row, 37);
    // c1.setCellValue(dc.getMOTIVOPERRIDUZIONEDELLACOMM());
    // c1 = getCell(row, 38);
    // c1.setCellValue(dc.getMOTIVOPERRIDUZIONEDELLACOMMFISSA());
    // c1 = getCell(row, 39);
    // c1.setCellValue(dc.getCODICESBLOCCO());
    // c1 = getCell(row, 39);
    // c1.setCellValue(dc.getCODICESBLOCCO());
    // c1 = getCell(row, 40);
    // c1.setCellValue(dc.getLOYALTYCODE());
    // c1.setCellStyle(cellStyle);
    // }
    //
    // for (int r = 0; r < 41; r++) {
    // sheet.autoSizeColumn(r);
    // }
    //
    // try {
    // FileOutputStream fileOut = new FileOutputStream(outputfile);
    // wb.write(fileOut);
    // fileOut.close();
    // is.close();
    // wb.close();
    //
    // String base64 = new
    // String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
    // return base64;
    // } catch (IOException ex) {
    // ex.printStackTrace();
    // }
    //
    // } catch (FileNotFoundException ex) {
    // ex.printStackTrace();
    // } catch (IOException ex) {
    // ex.printStackTrace();
    // }
    //
    // return null;
    // }
    public static String limit_insurance(File outputfile, ArrayList<String> primatabella,
            ArrayList<String> secondatabella,
            ArrayList<String> terzatabella, ArrayList<String> quartatabella, ArrayList<LimitInsur> li,
            ArrayList<Branch> fil1,
            ArrayList<String> giornidacontrollare,
            ArrayList<String> giornidacontrollarestring) {
        try {
            String formatdataCell = "#,#.00";
            XSSFWorkbook wb = new XSSFWorkbook();

            Sheet sheet = wb.createSheet("CONTROLLO MASSIMALI");

            XSSFFont font = (XSSFFont) wb.createFont();
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 12);
            font.setBold(true);

            XSSFDataFormat hssfDataFormat = wb.createDataFormat();

            CellStyle style = wb.createCellStyle(); // Create new style
            // style.setWrapText(true); //Set wordwrap
            style.setFont(font);
            style.setBorderBottom(BorderStyle.DOUBLE);
            style.setBorderTop(BorderStyle.DOUBLE);
            style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
            style.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            CellStyle styleW = wb.createCellStyle(); // Create new style
            styleW.setWrapText(true); // Set wordwrap
            styleW.setFont(font);
            styleW.setBorderBottom(BorderStyle.DOUBLE);
            styleW.setBorderTop(BorderStyle.DOUBLE);
            styleW.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
            styleW.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            CellStyle styleN = wb.createCellStyle(); // Create new style
            // style.setWrapText(true); //Set wordwrap
            styleN.setFont(font);
            styleN.setBorderBottom(BorderStyle.DOUBLE);
            styleN.setBorderTop(BorderStyle.DOUBLE);
            styleN.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
            styleN.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            styleN.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

            XSSFCellStyle cellStylenum = wb.createCellStyle();

            cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenum.setBorderBottom(BorderStyle.THIN);

            CellStyle styleNORM = wb.createCellStyle();

            styleNORM.setBorderBottom(BorderStyle.THIN);

            int riga = 0;

            Row row;

            AtomicInteger ind = new AtomicInteger(riga);
            AtomicInteger first = new AtomicInteger(0);
            AtomicInteger ind_x = new AtomicInteger(0);

            primatabella.forEach(string -> {
                Row r1 = getRow(sheet, ind.get());
                Cell cell = getCell(r1, ind_x.get());
                XSSFRichTextString richString = new XSSFRichTextString(string);

                if (ind.get() == 0) {
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                    if (first.get() == 0) {
                        ind_x.addAndGet(2);
                        sheet.addMergedRegion(new CellRangeAddress(first.get(), first.get() + 2, 0, 1));
                    } else {
                        ind.addAndGet(1);
                    }

                    first.addAndGet(1);
                } else if (ind.get() % 2 == 0) {
                    ind.set(0);
                    ind_x.addAndGet(1);
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(fd(string));
                    cell.setCellStyle(styleN);
                } else {
                    ind.addAndGet(1);
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                }

            });

            riga = 2;

            // for (int i = 0; i < primatabella.size(); i++) {
            // Cell cell = row.createCell(i);
            //
            // String complete = primatabella.get(i);
            //
            // XSSFRichTextString richString = new XSSFRichTextString(complete);
            // cell.setCellValue(richString);
            // cell.setCellStyle(style);
            // }
            for (int i = 0; i < giornidacontrollare.size(); i++) {

                riga++;
                row = sheet.createRow(riga);
                String giorno1 = giornidacontrollare.get(i);
                Cell cell0 = row.createCell(0);
                cell0.setCellValue(giorno1);
                cell0.setCellStyle(styleNORM);
                Cell cell1 = row.createCell(1);
                cell1.setCellValue(StringUtils.capitalize(giornidacontrollarestring.get(i)));
                cell1.setCellStyle(styleNORM);
                for (int x = 0; x < fil1.size(); x++) {
                    Branch b = fil1.get(x);
                    for (int r = 0; r < li.size(); r++) {
                        LimitInsur lins = li.get(r);
                        if (lins.getFiliale().equals(b.getCod()) && lins.getGiorno().equals(giorno1)) {
                            cell1 = row.createCell(2 + x);
                            cell1.setCellType(CellType.NUMERIC);
                            cell1.setCellValue(fd(lins.getCop()));
                            cell1.setCellStyle(cellStylenum);
                        }
                    }
                }
            }

            riga = riga + 34 - giornidacontrollare.size();

            AtomicInteger start = new AtomicInteger(riga);

            first.set(0);
            ind.set(start.get());
            ind_x.set(0);

            secondatabella.forEach(string -> {

                Row r1 = getRow(sheet, ind.get());
                Cell cell = getCell(r1, ind_x.get());
                XSSFRichTextString richString = new XSSFRichTextString(string);

                if (ind.get() == start.get()) {
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                    if (first.get() == 0) {
                        ind_x.addAndGet(2);
                        sheet.addMergedRegion(new CellRangeAddress(start.get(), start.get() + 2, 0, 1));
                    } else {
                        ind.addAndGet(1);
                    }
                    first.addAndGet(1);
                } else if (ind.get() % 2 == 0) {
                    ind.set(start.get());
                    ind_x.addAndGet(1);
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(fd(string));
                    cell.setCellStyle(styleN);
                } else {
                    ind.addAndGet(1);
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                }
            });

            riga = 38;

            // row = sheet.createRow(riga);
            // for (int i = 0; i < secondatabella.size(); i++) {
            // Cell cell = row.createCell(i);
            // cell.setCellValue(secondatabella.get(i));
            // cell.setCellStyle(style);
            // }
            for (int i = 0; i < giornidacontrollare.size(); i++) {
                riga++;
                row = sheet.createRow(riga);
                String giorno1 = giornidacontrollare.get(i);
                Cell cell0 = row.createCell(0);
                cell0.setCellValue(giorno1);
                cell0.setCellStyle(styleNORM);
                Cell cell1 = row.createCell(1);
                cell1.setCellValue(StringUtils.capitalize(giornidacontrollarestring.get(i)));
                cell1.setCellStyle(styleNORM);
                for (int x = 0; x < fil1.size(); x++) {
                    Branch b = fil1.get(x);
                    for (int r = 0; r < li.size(); r++) {
                        LimitInsur lins = li.get(r);
                        if (lins.getFiliale().equals(b.getCod()) && lins.getGiorno().equals(giorno1)) {
                            cell1 = row.createCell(2 + x);
                            cell1.setCellType(CellType.NUMERIC);
                            cell1.setCellValue(fd(lins.getFx()));
                            cell1.setCellStyle(cellStylenum);
                        }
                    }
                }
            }

            riga = riga + 34 - giornidacontrollare.size();

            start.set(riga);
            first.set(0);
            ind.set(start.get());
            ind_x.set(0);

            terzatabella.forEach(string -> {

                Row r1 = getRow(sheet, ind.get());
                Cell cell = getCell(r1, ind_x.get());
                XSSFRichTextString richString = new XSSFRichTextString(string);

                if (ind.get() == start.get()) {
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                    if (first.get() == 0) {
                        ind_x.addAndGet(2);
                        sheet.addMergedRegion(new CellRangeAddress(start.get(), start.get() + 2, 0, 1));
                    } else {
                        ind.addAndGet(1);
                    }
                    first.addAndGet(1);
                } else if (ind.get() % 2 == 0) {
                    ind.set(start.get());
                    ind_x.addAndGet(1);
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(fd(string));
                    cell.setCellStyle(styleN);
                } else {
                    ind.addAndGet(1);
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                }
            });

            riga = 74;

            for (int i = 0; i < giornidacontrollare.size(); i++) {
                riga++;
                row = sheet.createRow(riga);
                String giorno1 = giornidacontrollare.get(i);
                Cell cell0 = row.createCell(0);
                cell0.setCellValue(giorno1);
                cell0.setCellStyle(styleNORM);
                Cell cell1 = row.createCell(1);
                cell1.setCellValue(StringUtils.capitalize(giornidacontrollarestring.get(i)));
                cell1.setCellStyle(styleNORM);
                for (int x = 0; x < fil1.size(); x++) {
                    Branch b = fil1.get(x);
                    for (int r = 0; r < li.size(); r++) {
                        LimitInsur lins = li.get(r);
                        if (lins.getFiliale().equals(b.getCod()) && lins.getGiorno().equals(giorno1)) {
                            cell1 = row.createCell(2 + x);
                            cell1.setCellType(CellType.NUMERIC);
                            cell1.setCellValue(fd(lins.getTot()));
                            cell1.setCellStyle(cellStylenum);
                        }
                    }
                }
            }

            riga = riga + 34 - giornidacontrollare.size();

            start.set(riga);
            first.set(0);
            ind.set(start.get());
            ind_x.set(0);

            quartatabella.forEach(string -> {

                Row r1 = getRow(sheet, ind.get());
                Cell cell = getCell(r1, ind_x.get());
                XSSFRichTextString richString = new XSSFRichTextString(string);

                if (ind.get() == start.get()) {
                    cell.setCellValue(richString);
                    cell.setCellStyle(styleW);
                    if (first.get() == 0) {
                        ind_x.addAndGet(2);
                        sheet.addMergedRegion(new CellRangeAddress(start.get(), start.get() + 2, 0, 1));
                    } else {
                        ind.addAndGet(1);
                    }
                    first.addAndGet(1);
                } else if (ind.get() % 2 == 0) {
                    ind.set(start.get());
                    ind_x.addAndGet(1);
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(fd(string));
                    cell.setCellStyle(styleN);
                } else {
                    ind.addAndGet(1);
                    cell.setCellValue(richString);
                    cell.setCellStyle(style);
                }
            });

            riga = 110;

            for (int i = 0; i < giornidacontrollare.size(); i++) {
                riga++;
                row = sheet.createRow(riga);
                String giorno1 = giornidacontrollare.get(i);
                Cell cell0 = row.createCell(0);
                cell0.setCellValue(giorno1);
                cell0.setCellStyle(styleNORM);
                Cell cell1 = row.createCell(1);
                cell1.setCellValue(StringUtils.capitalize(giornidacontrollarestring.get(i)));
                cell1.setCellStyle(styleNORM);
                for (int x = 0; x < fil1.size(); x++) {
                    Branch b = fil1.get(x);
                    for (int r = 0; r < li.size(); r++) {
                        LimitInsur lins = li.get(r);
                        if (lins.getFiliale().equals(b.getCod()) && lins.getGiorno().equals(giorno1)) {
                            cell1 = row.createCell(2 + x);
                            cell1.setCellType(CellType.NUMERIC);
                            cell1.setCellValue(fd(lins.getDelta()));
                            cell1.setCellStyle(cellStylenum);
                        }
                    }
                }
            }

            for (int i = 0; i < quartatabella.size(); i++) {
                sheet.autoSizeColumn(i);
            }

            try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                wb.write(fileOut);
                fileOut.close();
                wb.close();
                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
                return base64;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String create_changeacc1(File outputfile, ArrayList<DailyChange> output, DatabaseCons db) {

        try {

            // InputStream is = new FileInputStream(new File("C:\\Maccorp\\Report1 CDC
            // T.xlsx"));
            // InputStream is = new
            // ByteArrayInputStream(Base64.decodeBase64(Engine.getConf("path.rep1")));
            String formatdataCell = "#,#.00";
            InputStream is = new ByteArrayInputStream(Base64.decodeBase64(db.getConf("path.rep1.2023")));
            XSSFWorkbook wb = new XSSFWorkbook(is);
            // Sheet sheet = wb.createSheet("RC");
            Sheet sheet = wb.getSheetAt(0);

            XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
            XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
            cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenum.setBorderBottom(THIN);
            cellStylenum.setBorderTop(THIN);
            cellStylenum.setBorderRight(THIN);
            cellStylenum.setBorderLeft(THIN);

            XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
            cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleint.setBorderBottom(THIN);
            cellStyleint.setBorderTop(THIN);
            cellStyleint.setBorderRight(THIN);
            cellStyleint.setBorderLeft(THIN);

            double getVOLUMEAC = 0.00;
            double getVOLUMECA = 0.00;
            double getVOLUMETC = 0.00;
            double getTRANSAC = 0.00;
            double getTRANSCA = 0.00;
            double getTRANSTC = 0.00;
            double getCOMMAC = 0.00;
            double getCOMMCA = 0.00;
            double getCOMMTC = 0.00;

            double getSPREADAC = 0.00;
            double getSPREADCA = 0.00;
            double getSPREADBR = 0.00;
            double getSPREADBA = 0.00;
            int getTOTTRANSACQ = 0;
            double getTOTVOLACQ = 0.00;
            double getTOTGMACQ = 0.00;
            double getPERCACQ = 0.00;
            double getVOLUMEVENDOFF = 0.00;
            double getVOLUMEONL = 0.00;
            double getVOLUMERIVA = 0.00;

            int getTRANSVENDOFF = 0;
            int getTRANSONL = 0;
            int getTRANSRIVA = 0;

            double getCOMMVENDOFF = 0.00;
            double getCOMMONL = 0.00;
            double getCOMMRIVA = 0.00;
            double getSPREADVEND = 0.00;
            double getTOTVOLVEN = 0.00;

            int getTOTTRANSVEN = 0;

            double getTOTGMVEN = 0.00;
            double getPERCVEN = 0.00;
            double getTOTVOL = 0.00;

            int getTOTTRANS = 0;

            double getTOTGM = 0.0;
            double getPERCVEND = 0.0;
            double getCOP = 0.0;
            double getTOBANKCOP = 0.0;
            double getFRBANKCOP = 0.0;
            double getTOBRCOP = 0.0;
            double getFRBRCOP = 0.0;
            double getOCERRCOP = 0.0;
            double getFX = 0.0;

            double getTOBANKFX = 0.0;
            double getFRBANKFX = 0.0;
            double getTOBRFX = 0.0;
            double getFRBRFX = 0.0;
            double getOCERRFX = 0.0;

            int st = 4;
            int lastindex = 0;
            for (int i = 0; i < output.size(); i++) {
                DailyChange dc = output.get(i);
                lastindex = st + i;
                Row row = getRow(sheet, lastindex);
                Cell c1 = getCell(row, 1);
                c1.setCellValue(dc.getFiliale());

                c1 = getCell(row, 2);
                c1.setCellValue(dc.getDescr());

                c1 = getCell(row, 3);
                c1.setCellValue(dc.getData());
                c1 = getCell(row, 4, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMEAC()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 5, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMECA()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 6, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMETC()));

                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 7, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSAC()));

                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 8, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSCA()));

                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 9, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSTC()));
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 10, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMAC()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 11, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMCA()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 12, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMTC()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 13, NUMERIC);
                c1.setCellValue(fd(dc.getSPREADAC()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 14, NUMERIC);
                c1.setCellValue(fd(dc.getSPREADCA()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 15, NUMERIC);
                c1.setCellValue(fd(dc.getSPREADBR()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 16, NUMERIC);
                c1.setCellValue(fd(dc.getSPREADBA()));
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 17, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTOTTRANSACQ()));
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 18, NUMERIC);
                c1.setCellValue(fd(dc.getTOTVOLACQ()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 19, NUMERIC);
                c1.setCellValue(fd(dc.getTOTGMACQ()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 20, NUMERIC);
                c1.setCellValue(fd(dc.getPERCACQ()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 21, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMEVENDOFF()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 22, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMEONL()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 23, NUMERIC);
                c1.setCellValue(fd(dc.getVOLUMERIVA()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 24, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSVENDOFF()));
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 25, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSONL()));
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 26, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTRANSRIVA()));
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 27, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMVENDOFF()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 28, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMONL()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 29, NUMERIC);
                c1.setCellValue(fd(dc.getCOMMRIVA()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 30, NUMERIC);
                c1.setCellValue(fd(dc.getSPREADVEND()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 31, NUMERIC);
                c1.setCellValue(fd(dc.getTOTVOLVEN()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 32, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTOTTRANSVEN()));
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 33, NUMERIC);
                c1.setCellValue(fd(dc.getTOTGMVEN()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 34, NUMERIC);
                c1.setCellValue(fd(dc.getPERCVEN()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 35, NUMERIC);
                c1.setCellValue(fd(dc.getTOTVOL()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 36, NUMERIC);
                c1.setCellValue(parseIntR(dc.getTOTTRANS()));
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 37, NUMERIC);
                c1.setCellValue(fd(dc.getTOTGM()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 38, NUMERIC);
                c1.setCellValue(fd(dc.getPERCVEND()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 39, NUMERIC);
                c1.setCellValue(fd(dc.getCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 40, NUMERIC);
                c1.setCellValue(fd(dc.getTOBANKCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 41, NUMERIC);
                c1.setCellValue(fd(dc.getFRBANKCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 42, NUMERIC);
                c1.setCellValue(fd(dc.getTOBRCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 43, NUMERIC);
                c1.setCellValue(fd(dc.getFRBRCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 44, NUMERIC);
                c1.setCellValue(fd(dc.getOCERRCOP()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 45, NUMERIC);
                c1.setCellValue(fd(dc.getFX()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 46, NUMERIC);
                c1.setCellValue(fd(dc.getTOBANKFX()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 47, NUMERIC);
                c1.setCellValue(fd(dc.getFRBANKFX()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 48, NUMERIC);
                c1.setCellValue(fd(dc.getTOBRFX()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 49, NUMERIC);
                c1.setCellValue(fd(dc.getFRBRFX()));
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 50, NUMERIC);
                c1.setCellValue(fd(dc.getOCERRFX()));
                c1.setCellStyle(cellStylenum);

                getVOLUMEAC += fd(dc.getVOLUMEAC());
                getVOLUMECA += fd(dc.getVOLUMECA());
                getVOLUMETC += fd(dc.getVOLUMETC());
                getTRANSAC += parseIntR(dc.getTRANSAC());
                getTRANSCA += parseIntR(dc.getTRANSCA());
                getTRANSTC += parseIntR(dc.getTRANSTC());
                getCOMMAC += fd(dc.getCOMMAC());
                getCOMMCA += fd(dc.getCOMMCA());
                getCOMMTC += fd(dc.getCOMMTC());

                getSPREADAC += fd(dc.getSPREADAC());
                getSPREADCA += fd(dc.getSPREADCA());

                getSPREADBR += fd(dc.getSPREADBR());
                getSPREADBA += fd(dc.getSPREADBA());
                getTOTTRANSACQ += parseIntR(dc.getTOTTRANSACQ());
                getTOTVOLACQ += fd(dc.getTOTVOLACQ());
                getTOTGMACQ += fd(dc.getTOTGMACQ());
                getPERCACQ += fd(dc.getPERCACQ());
                getVOLUMEVENDOFF += fd(dc.getVOLUMEVENDOFF());
                getVOLUMEONL += fd(dc.getVOLUMEONL());
                getVOLUMERIVA += fd(dc.getVOLUMERIVA());
                getTRANSVENDOFF += parseIntR(dc.getTRANSVENDOFF());
                getTRANSONL += parseIntR(dc.getTRANSONL());
                getTRANSRIVA += parseIntR(dc.getTRANSRIVA());
                getCOMMVENDOFF += fd(dc.getCOMMVENDOFF());
                getCOMMONL += fd(dc.getCOMMONL());
                getCOMMRIVA += fd(dc.getCOMMRIVA());
                getSPREADVEND += fd(dc.getSPREADVEND());
                getTOTVOLVEN += fd(dc.getTOTVOLVEN());
                getTOTTRANSVEN += parseIntR(dc.getTOTTRANSVEN());
                getTOTGMVEN += fd(dc.getTOTGMVEN());
                getPERCVEN += fd(dc.getPERCVEN());
                getTOTVOL += fd(dc.getTOTVOL());
                getTOTTRANS += parseIntR(dc.getTOTTRANS());
                getTOTGM += fd(dc.getTOTGM());
                getPERCVEND += fd(dc.getPERCVEND());
                getCOP += fd(dc.getCOP());
                getTOBANKCOP += fd(dc.getTOBANKCOP());
                getFRBANKCOP += fd(dc.getFRBANKCOP());
                getTOBRCOP += fd(dc.getTOBRCOP());
                getFRBRCOP += fd(dc.getFRBRCOP());
                getOCERRCOP += fd(dc.getOCERRCOP());
                getFX += fd(dc.getFX());
                getTOBANKFX += fd(dc.getTOBANKFX());
                getFRBANKFX += fd(dc.getFRBANKFX());
                getTOBRFX += fd(dc.getTOBRFX());
                getFRBRFX += fd(dc.getFRBRFX());
                getOCERRFX += fd(dc.getOCERRFX());

            }

            XSSFFont font = (XSSFFont) wb.createFont();
            font.setFontName(FONT_ARIAL);
            font.setFontHeightInPoints((short) 11);
            font.setBold(true);

            XSSFCellStyle cellStyleblanktot = (XSSFCellStyle) wb.createCellStyle();
            cellStyleblanktot.setBorderRight(THIN);
            cellStyleblanktot.setBorderBottom(THICK);

            cellStyleblanktot.setBorderTop(THICK);
            cellStyleblanktot.setBorderLeft(THIN);
            cellStyleblanktot.setFont(font);

            XSSFCellStyle cellStylenumtot = (XSSFCellStyle) wb.createCellStyle();
            cellStylenumtot.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenumtot.setBorderRight(THIN);
            cellStylenumtot.setBorderBottom(THICK);
            cellStylenumtot.setBorderTop(THICK);
            cellStylenumtot.setBorderLeft(THIN);
            cellStylenumtot.setFont(font);

            XSSFCellStyle cellStyleinttot = (XSSFCellStyle) wb.createCellStyle();
            cellStyleinttot.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleinttot.setBorderRight(THIN);
            cellStyleinttot.setBorderBottom(THICK);
            cellStyleinttot.setBorderTop(THICK);
            cellStyleinttot.setBorderLeft(THIN);
            cellStyleinttot.setFont(font);

            Row row = getRow(sheet, lastindex + 3);
            Cell c1 = getCell(row, 1);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("");
            c1 = getCell(row, 2);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("TOTALE");
            c1 = getCell(row, 3);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("");
            c1 = getCell(row, 4, NUMERIC);
            c1.setCellValue(getVOLUMEAC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 5, NUMERIC);
            c1.setCellValue(getVOLUMECA);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 6, NUMERIC);
            c1.setCellValue(getVOLUMETC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 7, NUMERIC);
            c1.setCellValue(getTRANSAC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 8, NUMERIC);
            c1.setCellValue(getTRANSCA);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 9, NUMERIC);
            c1.setCellValue(getTRANSTC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 10, NUMERIC);
            c1.setCellValue(getCOMMAC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 11, NUMERIC);
            c1.setCellValue(getCOMMCA);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 12, NUMERIC);
            c1.setCellValue(getCOMMTC);
            c1.setCellStyle(cellStylenumtot);

            // NEW
            c1 = getCell(row, 13, NUMERIC);
            c1.setCellValue(getSPREADAC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 14, NUMERIC);
            c1.setCellValue(getSPREADCA);
            c1.setCellStyle(cellStylenumtot);
            // NEW

            c1 = getCell(row, 15, NUMERIC);
            c1.setCellValue(getSPREADBR);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 16, NUMERIC);
            c1.setCellValue(getSPREADBA);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 17, NUMERIC);
            c1.setCellValue(getTOTTRANSACQ);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 18, NUMERIC);
            c1.setCellValue(getTOTVOLACQ);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 19, NUMERIC);
            c1.setCellValue(getTOTGMACQ);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 20, NUMERIC);
            c1.setCellValue(divisione_controllozero(getPERCACQ, output.size()));
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 21, NUMERIC);
            c1.setCellValue(getVOLUMEVENDOFF);

            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 22, NUMERIC);
            c1.setCellValue(getVOLUMEONL);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 23, NUMERIC);
            c1.setCellValue(getVOLUMERIVA);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 24, NUMERIC);
            c1.setCellValue(getTRANSVENDOFF);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 25, NUMERIC);
            c1.setCellValue(getTRANSONL);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 26, NUMERIC);
            c1.setCellValue(getTRANSRIVA);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 27, NUMERIC);
            c1.setCellValue(getCOMMVENDOFF);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 28, NUMERIC);
            c1.setCellValue(getCOMMONL);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 29, NUMERIC);
            c1.setCellValue(getCOMMRIVA);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 30, NUMERIC);
            c1.setCellValue(getSPREADVEND);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 31, NUMERIC);
            c1.setCellValue(getTOTVOLVEN);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 32, NUMERIC);
            c1.setCellValue(getTOTTRANSVEN);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 33, NUMERIC);
            c1.setCellValue(getTOTGMVEN);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 34, NUMERIC);
            c1.setCellValue(divisione_controllozero(getPERCVEN, output.size()));
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 35, NUMERIC);
            c1.setCellValue(getTOTVOL);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 36, NUMERIC);
            c1.setCellValue(getTOTTRANS);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 37, NUMERIC);
            c1.setCellValue(getTOTGM);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 38, NUMERIC);
            c1.setCellValue(divisione_controllozero(getPERCVEND, output.size()));
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 39, NUMERIC);
            c1.setCellValue(getCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 40, NUMERIC);
            c1.setCellValue(getTOBANKCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 41, NUMERIC);
            c1.setCellValue(getFRBANKCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 42, NUMERIC);
            c1.setCellValue(getTOBRCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 43, NUMERIC);
            c1.setCellValue(getFRBRCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 44, NUMERIC);
            c1.setCellValue(getOCERRCOP);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 45, NUMERIC);
            c1.setCellValue(getFX);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 46, NUMERIC);
            c1.setCellValue(getTOBANKFX);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 47, NUMERIC);
            c1.setCellValue(getFRBANKFX);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 48, NUMERIC);
            c1.setCellValue(getTOBRFX);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 49, NUMERIC);
            c1.setCellValue(getFRBRFX);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 50, NUMERIC);
            c1.setCellValue(getOCERRFX);
            c1.setCellStyle(cellStylenumtot);

            for (int r = 0; r < 51; r++) {
                sheet.autoSizeColumn(r);
            }

            try {
                try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
                    wb.write(fileOut);
                }
                is.close();
                wb.close();

                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
                return base64;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static String create_changeacc1OLD(File outputfile, ArrayList<DailyChange> output, DatabaseCons db) {

        try (InputStream is = new ByteArrayInputStream(Base64.decodeBase64(db.getConf("path.rep1.2023")));
                XSSFWorkbook wb = new XSSFWorkbook(is);
                FileOutputStream fileOut = new FileOutputStream(outputfile)) {
            String formatdataCell = "#,#.00";
            Sheet sheet = wb.getSheetAt(0);

            XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
            XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
            cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenum.setBorderBottom(BorderStyle.THIN);
            cellStylenum.setBorderTop(BorderStyle.THIN);
            cellStylenum.setBorderRight(BorderStyle.THIN);
            cellStylenum.setBorderLeft(BorderStyle.THIN);

            XSSFCellStyle cellStyleint = (XSSFCellStyle) wb.createCellStyle();
            cellStyleint.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleint.setBorderBottom(BorderStyle.THIN);
            cellStyleint.setBorderTop(BorderStyle.THIN);
            cellStyleint.setBorderRight(BorderStyle.THIN);
            cellStyleint.setBorderLeft(BorderStyle.THIN);

            double getVOLUMEAC = 0.00;
            double getVOLUMECA = 0.00;
            double getVOLUMETC = 0.00;
            double getTRANSAC = 0.00;
            double getTRANSCA = 0.00;
            double getTRANSTC = 0.00;
            double getCOMMAC = 0.00;
            double getCOMMCA = 0.00;
            double getCOMMTC = 0.00;
            double getSPREADBR = 0.00;
            double getSPREADBA = 0.00;
            int getTOTTRANSACQ = 0;
            double getTOTVOLACQ = 0.00;
            double getTOTGMACQ = 0.00;
            double getPERCACQ = 0.00;
            double getVOLUMEVENDOFF = 0.00;
            double getVOLUMEONL = 0.00;
            double getVOLUMERIVA = 0.00;

            int getTRANSVENDOFF = 0;
            int getTRANSONL = 0;
            int getTRANSRIVA = 0;

            double getCOMMVENDOFF = 0.00;
            double getCOMMONL = 0.00;
            double getCOMMRIVA = 0.00;
            double getSPREADVEND = 0.00;
            double getTOTVOLVEN = 0.00;

            int getTOTTRANSVEN = 0;

            double getTOTGMVEN = 0.00;
            double getPERCVEN = 0.00;
            double getTOTVOL = 0.00;

            int getTOTTRANS = 0;

            double getTOTGM = 0.0;
            double getPERCVEND = 0.0;
            double getCOP = 0.0;
            double getTOBANKCOP = 0.0;
            double getFRBANKCOP = 0.0;
            double getTOBRCOP = 0.0;
            double getFRBRCOP = 0.0;
            double getOCERRCOP = 0.0;
            double getFX = 0.0;

            double getTOBANKFX = 0.0;
            double getFRBANKFX = 0.0;
            double getTOBRFX = 0.0;
            double getFRBRFX = 0.0;
            double getOCERRFX = 0.0;

            int st = 4;
            int lastindex = 0;
            for (int i = 0; i < output.size(); i++) {
                DailyChange dc = output.get(i);
                lastindex = st + i;
                Row row = getRow(sheet, lastindex);
                Cell c1 = getCell(row, 1);
                c1.setCellValue(dc.getFiliale());

                c1 = getCell(row, 2);
                c1.setCellValue(dc.getDescr());

                c1 = getCell(row, 3);
                c1.setCellValue(dc.getData());
                c1 = getCell(row, 4);
                c1.setCellValue(fd(dc.getVOLUMEAC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 5);
                c1.setCellValue(fd(dc.getVOLUMECA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 6);
                c1.setCellValue(fd(dc.getVOLUMETC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 7);
                c1.setCellValue(parseIntR(dc.getTRANSAC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 8);
                c1.setCellValue(parseIntR(dc.getTRANSCA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 9);
                c1.setCellValue(parseIntR(dc.getTRANSTC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 10);
                c1.setCellValue(fd(dc.getCOMMAC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 11);
                c1.setCellValue(fd(dc.getCOMMCA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 12);
                c1.setCellValue(fd(dc.getCOMMTC()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 13);
                c1.setCellValue(fd(dc.getSPREADBR()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 14);
                c1.setCellValue(fd(dc.getSPREADBA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 15);
                c1.setCellValue(parseIntR(dc.getTOTTRANSACQ()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);

                c1 = getCell(row, 16);
                c1.setCellValue(fd(dc.getTOTVOLACQ()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 17);
                c1.setCellValue(fd(dc.getTOTGMACQ()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 18);
                c1.setCellValue(fd(dc.getPERCACQ()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 19);
                c1.setCellValue(fd(dc.getVOLUMEVENDOFF()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 20);
                c1.setCellValue(fd(dc.getVOLUMEONL()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 21);
                c1.setCellValue(fd(dc.getVOLUMERIVA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 22);
                c1.setCellValue(parseIntR(dc.getTRANSVENDOFF()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 23);
                c1.setCellValue(parseIntR(dc.getTRANSONL()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 24);
                c1.setCellValue(parseIntR(dc.getTRANSRIVA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 25);
                c1.setCellValue(fd(dc.getCOMMVENDOFF()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 26);
                c1.setCellValue(fd(dc.getCOMMONL()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 27);
                c1.setCellValue(fd(dc.getCOMMRIVA()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 28);
                c1.setCellValue(fd(dc.getSPREADVEND()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 29);
                c1.setCellValue(fd(dc.getTOTVOLVEN()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 30);
                c1.setCellValue(parseIntR(dc.getTOTTRANSVEN()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 31);
                c1.setCellValue(fd(dc.getTOTGMVEN()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 32);
                c1.setCellValue(fd(dc.getPERCVEN()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 33);
                c1.setCellValue(fd(dc.getTOTVOL()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 34);
                c1.setCellValue(parseIntR(dc.getTOTTRANS()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStyleint);
                c1 = getCell(row, 35);
                c1.setCellValue(fd(dc.getTOTGM()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 36);
                c1.setCellValue(fd(dc.getPERCVEND()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 37);
                c1.setCellValue(fd(dc.getCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 38);
                c1.setCellValue(fd(dc.getTOBANKCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 39);
                c1.setCellValue(fd(dc.getFRBANKCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 40);
                c1.setCellValue(fd(dc.getTOBRCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 41);
                c1.setCellValue(fd(dc.getFRBRCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 42);
                c1.setCellValue(fd(dc.getOCERRCOP()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                c1 = getCell(row, 43);
                c1.setCellValue(fd(dc.getFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 44);
                c1.setCellValue(fd(dc.getTOBANKFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 45);
                c1.setCellValue(fd(dc.getFRBANKFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 46);
                c1.setCellValue(fd(dc.getTOBRFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 47);
                c1.setCellValue(fd(dc.getFRBRFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);
                c1 = getCell(row, 48);
                c1.setCellValue(fd(dc.getOCERRFX()));
                c1.setCellType(CellType.NUMERIC);
                c1.setCellStyle(cellStylenum);

                getVOLUMEAC += fd(dc.getVOLUMEAC());
                getVOLUMECA += fd(dc.getVOLUMECA());
                getVOLUMETC += fd(dc.getVOLUMETC());
                getTRANSAC += parseIntR(dc.getTRANSAC());
                getTRANSCA += parseIntR(dc.getTRANSCA());
                getTRANSTC += parseIntR(dc.getTRANSTC());
                getCOMMAC += fd(dc.getCOMMAC());
                getCOMMCA += fd(dc.getCOMMCA());
                getCOMMTC += fd(dc.getCOMMTC());
                getSPREADBR += fd(dc.getSPREADBR());
                getSPREADBA += fd(dc.getSPREADBA());
                getTOTTRANSACQ += parseIntR(dc.getTOTTRANSACQ());
                getTOTVOLACQ += fd(dc.getTOTVOLACQ());
                getTOTGMACQ += fd(dc.getTOTGMACQ());
                getPERCACQ += fd(dc.getPERCACQ());
                getVOLUMEVENDOFF += fd(dc.getVOLUMEVENDOFF());
                getVOLUMEONL += fd(dc.getVOLUMEONL());
                getVOLUMERIVA += fd(dc.getVOLUMERIVA());
                getTRANSVENDOFF += parseIntR(dc.getTRANSVENDOFF());
                getTRANSONL += parseIntR(dc.getTRANSONL());
                getTRANSRIVA += parseIntR(dc.getTRANSRIVA());
                getCOMMVENDOFF += fd(dc.getCOMMVENDOFF());
                getCOMMONL += fd(dc.getCOMMONL());
                getCOMMRIVA += fd(dc.getCOMMRIVA());
                getSPREADVEND += fd(dc.getSPREADVEND());
                getTOTVOLVEN += fd(dc.getTOTVOLVEN());
                getTOTTRANSVEN += parseIntR(dc.getTOTTRANSVEN());
                getTOTGMVEN += fd(dc.getTOTGMVEN());
                getPERCVEN += fd(dc.getPERCVEN());
                getTOTVOL += fd(dc.getTOTVOL());
                getTOTTRANS += parseIntR(dc.getTOTTRANS());
                getTOTGM += fd(dc.getTOTGM());
                getPERCVEND += fd(dc.getPERCVEND());
                getCOP += fd(dc.getCOP());
                getTOBANKCOP += fd(dc.getTOBANKCOP());
                getFRBANKCOP += fd(dc.getFRBANKCOP());
                getTOBRCOP += fd(dc.getTOBRCOP());
                getFRBRCOP += fd(dc.getFRBRCOP());
                getOCERRCOP += fd(dc.getOCERRCOP());
                getFX += fd(dc.getFX());
                getTOBANKFX += fd(dc.getTOBANKFX());
                getFRBANKFX += fd(dc.getFRBANKFX());
                getTOBRFX += fd(dc.getTOBRFX());
                getFRBRFX += fd(dc.getFRBRFX());
                getOCERRFX += fd(dc.getOCERRFX());

            }

            XSSFFont font = (XSSFFont) wb.createFont();
            font.setFontName(HSSFFont.FONT_ARIAL);
            font.setFontHeightInPoints((short) 11);
            font.setBold(true);

            XSSFCellStyle cellStyleblanktot = (XSSFCellStyle) wb.createCellStyle();
            cellStyleblanktot.setBorderRight(BorderStyle.THIN);
            cellStyleblanktot.setBorderBottom(BorderStyle.THICK);
            cellStyleblanktot.setBorderTop(BorderStyle.THICK);
            cellStyleblanktot.setBorderLeft(BorderStyle.THIN);
            cellStyleblanktot.setFont(font);

            XSSFCellStyle cellStylenumtot = (XSSFCellStyle) wb.createCellStyle();
            cellStylenumtot.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
            cellStylenumtot.setBorderRight(BorderStyle.THIN);
            cellStylenumtot.setBorderBottom(BorderStyle.THICK);
            cellStylenumtot.setBorderTop(BorderStyle.THICK);
            cellStylenumtot.setBorderLeft(BorderStyle.THIN);
            cellStylenumtot.setFont(font);

            XSSFCellStyle cellStyleinttot = (XSSFCellStyle) wb.createCellStyle();
            cellStyleinttot.setDataFormat(hssfDataFormat.getFormat(formatdataCelINT));
            cellStyleinttot.setBorderRight(BorderStyle.THIN);
            cellStyleinttot.setBorderBottom(BorderStyle.THICK);
            cellStyleinttot.setBorderTop(BorderStyle.THICK);
            cellStyleinttot.setBorderLeft(BorderStyle.THIN);
            cellStyleinttot.setFont(font);

            Row row = getRow(sheet, lastindex + 3);
            Cell c1 = getCell(row, 1);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("");
            c1 = getCell(row, 2);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("TOTALE");
            c1 = getCell(row, 3);
            c1.setCellStyle(cellStyleblanktot);
            c1.setCellValue("");
            c1 = getCell(row, 4);
            c1.setCellValue(getVOLUMEAC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 5);
            c1.setCellValue(getVOLUMECA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 6);
            c1.setCellValue(getVOLUMETC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 7);
            c1.setCellValue(getTRANSAC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 8);
            c1.setCellValue(getTRANSCA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 9);
            c1.setCellValue(getTRANSTC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 10);
            c1.setCellValue(getCOMMAC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 11);
            c1.setCellValue(getCOMMCA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 12);
            c1.setCellValue(getCOMMTC);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 13);
            c1.setCellValue(getSPREADBR);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 14);
            c1.setCellValue(getSPREADBA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);

            c1 = getCell(row, 15);
            c1.setCellValue(getTOTTRANSACQ);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);

            c1 = getCell(row, 16);
            c1.setCellValue(getTOTVOLACQ);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 17);
            c1.setCellValue(getTOTGMACQ);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 18);
            c1.setCellValue(divisione_controllozero(getPERCACQ, output.size()));
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 19);
            c1.setCellValue(getVOLUMEVENDOFF);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 20);
            c1.setCellValue(getVOLUMEONL);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 21);
            c1.setCellValue(getVOLUMERIVA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 22);
            c1.setCellValue(getTRANSVENDOFF);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 23);
            c1.setCellValue(getTRANSONL);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 24);
            c1.setCellValue(getTRANSRIVA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 25);
            c1.setCellValue(getCOMMVENDOFF);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 26);
            c1.setCellValue(getCOMMONL);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 27);
            c1.setCellValue(getCOMMRIVA);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 28);
            c1.setCellValue(getSPREADVEND);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 29);
            c1.setCellValue(getTOTVOLVEN);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 30);
            c1.setCellValue(getTOTTRANSVEN);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 31);
            c1.setCellValue(getTOTGMVEN);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 32);
            c1.setCellValue(divisione_controllozero(getPERCVEN, output.size()));
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 33);
            c1.setCellValue(getTOTVOL);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 34);
            c1.setCellValue(getTOTTRANS);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStyleinttot);
            c1 = getCell(row, 35);
            c1.setCellValue(getTOTGM);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 36);
            c1.setCellValue(divisione_controllozero(getPERCVEND, output.size()));

            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 37);
            c1.setCellValue(getCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 38);
            c1.setCellValue(getTOBANKCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 39);
            c1.setCellValue(getFRBANKCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 40);
            c1.setCellValue(getTOBRCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 41);
            c1.setCellValue(getFRBRCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 42);
            c1.setCellValue(getOCERRCOP);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 43);
            c1.setCellValue(getFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 44);
            c1.setCellValue(getTOBANKFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 45);
            c1.setCellValue(getFRBANKFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 46);
            c1.setCellValue(getTOBRFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 47);
            c1.setCellValue(getFRBRFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);
            c1 = getCell(row, 48);
            c1.setCellValue(getOCERRFX);
            c1.setCellType(CellType.NUMERIC);
            c1.setCellStyle(cellStylenumtot);

            for (int r = 0; r < 49; r++) {
                sheet.autoSizeColumn(r);
            }

            wb.write(fileOut);
            fileOut.close();
            is.close();
            wb.close();
            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;

        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static String C_OpenCloseError(File outputfile, ArrayList<Openclose_Error_value> osplist,
            String data1, String data2, String localcurrency, boolean cashier,
            List<Openclose_Error_value> completecashier) {

        String formatdataCellint = "#,#";
        String formatdataCell = "#,#.00";
        String formatdataCellRate = "#,#.00000000";

        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("CASHIER OPENCLOSE ERRORS");
        // CREAZIONE FONT
        XSSFFont font = workbook.createFont();
        font.setFontName(HSSFFont.FONT_ARIAL);
        font.setFontHeightInPoints((short) 12);
        font.setBold(true);
        XSSFCellStyle style1 = workbook.createCellStyle();
        style1.setFont(font);
        XSSFFont font2 = workbook.createFont();
        font2.setFontName(HSSFFont.FONT_ARIAL);
        font2.setFontHeightInPoints((short) 12);
        XSSFCellStyle style2 = workbook.createCellStyle();
        style2.setFont(font2);
        XSSFFont font3 = workbook.createFont();
        font3.setFontName(HSSFFont.FONT_ARIAL);
        font3.setFontHeightInPoints((short) 10);
        font3.setBold(true);
        XSSFCellStyle style3 = workbook.createCellStyle();
        style3.setFont(font3);
        style3.setAlignment(HorizontalAlignment.CENTER);
        style3.setBorderTop(BorderStyle.THIN);
        style3.setBorderLeft(BorderStyle.THIN);
        style3.setBorderRight(BorderStyle.THIN);
        style3.setBorderBottom(BorderStyle.THIN);
        XSSFCellStyle style3left = workbook.createCellStyle();
        style3left.setFont(font3);
        style3left.setAlignment(HorizontalAlignment.LEFT);
        style3left.setBorderTop(BorderStyle.THIN);
        style3left.setBorderBottom(BorderStyle.THIN);
        XSSFFont font4 = workbook.createFont();
        font4.setFontName(HSSFFont.FONT_ARIAL);
        font4.setFontHeightInPoints((short) 10);
        XSSFCellStyle style4 = workbook.createCellStyle();
        style4.setAlignment(HorizontalAlignment.RIGHT);
        style4.setBorderTop(BorderStyle.THIN);
        style4.setBorderLeft(BorderStyle.THIN);
        style4.setBorderRight(BorderStyle.THIN);
        style4.setBorderBottom(BorderStyle.THIN);
        XSSFDataFormat hssfDataFormat = workbook.createDataFormat();
        XSSFCellStyle cellStylenumint = workbook.createCellStyle();
        cellStylenumint.setDataFormat(hssfDataFormat.getFormat(formatdataCellint));
        cellStylenumint.setAlignment(HorizontalAlignment.RIGHT);
        cellStylenumint.setBorderTop(BorderStyle.THIN);
        cellStylenumint.setBorderBottom(BorderStyle.THIN);
        XSSFCellStyle style4left = workbook.createCellStyle();
        style4left.setAlignment(HorizontalAlignment.LEFT);
        style4left.setBorderTop(BorderStyle.THIN);
        style4left.setBorderLeft(BorderStyle.THIN);
        style4left.setBorderRight(BorderStyle.THIN);
        style4left.setBorderBottom(BorderStyle.THIN);
        XSSFCellStyle style4CE = workbook.createCellStyle();
        style4CE.setAlignment(HorizontalAlignment.CENTER);
        style4CE.setBorderTop(BorderStyle.THIN);
        style4CE.setBorderLeft(BorderStyle.THIN);
        style4CE.setBorderRight(BorderStyle.THIN);
        style4CE.setBorderBottom(BorderStyle.THIN);
        XSSFCellStyle cellStylenum = workbook.createCellStyle();
        cellStylenum.setAlignment(HorizontalAlignment.RIGHT);
        cellStylenum.setBorderTop(BorderStyle.THIN);
        cellStylenum.setBorderLeft(BorderStyle.THIN);
        cellStylenum.setBorderRight(BorderStyle.THIN);
        cellStylenum.setBorderBottom(BorderStyle.THIN);
        cellStylenum.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
        XSSFCellStyle style3num = workbook.createCellStyle();
        style3num.setFont(font3);
        style3num.setAlignment(HorizontalAlignment.RIGHT);
        style3num.setBorderTop(BorderStyle.THIN);
        style3num.setBorderBottom(BorderStyle.THIN);
        style3num.setDataFormat(hssfDataFormat.getFormat(formatdataCell));
        XSSFCellStyle cellStylenumRATE = workbook.createCellStyle();
        XSSFDataFormat hssfDataFormatRATE = workbook.createDataFormat();
        cellStylenumRATE.setAlignment(HorizontalAlignment.RIGHT);
        cellStylenumRATE.setBorderTop(BorderStyle.THIN);
        cellStylenumRATE.setBorderLeft(BorderStyle.THIN);
        cellStylenumRATE.setBorderRight(BorderStyle.THIN);
        cellStylenumRATE.setBorderBottom(BorderStyle.THIN);
        cellStylenumRATE.setDataFormat(hssfDataFormatRATE.getFormat(formatdataCellRate));
        XSSFRow rowP = sheet.createRow(1);
        XSSFCell cl = rowP.createCell(1);
        cl.setCellStyle(style1);
        cl.setCellValue("CASHIER OPENCLOSE ERRORS " + " FROM " + data1 + " TO " + data2);
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 1, 7));
        AtomicInteger start = new AtomicInteger(0);
        if (cashier) {

            Comparator<Openclose_Error_value> compareByUser = (Openclose_Error_value o1, Openclose_Error_value o2) -> o1
                    .getUser().compareTo(o2.getUser());
            Collections.sort(completecashier, compareByUser);

            AtomicInteger indice = new AtomicInteger(start.get() + 6);
            XSSFRow row = sheet.createRow(indice.get());

            Cell cl5 = row.createCell(1);
            cl5.setCellStyle(style3);
            cl5.setCellValue("USER");

            cl5 = row.createCell(2);
            cl5.setCellStyle(style3);
            cl5.setCellValue("DATE");

            cl5 = row.createCell(3);
            cl5.setCellStyle(style3);
            cl5.setCellValue("BRANCH");

            cl5 = row.createCell(4);
            cl5.setCellStyle(style3);
            cl5.setCellValue("KIND");

            cl5 = row.createCell(5);
            cl5.setCellStyle(style3);
            cl5.setCellValue("CURRENCY/CATEGORY");

            cl5 = row.createCell(6);
            cl5.setCellStyle(style3);
            cl5.setCellValue("DIFFERENCE (" + localcurrency + ")");

            cl5 = row.createCell(7);
            cl5.setCellStyle(style3);
            cl5.setCellValue("NOTE");

            cl5 = row.createCell(8);
            cl5.setCellStyle(style3);
            cl5.setCellValue("QUANTITY USER");

            cl5 = row.createCell(9);
            cl5.setCellStyle(style3);
            cl5.setCellValue("AMOUNT USER");

            cl5 = row.createCell(10);
            cl5.setCellStyle(style3);
            cl5.setCellValue("QUANTITY SYSTEM");

            cl5 = row.createCell(11);
            cl5.setCellStyle(style3);
            cl5.setCellValue("AMOUNT SYSTEM");

            cl5 = row.createCell(12);
            cl5.setCellStyle(style3);
            cl5.setCellValue("QUANTITY DIFFERENCE");

            cl5 = row.createCell(13);
            cl5.setCellStyle(style3);
            cl5.setCellValue("AMOUNT DIFFERENCE");

            cl5 = row.createCell(14);
            cl5.setCellStyle(style3);
            cl5.setCellValue("RATE / PRICE");

            cl5 = row.createCell(15);
            cl5.setCellStyle(style3);
            cl5.setCellValue("SAFE / TILL");

            cl5 = row.createCell(16);
            cl5.setCellStyle(style3);
            cl5.setCellValue("TYPE");

            cl5 = row.createCell(17);
            cl5.setCellStyle(style3);
            cl5.setCellValue("OPERATION");

            cl5 = row.createCell(18);
            cl5.setCellStyle(style3);
            cl5.setCellValue("POS / BANK ACCOUNT");

            completecashier.forEach(d2 -> {
                indice.addAndGet(1);
                XSSFRow row1 = sheet.createRow(indice.get());

                double diff = fd(d2.getQuantityUser()) - fd(d2.getQuantitySystem());

                XSSFCell f1 = row1.createCell(1);
                f1.setCellStyle(style4left);
                f1.setCellValue(d2.getUser());

                f1 = row1.createCell(2);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getData());

                f1 = row1.createCell(3);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getId_filiale() + " " + d2.getDe_filiale());

                f1 = row1.createCell(4);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getKind());

                f1 = row1.createCell(5);
                f1.setCellStyle(style4left);
                f1.setCellValue(d2.getCurrency());

                f1 = row1.createCell(6);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getDiffAmount()));

                f1 = row1.createCell(7);
                f1.setCellStyle(style4left);
                f1.setCellValue(d2.getNote());

                f1 = row1.createCell(8);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getQuantityUser()));

                f1 = row1.createCell(9);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getAmountuser()));

                f1 = row1.createCell(10);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getQuantitySystem()));

                f1 = row1.createCell(11);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getAmountsystem()));

                f1 = row1.createCell(12);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getQuantitydiff()));

                f1 = row1.createCell(13);
                f1.setCellStyle(cellStylenum);
                f1.setCellType(CellType.NUMERIC);
                f1.setCellValue(fd(d2.getDiffAmount()));

                f1 = row1.createCell(14);
                f1.setCellStyle(cellStylenumRATE);
                f1.setCellType(CellType.NUMERIC);
                if (d2.getCurrency().equals(localcurrency)) {
                    f1.setCellValue(fd("1.00000000"));
                } else {
                    f1.setCellValue(fd(d2.getRate()));
                }

                f1 = row1.createCell(15);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getTill());

                f1 = row1.createCell(16);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getTipo());

                f1 = row1.createCell(17);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getOperazione());

                f1 = row1.createCell(18);
                f1.setCellStyle(style4);
                f1.setCellValue(d2.getPos());

                if (d2.getType().equals("CH")) {

                    f1 = row1.createCell(8);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");

                    f1 = row1.createCell(9);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getQuantityUser()));

                    f1 = row1.createCell(10);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");

                    f1 = row1.createCell(11);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getQuantitySystem()));

                    f1 = row1.createCell(12);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");

                    f1 = row1.createCell(13);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(roundDoubleandFormat(diff, 2)));

                    f1 = row1.createCell(18);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");
                } else if (d2.getType().equals("NC")) {

                    f1 = row1.createCell(4);
                    f1.setCellStyle(style4);
                    f1.setCellValue("NC");

                    f1 = row1.createCell(5);
                    f1.setCellStyle(style4left);
                    f1.setCellValue(d2.getNc());

                    f1 = row1.createCell(6);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getLocalamount()));

                    f1 = row1.createCell(9);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");

                    f1 = row1.createCell(11);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");

                    f1 = row1.createCell(12);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getDiffAmount()));

                    f1 = row1.createCell(13);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getLocalamount()));

                    f1 = row1.createCell(14);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getNcprice()));

                    f1 = row1.createCell(18);
                    f1.setCellStyle(style4CE);
                    f1.setCellValue("-");
                } else if (d2.getType().equals("PO") && d2.getCurrency().equals(localcurrency)) {

                } else {

                    f1 = row1.createCell(6);
                    f1.setCellStyle(cellStylenum);
                    f1.setCellType(CellType.NUMERIC);
                    f1.setCellValue(fd(d2.getDiffContr()));
                }
                start.set(indice.get());
            });

        } else {
            for (int h = 0; h < osplist.size(); h++) {

                sheet.addMergedRegion(new CellRangeAddress(start.get() + 3, start.get() + 3, 1, 7));

                Openclose_Error_value osp = osplist.get(h);

                XSSFRow row = sheet.createRow(start.get() + 3);
                XSSFCell c2 = row.createCell(1);
                c2.setCellStyle(style1);
                c2.setCellValue(osp.getId_filiale() + " " + osp.getDe_filiale());

                ArrayList<Openclose_Error_value_stock> dati = osp.getDati();
                AtomicInteger indice = new AtomicInteger(start.get() + 6);
                row = sheet.createRow(indice.get());

                Cell cl5 = row.createCell(1);
                cl5.setCellStyle(style3);
                cl5.setCellValue("USER");

                cl5 = row.createCell(2);
                cl5.setCellStyle(style3);
                cl5.setCellValue("DATE");

                cl5 = row.createCell(3);
                cl5.setCellStyle(style3);
                cl5.setCellValue("BRANCH");

                cl5 = row.createCell(4);
                cl5.setCellStyle(style3);
                cl5.setCellValue("KIND");

                cl5 = row.createCell(5);
                cl5.setCellStyle(style3);
                cl5.setCellValue("CURRENCY/CATEGORY");

                cl5 = row.createCell(6);
                cl5.setCellStyle(style3);
                cl5.setCellValue("DIFFERENCE (" + localcurrency + ")");

                cl5 = row.createCell(7);
                cl5.setCellStyle(style3);
                cl5.setCellValue("NOTE");

                cl5 = row.createCell(8);
                cl5.setCellStyle(style3);
                cl5.setCellValue("QUANTITY USER");

                cl5 = row.createCell(9);
                cl5.setCellStyle(style3);
                cl5.setCellValue("AMOUNT USER");

                cl5 = row.createCell(10);
                cl5.setCellStyle(style3);
                cl5.setCellValue("QUANTITY SYSTEM");

                cl5 = row.createCell(11);
                cl5.setCellStyle(style3);
                cl5.setCellValue("AMOUNT SYSTEM");

                cl5 = row.createCell(12);
                cl5.setCellStyle(style3);
                cl5.setCellValue("QUANTITY DIFFERENCE");

                cl5 = row.createCell(13);
                cl5.setCellStyle(style3);
                cl5.setCellValue("AMOUNT DIFFERENCE");

                cl5 = row.createCell(14);
                cl5.setCellStyle(style3);
                cl5.setCellValue("RATE / PRICE");

                cl5 = row.createCell(15);
                cl5.setCellStyle(style3);
                cl5.setCellValue("SAFE / TILL");

                cl5 = row.createCell(16);
                cl5.setCellStyle(style3);
                cl5.setCellValue("TYPE");

                cl5 = row.createCell(17);
                cl5.setCellStyle(style3);
                cl5.setCellValue("OPERATION");

                cl5 = row.createCell(18);
                cl5.setCellStyle(style3);
                cl5.setCellValue("POS / BANK ACCOUNT");

                dati.forEach(d1 -> {

                    List<Openclose_Error_value> content = d1.getDati();

                    content.forEach(d2 -> {
                        indice.addAndGet(1);
                        XSSFRow row1 = sheet.createRow(indice.get());

                        double diff = fd(d2.getQuantityUser()) - fd(d2.getQuantitySystem());

                        XSSFCell f1 = row1.createCell(1);
                        f1.setCellStyle(style4left);
                        f1.setCellValue(d2.getUser());

                        f1 = row1.createCell(2);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d2.getData());

                        f1 = row1.createCell(3);
                        f1.setCellStyle(style4);
                        f1.setCellValue(osp.getId_filiale() + " " + osp.getDe_filiale());

                        f1 = row1.createCell(4);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d2.getKind());

                        f1 = row1.createCell(5);
                        f1.setCellStyle(style4left);
                        f1.setCellValue(d2.getCurrency());

                        f1 = row1.createCell(6);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getDiffAmount()));

                        f1 = row1.createCell(7);
                        f1.setCellStyle(style4left);
                        f1.setCellValue(d2.getNote());

                        f1 = row1.createCell(8);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getQuantityUser()));

                        f1 = row1.createCell(9);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getAmountuser()));

                        f1 = row1.createCell(10);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getQuantitySystem()));

                        f1 = row1.createCell(11);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getAmountsystem()));

                        f1 = row1.createCell(12);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getQuantitydiff()));

                        f1 = row1.createCell(13);
                        f1.setCellStyle(cellStylenum);
                        f1.setCellType(CellType.NUMERIC);
                        f1.setCellValue(fd(d2.getDiffAmount()));

                        f1 = row1.createCell(14);
                        f1.setCellStyle(cellStylenumRATE);
                        f1.setCellType(CellType.NUMERIC);
                        if (d2.getCurrency().equals(localcurrency)) {
                            f1.setCellValue(fd("1.00000000"));
                        } else {
                            f1.setCellValue(fd(d2.getRate()));
                        }

                        f1 = row1.createCell(15);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d2.getTill());

                        f1 = row1.createCell(16);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d2.getTipo());

                        f1 = row1.createCell(17);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d1.getOperazione());

                        f1 = row1.createCell(18);
                        f1.setCellStyle(style4);
                        f1.setCellValue(d2.getPos());

                        if (d2.getType().equals("CH")) {

                            f1 = row1.createCell(8);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");

                            f1 = row1.createCell(9);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getQuantityUser()));

                            f1 = row1.createCell(10);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");

                            f1 = row1.createCell(11);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getQuantitySystem()));

                            f1 = row1.createCell(12);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");

                            f1 = row1.createCell(13);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(roundDoubleandFormat(diff, 2)));

                            f1 = row1.createCell(18);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");
                        } else if (d2.getType().equals("NC")) {

                            f1 = row1.createCell(4);
                            f1.setCellStyle(style4);
                            f1.setCellValue("NC");

                            f1 = row1.createCell(5);
                            f1.setCellStyle(style4left);
                            f1.setCellValue(d2.getNc());

                            f1 = row1.createCell(6);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getLocalamount()));

                            f1 = row1.createCell(9);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");

                            f1 = row1.createCell(11);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");

                            f1 = row1.createCell(12);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getDiffAmount()));

                            f1 = row1.createCell(13);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getLocalamount()));

                            f1 = row1.createCell(14);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getNcprice()));

                            f1 = row1.createCell(18);
                            f1.setCellStyle(style4CE);
                            f1.setCellValue("-");
                        } else if (d2.getType().equals("PO") && d2.getCurrency().equals(localcurrency)) {

                        } else {

                            f1 = row1.createCell(6);
                            f1.setCellStyle(cellStylenum);
                            f1.setCellType(CellType.NUMERIC);
                            f1.setCellValue(fd(d2.getDiffContr()));
                        }

                        start.set(indice.get());

                    });
                });

                start.addAndGet(5);
            }
        }
        for (int i = 1; i < 19; i++) {
            sheet.autoSizeColumn(i);
        }
        try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
            workbook.write(fileOut);
            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(outputfile)));
            return base64;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
