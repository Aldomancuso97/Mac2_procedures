///////*
////// * To change this license header, choose License Headers in Project Properties.
////// * To change this template file, choose Tools | Templates
////// * and open the template in the editor.
////// */
//////package rc.soop.testarea;
//////
//////import rc.soop.aggiornamenti.Mactest;
//////import rc.soop.aggiornamenti.Utility;
//////import rc.soop.aggiornamenti.Db;
//////import com.jcraft.jsch.Channel;
//////import com.jcraft.jsch.ChannelSftp;
//////import com.jcraft.jsch.JSch;
//////import com.jcraft.jsch.JSchException;
//////import com.jcraft.jsch.Session;
//////import java.io.BufferedOutputStream;
//////import java.io.File;
//////import java.io.FileOutputStream;
//////import java.io.OutputStream;
//////import java.sql.ResultSet;
//////import java.util.Properties;
//////import org.apache.commons.lang3.StringUtils;
//////import org.apache.poi.hssf.usermodel.HSSFFont;
//////import org.apache.poi.ss.usermodel.BorderStyle;
//////import org.apache.poi.ss.usermodel.Cell;
//////import org.apache.poi.ss.usermodel.CellType;
//////import org.apache.poi.ss.usermodel.HorizontalAlignment;
//////import org.apache.poi.ss.usermodel.Row;
//////import org.apache.poi.ss.usermodel.Sheet;
//////import org.apache.poi.ss.usermodel.Workbook;
//////import org.apache.poi.ss.util.CellRangeAddress;
//////import org.apache.poi.xssf.usermodel.XSSFCellStyle;
//////import org.apache.poi.xssf.usermodel.XSSFDataFormat;
//////import org.apache.poi.xssf.usermodel.XSSFFont;
//////import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//////
///////**
////// *
////// * @author rcosco
////// */
//////public class ExportDataCLiente {
//////
//////    public static double fd(String si_t_old) {
//////        if (si_t_old == null) {
//////            return 0.0D;
//////        }
//////        double d1;
//////        si_t_old = si_t_old.replace(",", "").trim();
//////        try {
//////            d1 = Double.parseDouble(si_t_old);
//////        } catch (NumberFormatException e) {
//////            d1 = 0.0D;
//////        }
//////        return d1;
//////    }
//////
//////    public static String formatType(String tipotr) {
//////        if (tipotr != null) {
//////            if (tipotr.equals("B")) {
//////                return "Buy";
//////            }
//////            if (tipotr.equals("S")) {
//////                return "Sell";
//////            }
//////        }
//////        return "-";
//////    }
//////
//////    public static void main(String[] args) {
//////
//////        File outputfile = new File("C:\\mnt\\mac\\salvati.xlsx");
//////
//////        Workbook wb = new XSSFWorkbook();
//////        Sheet sheet = wb.createSheet("Transaction_list_E");
//////        XSSFCellStyle style = (XSSFCellStyle) wb.createCellStyle();
//////        style.setBorderBottom(BorderStyle.THIN);
//////        style.setBorderTop(BorderStyle.THIN);
//////        style.setBorderRight(BorderStyle.THIN);
//////        style.setBorderLeft(BorderStyle.THIN);
//////        XSSFCellStyle style1 = (XSSFCellStyle) wb.createCellStyle();
//////        style1.setBorderBottom(BorderStyle.THIN);
//////        style1.setBorderTop(BorderStyle.THIN);
//////        style1.setBorderRight(BorderStyle.THIN);
//////        style1.setBorderLeft(BorderStyle.THIN);
//////        style1.setAlignment(HorizontalAlignment.CENTER);
//////        XSSFFont font = (XSSFFont) wb.createFont();
//////        font.setFontName(HSSFFont.FONT_ARIAL);
//////        font.setFontHeightInPoints((short) 12);
//////        font.setBold(true);
//////        //  font.setColor(HSSFColor.BLUE.index);
//////        style1.setFont(font);
//////        XSSFCellStyle style2 = (XSSFCellStyle) wb.createCellStyle();
//////        style2.setAlignment(HorizontalAlignment.CENTER);
//////        XSSFFont font2 = (XSSFFont) wb.createFont();
//////        font2.setFontName(HSSFFont.FONT_ARIAL);
//////        font2.setFontHeightInPoints((short) 14);
//////        font2.setBold(true);
//////        style2.setFont(font2);
//////        Row row0 = sheet.createRow(0);
//////        Cell cell0 = row0.createCell(1);
//////        cell0.setCellStyle(style2);
//////        sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 9));
//////        cell0.setCellValue("Transaction List");
//////
//////        XSSFCellStyle cellStylenum = (XSSFCellStyle) wb.createCellStyle();
//////        XSSFDataFormat hssfDataFormat = (XSSFDataFormat) wb.createDataFormat();
//////        cellStylenum.setDataFormat(hssfDataFormat.getFormat("#,#.00"));
//////        //private static final String formatdataCell = "#.#0,0";
//////        cellStylenum.setBorderBottom(BorderStyle.THIN);
//////        cellStylenum.setBorderTop(BorderStyle.THIN);
//////        cellStylenum.setBorderRight(BorderStyle.THIN);
//////        cellStylenum.setBorderLeft(BorderStyle.THIN);
//////
//////        Row row = sheet.createRow(1);
//////        Cell cell12 = row.createCell(1);
//////        cell12.setCellValue("Branch ID");
//////        cell12.setCellStyle(style1);
//////        Cell cell = row.createCell(2);
//////        cell.setCellValue("Code");
//////        cell.setCellStyle(style1);
//////        Cell cell2 = row.createCell(3);
//////        cell2.setCellValue("Date");
//////        cell2.setCellStyle(style1);
//////        Cell cell4 = row.createCell(4);
//////        cell4.setCellValue("Operator");
//////        cell4.setCellStyle(style1);
//////        Cell cell6 = row.createCell(5);
//////        cell6.setCellValue("Type");
//////        cell6.setCellStyle(style1);
//////        Cell cell7 = row.createCell(6);
//////        cell7.setCellValue("Total");
//////        cell7.setCellStyle(style1);
//////        Cell cell8 = row.createCell(7);
//////        cell8.setCellValue("Net");
//////        cell8.setCellStyle(style1);
//////        Cell cell8a = row.createCell(8);
//////        cell8a.setCellValue("Currency");
//////        cell8a.setCellStyle(style1);
//////        Cell cell9 = row.createCell(9);
//////        cell9.setCellValue("Quantity");
//////        cell9.setCellStyle(style1);
////////        Cell cell10 = row.createCell(10);
////////        cell10.setCellValue("Rate");
////////        cell10.setCellStyle(style1);
//////
//////        Db db = new Db(Mactest.host_PROD, false);
//////
//////        try {
//////            String se_user = "root";
//////            String se_pwd = "Xray666$$!";
//////            String se_ip = "";
//////            int se_port = 22;
//////            ChannelSftp sftpaws = connect(se_user, se_pwd, se_ip, se_port);
//////
//////            String sql = "SELECT c.cod, c.filiale, c.id, c.data,  c.user, c.tipotr, c.total, c.pay, v.valuta, v.quantita, v.rate "
//////                    + "FROM ch_transaction_client c1, ch_transaction c, ch_transaction_valori v "
//////                    + "WHERE c1.codfisc='SLVGTN55S25I982X' AND c1.codtr=c.cod AND c.del_fg='0' AND c.cod=v.cod_tr ORDER BY c.filiale,c.cod";
//////
//////            ResultSet rs = db.getC().createStatement().executeQuery(sql);
//////            int i = 0;
//////            while (rs.next()) {
//////
//////                String codtr = rs.getString(1);
//////
//////                String filiale = rs.getString(2);
//////                String id = rs.getString(3);
//////
//////                ResultSet rs0 = db.getC().createStatement().executeQuery("SELECT tipodoc,content,nomefile FROM ch_transaction_doc c WHERE c.codtr='" + codtr + "'");
//////
//////                while (rs0.next()) {
//////                    String pa1 = StringUtils.replace(rs0.getString(2), "FILE[", "");
//////                    File download = new File("C:\\mnt\\mac\\" + filiale + "_" + id + "_" + rs0.getString(3));
//////                    try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(download))) {
//////                        sftpaws.get(pa1, outputStream);
//////                        outputStream.flush();
//////                    }
//////                }
//////
//////                String data = rs.getString(4);
//////                String user = rs.getString(5);
//////                String tipotr = rs.getString(6);
//////                String total = rs.getString(7);
//////                String pay = rs.getString(8);
//////                String valuta = rs.getString(9);
//////                String quantita = rs.getString(10);
////////                String rate = rs.getString(11);
//////
//////                Row row2 = sheet.createRow(i + 3);
//////                i++;
//////
//////                Cell cell1b = row2.createCell(1);
//////                cell1b.setCellValue(filiale);
//////                cell1b.setCellStyle(style);
//////                Cell cell1 = row2.createCell(2);
//////                cell1.setCellValue(id);
//////                cell1.setCellStyle(style);
//////                Cell cell22 = row2.createCell(3);
//////                cell22.setCellValue(Utility.formatStringtoStringDate(data.split("\\.")[0], "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss"));
//////                cell22.setCellStyle(style);
//////                Cell cell44 = row2.createCell(4);
//////                cell44.setCellValue(user);
//////                cell44.setCellStyle(style);
//////                Cell cell66 = row2.createCell(5);
//////                cell66.setCellValue(formatType(tipotr));
//////                cell66.setCellStyle(style);
//////                Cell cell77 = row2.createCell(6, CellType.NUMERIC);
//////                cell77.setCellValue(fd(total));
//////                cell77.setCellStyle(cellStylenum);
//////                Cell cell88 = row2.createCell(7, CellType.NUMERIC);
//////                cell88.setCellValue(fd(pay));
//////                cell88.setCellStyle(cellStylenum);
//////                Cell cell8v = row2.createCell(8);
//////                cell8v.setCellValue(valuta);
//////                cell8v.setCellStyle(style);
//////                Cell cell9v = row2.createCell(9, CellType.NUMERIC);
//////                cell9v.setCellValue(fd(quantita));
//////                cell9v.setCellStyle(cellStylenum);
//////            }
//////
//////            try (FileOutputStream fileOut = new FileOutputStream(outputfile)) {
//////                wb.write(fileOut);
//////            }
//////            wb.close();
//////
//////            closeConnection(sftpaws);
//////
//////        } catch (Exception e) {
//////            e.printStackTrace();
//////        }
//////
//////        db.closeDB();
//////
//////    }
//////
//////    public static ChannelSftp connect(
//////            String user, String pwd,
//////            String ip, int port
//////    ) {
//////        try {
//////            JSch jsch = new JSch();
//////            Properties config = new Properties();
//////            config.put("StrictHostKeyChecking", "no");
//////            Session session = jsch.getSession(user, ip, port);
//////            session.setPassword(pwd);
//////            session.setConfig(config);
//////            session.connect(30000);
//////            Channel channel = session.openChannel("sftp");
//////            ChannelSftp sftp = (ChannelSftp) channel;
//////            sftp.connect(3000);
//////            return sftp;
//////        } catch (JSchException e) {
//////            e.printStackTrace();
//////        }
//////        return null;
//////    }
//////
//////    public static void closeConnection(ChannelSftp sftp) {
//////        try {
//////            sftp.disconnect();
//////            sftp.getSession().disconnect();
//////        } catch (JSchException e) {
//////            e.printStackTrace();
//////        }
//////    }
//////}
