package rc.soop.testarea;

//
//import com.google.common.util.concurrent.AtomicDouble;
//import com.itextpdf.text.BaseColor;
//import com.itextpdf.text.Chunk;
//import com.itextpdf.text.Document;
//import com.itextpdf.text.Element;
//import com.itextpdf.text.Font;
//import com.itextpdf.text.FontFactory;
//import com.itextpdf.text.PageSize;
//import com.itextpdf.text.Paragraph;
//import com.itextpdf.text.Phrase;
//import com.itextpdf.text.Rectangle;
//import com.itextpdf.text.pdf.BaseFont;
//import com.itextpdf.text.pdf.PdfPCell;
//import com.itextpdf.text.pdf.PdfPTable;
//import com.itextpdf.text.pdf.PdfWriter;
//import rc.soop.esolver.Branch;
//import static rc.soop.esolver.Util.fd;
//import static rc.soop.esolver.Util.generaId;
//import static rc.soop.esolver.Util.roundDoubleandFormat;
//import rc.soop.rilasciofile.BCE;
//import rc.soop.rilasciofile.Currency;
//import rc.soop.sftp.Db;
//import rc.soop.rilasciofile.Utility;
//import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
//import static rc.soop.rilasciofile.Utility.roundDouble;
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.OutputStream;
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
//import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;
//
///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
///**
// *
// * @author raf
// */
//public class recuperaOSP {
//
//    public static void main(String[] args) {
//        Db dbm = new Db(true);
//        ArrayList<Currency> cur = dbm.list_figures_query_edit(null);
//        ArrayList<Branch> allenabledbr = dbm.list_branch_completeAFTER311217();
//        List<String> br1 = allenabledbr.stream().map(valore -> valore.getCod()).distinct().collect(Collectors.toList());
//        ArrayList<BCE> rate = dbm.getRate3112();
//        ArrayList<ReportNew> complete = new ArrayList<>();
//
//        for (int i = 0; i < br1.size(); i++) {
//            try {
//                String cod1 = br1.get(i);
//                String desc1 = allenabledbr.stream().filter(c1 -> c1.getCod().equals(cod1)).findAny().get().getDe_branch();
//                String[] fil = {br1.get(i), desc1};
//
//                String sel1 = "SELECT codice FROM office_sp s WHERE s.data < '2022-01-01 00:00:00' AND s.filiale='"
//                        + cod1 + "' ORDER BY data DESC LIMIT 1";
//                try (ResultSet rs = dbm.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
//                        .executeQuery(sel1)) {
//                    if (rs.next()) {
//                        String codice = rs.getString(1);
//                        String sel2 = "SELECT * FROM office_sp_valori s WHERE s.cod='" + codice + "'";
//                        try (ResultSet rs2 = dbm.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
//                                .executeQuery(sel2)) {
//                            while (rs2.next()) {
//                                String v1 = rs2.getString("currency");
//                                String k1 = rs2.getString("kind");
//                                double q1 = roundDouble(fd(rs2.getString("quantity")), 2);
//                                if (!k1.equals("01")) {
//                                    System.err.println(cod1 + ") "
//                                            + v1 + " - " + rs2.getString("kind") + " " + rs2.getString("quantity") + " -- 1");
//                                    break;
//                                }
//                                if (rate.stream().filter(b1 -> b1.getValuta().equals(v1)).findAny().isPresent()) {
//                                    double bce1 = roundDouble(rate.stream().filter(b1 -> b1.getValuta().equals(v1)).findAny().get().getRif_bce(), 8);
//                                    double cv = roundDouble(q1 / bce1, 2);
//                                    complete.add(new ReportNew(cod1, v1, q1, bce1, cv));
//                                } else {
//                                    if (v1.equals("EUR")) {
//                                        complete.add(new ReportNew(cod1, v1, q1, 1, q1));
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//
//            } catch (Exception e) {
//
//            }
//
//        }
//
//        try {
//
//            Font f0_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 14.04f, Font.BOLD);
//            Font f1_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 9.96f, Font.BOLD);
//            Font f2_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.BOLD);
//            Font f3_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.BOLDITALIC);
//            Font f4_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 6.96f, Font.BOLD);
//            Font f1_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 9.96f, Font.NORMAL);
//            Font f2_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.NORMAL);
//            Font f3_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 6.96f, Font.NORMAL);
//            float[] columnWidths0 = new float[]{70f, 30f};
//            float[] columnWidthsf = new float[]{60f, 30f, 30f, 30f};
//            float[] columnWidthsf2 = new float[]{30f, 60f, 30f, 30f};
//
//            Phrase vuoto = new Phrase("");
//            Phrase vuoto1 = new Phrase("\n");
//            PdfPCell cellavuota = new PdfPCell(vuoto);
//            PdfPCell cellavuota1 = new PdfPCell(vuoto1);
//
//            File pdf = new File(path + generaId() + "_RIMANENZE AL 31122021.pdf");
//
//            Document document = new Document(PageSize.A4, 20, 20, 20, 20);
//            OutputStream ou = new FileOutputStream(pdf);
//            PdfWriter wr = PdfWriter.getInstance(document, ou);
//            document.open();
//
//            PdfPTable table = new PdfPTable(2);
//            table.setWidths(columnWidths0);
//            table.setWidthPercentage(100);
//
//            Phrase phrase1 = new Phrase();
//            phrase1.add(new Chunk("STATUS 31/12/2021 23:55 - GROUP BY CURRENCY", f3_bold));
//            PdfPCell cell1 = new PdfPCell(phrase1);
//            cell1.setBorder(Rectangle.NO_BORDER);
//
//            Paragraph pa1 = new Paragraph(new Phrase("", f3_normal));
//            pa1.setAlignment(Element.ALIGN_RIGHT);
//            PdfPCell cell2 = new PdfPCell(pa1);
//            cell2.setBorder(Rectangle.NO_BORDER);
//            cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);
//
//            table.addCell(cell1);
//            table.addCell(cell2);
//            document.add(table);
//
//            vuoto.setFont(f3_normal);
//            document.add(cellavuota);
//
//            PdfPTable table2 = new PdfPTable(4);
//            table2.setWidths(columnWidthsf);
//            table2.setWidthPercentage(100);
//
//            Phrase phraset1 = new Phrase();
//            phraset1.add(new Chunk("Currency", f4_bold));
//            PdfPCell cellt1 = new PdfPCell(phraset1);
//            cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
//            cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt1.setBorderWidth(0.7f);
//
//            Phrase phraset2 = new Phrase();
//            phraset2.add(new Chunk("Quantity - Kind 01", f4_bold));
//            PdfPCell cellt2 = new PdfPCell(phraset2);
//            cellt2.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt2.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt2.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt2.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt2.setBorderWidth(0.7f);
//
//            Phrase phraset3 = new Phrase();
//            phraset3.add(new Chunk("BCE 31/12/2021", f4_bold));
//            PdfPCell cellt3 = new PdfPCell(phraset3);
//            cellt3.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt3.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt3.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt3.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt3.setBorderWidth(0.7f);
//
//            Phrase phraset4 = new Phrase();
//            phraset4.add(new Chunk("Countervalue", f4_bold));
//            PdfPCell cellt4 = new PdfPCell(phraset4);
//            cellt4.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt4.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt4.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt4.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt4.setBorderWidth(0.7f);
//
//            table2.addCell(cellt1);
//            table2.addCell(cellt2);
//            table2.addCell(cellt3);
//            table2.addCell(cellt4);
//            document.add(table2);
//
//            List<String> pervaluta = complete.stream()
//                    .filter(f1 -> !f1.getValuta().equals("EUR"))
//                    .map(f1 -> f1.getValuta()).sorted().distinct().collect(Collectors.toList());
//
//            AtomicDouble tot2 = new AtomicDouble(0.0);
//            AtomicDouble cv2 = new AtomicDouble(0.0);
//
//            pervaluta.forEach(v1 -> {
//
//                AtomicDouble tot1 = new AtomicDouble(0.0);
//                AtomicDouble cv1 = new AtomicDouble(0.0);
//
//                complete.stream().filter(p1 -> p1.getValuta().equals(v1)).collect(Collectors.toList()).forEach(v2 -> {
//                    tot1.addAndGet(v2.getQuantita());
//                    cv1.addAndGet(roundDouble(v2.getQuantita() / v2.getBce(), 2));
//                });
//                try {
//                    PdfPTable table3 = new PdfPTable(4);
//                    table3.setWidths(columnWidthsf);
//                    table3.setWidthPercentage(100);
//                    Phrase phraset22 = new Phrase();
//                    String desc1 = cur.stream().filter(d1 -> d1.getCode().equals(v1)).findAny().get().getDescrizione();
//                    phraset22.add(new Chunk(v1 + " - " + desc1, f4_bold));
//                    PdfPCell cellt22 = new PdfPCell(phraset22);
//                    cellt22.setHorizontalAlignment(Element.ALIGN_LEFT);
//                    cellt22.setBorder(Rectangle.BOTTOM);
//                    table3.addCell(cellt22);
//                    phraset22 = new Phrase();
//                    phraset22.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(tot1.get(), 2)), f4_bold));
//                    cellt22 = new PdfPCell(phraset22);
//                    cellt22.setHorizontalAlignment(Element.ALIGN_RIGHT);
//                    cellt22.setBorder(Rectangle.BOTTOM);
//                    table3.addCell(cellt22);
//                    phraset22 = new Phrase();
//                    if (rate.stream().filter(b1 -> b1.getValuta().equals(v1)).findAny().isPresent()) {
//                        phraset22.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(rate.stream().filter(b1 -> b1.getValuta().equals(v1)).findAny().get().getRif_bce(), 8)), f4_bold));
//                    } else {
//                        phraset22.add(new Chunk("1", f4_bold));
//                    }
//                    cellt22 = new PdfPCell(phraset22);
//                    cellt22.setHorizontalAlignment(Element.ALIGN_RIGHT);
//                    cellt22.setBorder(Rectangle.BOTTOM);
//                    table3.addCell(cellt22);
//                    phraset22 = new Phrase();
//                    phraset22.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(cv1.get(), 2)), f4_bold));
//                    cellt22 = new PdfPCell(phraset22);
//                    cellt22.setHorizontalAlignment(Element.ALIGN_RIGHT);
//                    cellt22.setBorder(Rectangle.BOTTOM);
//                    table3.addCell(cellt22);
//                    document.add(table3);
//                    tot2.addAndGet(tot1.get());
//                    cv2.addAndGet(cv1.get());
//                } catch (Exception ex2) {
//                    ex2.printStackTrace();
//                }
////                System.out.println(v1 + " - " + roundDoubleandFormat(tot1.get(), 2) + " - " + roundDoubleandFormat(cv1.get(), 2));
//            });
//
//            table2 = new PdfPTable(4);
//            table2.setWidths(columnWidthsf);
//            table2.setWidthPercentage(100);
//
//            phraset1 = new Phrase();
//            phraset1.add(new Chunk("TOTAL", f4_bold));
//            cellt1 = new PdfPCell(phraset1);
//            cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
//            cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt1.setBorderWidth(0.7f);
//
//            phraset2 = new Phrase();
//            phraset2.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(tot2.get(), 2)), f4_bold));
//            cellt2 = new PdfPCell(phraset2);
//            cellt2.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt2.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt2.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt2.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt2.setBorderWidth(0.7f);
//
//            phraset3 = new Phrase();
//            phraset3.add(new Chunk("", f4_bold));
//            cellt3 = new PdfPCell(phraset3);
//            cellt3.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt3.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt3.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt3.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt3.setBorderWidth(0.7f);
//
//            phraset4 = new Phrase();
//            phraset4.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(cv2.get(), 2)), f4_bold));
//            cellt4 = new PdfPCell(phraset4);
//            cellt4.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt4.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt4.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt4.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt4.setBorderWidth(0.7f);
//
//            table2.addCell(cellt1);
//            table2.addCell(cellt2);
//            table2.addCell(cellt3);
//            table2.addCell(cellt4);
//            document.add(table2);
//            document.newPage();
//
//            table = new PdfPTable(2);
//            table.setWidths(columnWidths0);
//            table.setWidthPercentage(100);
//
//            phrase1 = new Phrase();
//            phrase1.add(new Chunk("STATUS 31/12/2021 23:55 - GROUP BY BRANCH", f3_bold));
//            cell1 = new PdfPCell(phrase1);
//            cell1.setBorder(Rectangle.NO_BORDER);
//
//            pa1 = new Paragraph(new Phrase("", f3_normal));
//            pa1.setAlignment(Element.ALIGN_RIGHT);
//            cell2 = new PdfPCell(pa1);
//            cell2.setBorder(Rectangle.NO_BORDER);
//            cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);
//
//            table.addCell(cell1);
//            table.addCell(cell2);
//            document.add(table);
//
//            vuoto.setFont(f3_normal);
//            document.add(cellavuota);
//
//            table2 = new PdfPTable(4);
//            table2.setWidths(columnWidthsf2);
//            table2.setWidthPercentage(100);
//
//            phraset1 = new Phrase();
//            phraset1.add(new Chunk("BRANCH CODE", f4_bold));
//            cellt1 = new PdfPCell(phraset1);
//            cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
//            cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt1.setBorderWidth(0.7f);
//
//            phraset2 = new Phrase();
//            phraset2.add(new Chunk("BRANCH NAME", f4_bold));
//            cellt2 = new PdfPCell(phraset2);
//            cellt2.setHorizontalAlignment(Element.ALIGN_LEFT);
//            cellt2.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt2.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt2.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt2.setBorderWidth(0.7f);
//
//            phraset3 = new Phrase();
//            phraset3.add(new Chunk("Quantity - Kind 01", f4_bold));
//            cellt3 = new PdfPCell(phraset3);
//            cellt3.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt3.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt3.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt3.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt3.setBorderWidth(0.7f);
//
//            phraset4 = new Phrase();
//            phraset4.add(new Chunk("Countervalue", f4_bold));
//            cellt4 = new PdfPCell(phraset4);
//            cellt4.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt4.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt4.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt4.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt4.setBorderWidth(0.7f);
//
//            table2.addCell(cellt1);
//            table2.addCell(cellt2);
//            table2.addCell(cellt3);
//            table2.addCell(cellt4);
//            document.add(table2);
//
//            AtomicDouble tot3 = new AtomicDouble(0.0);
//            AtomicDouble cv3 = new AtomicDouble(0.0);
//
//            for (int i = 0; i < br1.size(); i++) {
//                String f2 = br1.get(i);
//                List<ReportNew> perfiliale = complete.stream().filter(p1 -> p1.getFiliale().equals(f2) && !p1.getValuta().equals("EUR")).collect(Collectors.toList());
//                if (!perfiliale.isEmpty()) {
//                    AtomicDouble tot1 = new AtomicDouble(0.0);
//                    AtomicDouble cv1 = new AtomicDouble(0.0);
//                    perfiliale.forEach(v1 -> {
//                        tot1.addAndGet(v1.getQuantita());
//                        cv1.addAndGet(roundDouble(v1.getQuantita() / v1.getBce(), 2));
//                    });
//
//                    try {
//                        PdfPTable table3 = new PdfPTable(4);
//                        table3.setWidths(columnWidthsf2);
//                        table3.setWidthPercentage(100);
//                        Phrase phraset22 = new Phrase();
//                        phraset22.add(new Chunk(f2, f4_bold));
//                        PdfPCell cellt22 = new PdfPCell(phraset22);
//                        cellt22.setHorizontalAlignment(Element.ALIGN_LEFT);
//                        cellt22.setBorder(Rectangle.BOTTOM);
//                        table3.addCell(cellt22);
//                        phraset22 = new Phrase();
//                        String desc1 = allenabledbr.stream().filter(c1 -> c1.getCod().equals(f2)).findAny().get().getDe_branch();
//                        phraset22.add(new Chunk(desc1, f4_bold));
//                        cellt22 = new PdfPCell(phraset22);
//                        cellt22.setHorizontalAlignment(Element.ALIGN_LEFT);
//                        cellt22.setBorder(Rectangle.BOTTOM);
//                        table3.addCell(cellt22);
//                        phraset22 = new Phrase();
//                        phraset22.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(tot1.get(), 2)), f4_bold));
//                        cellt22 = new PdfPCell(phraset22);
//                        cellt22.setHorizontalAlignment(Element.ALIGN_RIGHT);
//                        cellt22.setBorder(Rectangle.BOTTOM);
//                        table3.addCell(cellt22);
//                        phraset22 = new Phrase();
//                        phraset22.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(cv1.get(), 2)), f4_bold));
//                        cellt22 = new PdfPCell(phraset22);
//                        cellt22.setHorizontalAlignment(Element.ALIGN_RIGHT);
//                        cellt22.setBorder(Rectangle.BOTTOM);
//                        table3.addCell(cellt22);
//                        document.add(table3);
//                        tot3.addAndGet(tot1.get());
//                        cv3.addAndGet(cv1.get());
//                    } catch (Exception ex2) {
//                        ex2.printStackTrace();
//                    }
//                }
//            }
//
//            table2 = new PdfPTable(4);
//            table2.setWidths(columnWidthsf2);
//            table2.setWidthPercentage(100);
//
//            phraset1 = new Phrase();
//            phraset1.add(new Chunk("TOTAL", f4_bold));
//            cellt1 = new PdfPCell(phraset1);
//            cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
//            cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt1.setBorderWidth(0.7f);
//
//            phraset2 = new Phrase();
//            phraset2.add(new Chunk("", f4_bold));
//            cellt2 = new PdfPCell(phraset2);
//            cellt2.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt2.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt2.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt2.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt2.setBorderWidth(0.7f);
//
//            phraset3 = new Phrase();
//            phraset3.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(tot3.get(), 2)), f4_bold));
//            cellt3 = new PdfPCell(phraset3);
//            cellt3.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt3.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt3.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt3.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt3.setBorderWidth(0.7f);
//
//            phraset4 = new Phrase();
//            phraset4.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(cv3.get(), 2)), f4_bold));
//            cellt4 = new PdfPCell(phraset4);
//            cellt4.setHorizontalAlignment(Element.ALIGN_RIGHT);
//            cellt4.setVerticalAlignment(Element.ALIGN_MIDDLE);
//            cellt4.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
//            cellt4.setBackgroundColor(BaseColor.LIGHT_GRAY);
//            cellt4.setBorderWidth(0.7f);
//
//            table2.addCell(cellt1);
//            table2.addCell(cellt2);
//            table2.addCell(cellt3);
//            table2.addCell(cellt4);
//            document.add(table2);
//
//            document.close();
//            wr.close();
//            ou.close();
//            System.out.println(pdf.getPath());
//        } catch (Exception ex1) {
//            ex1.printStackTrace();
//        }
//
//        dbm.closeDB();
//    }
//
//}
//
//class ReportNew {
//
//    String filiale;
//    String valuta;
//    double quantita;
//    double bce;
//    double cv;
//
//    public ReportNew(String filiale, String valuta, double quantita, double bce, double cv) {
//        this.filiale = filiale;
//        this.valuta = valuta;
//        this.quantita = quantita;
//        this.bce = bce;
//        this.cv = cv;
//    }
//
//    public String getFiliale() {
//        return filiale;
//    }
//
//    public void setFiliale(String filiale) {
//        this.filiale = filiale;
//    }
//
//    public String getValuta() {
//        return valuta;
//    }
//
//    public void setValuta(String valuta) {
//        this.valuta = valuta;
//    }
//
//    public double getQuantita() {
//        return quantita;
//    }
//
//    public void setQuantita(double quantita) {
//        this.quantita = quantita;
//    }
//
//    public double getBce() {
//        return bce;
//    }
//
//    public void setBce(double bce) {
//        this.bce = bce;
//    }
//
//    public double getCv() {
//        return cv;
//    }
//
//    public void setCv(double cv) {
//        this.cv = cv;
//    }
//
//    @Override
//    public String toString() {
//        return ReflectionToStringBuilder.toString(this, JSON_STYLE);
//    }
//
//}
