/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import com.google.common.util.concurrent.AtomicDouble;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.draw.LineSeparator;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.generaId;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
import static rc.soop.rilasciofile.Utility.roundDouble;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author vcrugliano
 */
public class StockPrice {

    // column
    /**
     *
     */
    public static final float[] columnWidths0 = new float[] { 70f, 30f };
    public static final float[] columnWidthsf = new float[] { 60f, 30f, 30f };

    /**
     *
     */
    public static final float[] columnWidths1 = new float[] { 60f, 30f };

    /**
     *
     */
    public static final float[] columnWidths2 = new float[] { 20f, 25f, 25f, 20f, 20f, 20f };
    public static final float[] columnWidths3 = new float[] { 20f, 25f, 25f, 20f, 20f, 25f, 20f };
    final String intestazionePdf = "Historical Stock Price Report";
    Phrase vuoto = new Phrase("");
    Phrase vuoto1 = new Phrase("\n");
    PdfPCell cellavuota = new PdfPCell(vuoto);
    PdfPCell cellavuota1 = new PdfPCell(vuoto1);

    // resource
    // public static final String logo = "web/resource/logocl.png";
    // other
    /**
     *
     */
    public static final String br = "\n";

    /**
     *
     */
    public static final String blank = " ";

    Font f0_bold, f1_bold, f2_bold, f1_normal, f2_normal, f3_normal, f3_bold, f4_bold;

    /**
     * Costructor
     */
    public StockPrice() {

        cellavuota.setBorder(Rectangle.NO_BORDER);
        cellavuota1.setBorder(Rectangle.NO_BORDER);

        this.f0_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 14.04f, Font.BOLD);
        this.f1_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 9.96f, Font.BOLD);
        this.f2_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.BOLD);
        this.f3_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.BOLDITALIC);
        this.f4_bold = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 6.96f, Font.BOLD);
        this.f1_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 9.96f, Font.NORMAL);
        this.f2_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 8f, Font.NORMAL);
        this.f3_normal = FontFactory.getFont(BaseFont.HELVETICA, BaseFont.WINANSI, 6.96f, Font.NORMAL);

    }

    /**
     *
     * @param qty
     * @param controvalore
     * @param delta
     * @param actual
     * @param document
     */
    public void scriviTotaleValuta(ArrayList<String> qty, ArrayList<String> controvalore, ArrayList<String> delta,
            ArrayList<String> rate, StockPrice_value actual, Document document) {
        try {
            double totqty = 0;
            double totcontrovalore = 0;
            double totdelta = 0;
            double totrate = 0;

            for (int k = 0; k < qty.size(); k++) {

                totrate += (fd(rate.get(k)) * fd(qty.get(k)));
                totqty += fd(qty.get(k));
                totcontrovalore += fd(controvalore.get(k));
                totdelta += fd(delta.get(k));
            }

            totqty = Utility.roundDouble(totqty, 2);
            totcontrovalore = Utility.roundDouble(totcontrovalore, 2);
            totdelta = Utility.roundDouble(totdelta, 2);
            if (totqty != 0) {
                totrate = Utility.roundDouble(totrate / totqty, 8);
            } else {
            }

            LineSeparator ls = new LineSeparator();
            ls.setLineWidth((float) 0.7);
            document.add(ls);

            PdfPTable table5 = new PdfPTable(6);
            table5.setWidths(columnWidths2);

            table5 = new PdfPTable(7);
            table5.setWidths(columnWidths3);

            table5.setWidthPercentage(100);

            Phrase phraset = new Phrase();
            phraset.add(new Chunk("Totale Valuta " + actual.getCurrency(), f4_bold));
            PdfPCell cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totrate, 8)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totqty, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totcontrovalore, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totdelta, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);

            table5.addCell(cellavuota1);

            document.add(table5);

            ls = new LineSeparator();
            ls.setLineWidth((float) 0.7);

        } catch (DocumentException ex) {
            ex.printStackTrace();
        }
    }

    /**
     *
     * @param qty
     * @param controvalore
     * @param delta
     * @param actual
     * @param document
     */
    public void scriviTotaleSupporto(ArrayList<String> qty, ArrayList<String> controvalore, ArrayList<String> delta,
            ArrayList<String> rate, StockPrice_value actual, Document document) {

        try {

            PdfPTable table5 = new PdfPTable(6);
            table5.setWidths(columnWidths2);
            table5 = new PdfPTable(7);
            table5.setWidths(columnWidths3);
            table5.setWidthPercentage(100);

            Phrase phraset = new Phrase();
            phraset.add(new Chunk("Totale Supporto " + actual.getSupportocod(), f4_bold));
            PdfPCell cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            table5.addCell(cellavuota1);
            table5.addCell(cellavuota1);

            double totqty = 0;
            double totcontrovalore = 0;
            double totdelta = 0;

            double totrate = 0;

            for (int k = 0; k < qty.size(); k++) {

                totrate += (fd(rate.get(k)) * fd(qty.get(k)));
                totqty += fd(qty.get(k));
                totcontrovalore += fd(controvalore.get(k));
                totdelta += fd(delta.get(k));

            }

            totqty = roundDouble(totqty, 2);
            totcontrovalore = roundDouble(totcontrovalore, 2);
            totdelta = roundDouble(totdelta, 2);
            if (totqty != 0) {
                totrate = roundDouble(totrate / totqty, 8);
            } else {
            }

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totrate, 8)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totqty, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totcontrovalore, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totdelta, 2)), f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table5.addCell(cellt);

            LineSeparator ls = new LineSeparator();
            ls.setLineWidth((float) 0.7);
            document.add(ls);

            document.add(table5);
            document.add(ls);

        } catch (DocumentException ex) {
            ex.printStackTrace();
        }

    }

    /**
     *
     * @param document
     */
    public void scriviIntestazioneColonne(Document document) {

        try {
            PdfPTable table2 = new PdfPTable(6);
            table2.setWidths(columnWidths2);

            table2 = new PdfPTable(7);
            table2.setWidths(columnWidths3);

            table2.setWidthPercentage(100);

            Phrase phraset1 = new Phrase();
            phraset1.add(new Chunk("Type", f4_bold));
            PdfPCell cellt1 = new PdfPCell(phraset1);
            cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
            cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt1.setBorderWidth(0.7f);

            Phrase phraset2 = new Phrase();
            phraset2.add(new Chunk("Operation", f4_bold));
            PdfPCell cellt2 = new PdfPCell(phraset2);
            cellt2.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt2.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt2.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt2.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt2.setBorderWidth(0.7f);

            Phrase phraset3 = new Phrase();
            phraset3.add(new Chunk("Date", f4_bold));
            PdfPCell cellt3 = new PdfPCell(phraset3);
            cellt3.setHorizontalAlignment(Element.ALIGN_LEFT);
            cellt3.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt3.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt3.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt3.setBorderWidth(0.7f);

            Phrase phraset4 = new Phrase();
            phraset4.add(new Chunk("Historical BCE Rate", f4_bold));

            PdfPCell cellt4 = new PdfPCell(phraset4);
            cellt4.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt4.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt4.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt4.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt4.setBorderWidth(0.7f);

            Phrase phraset5 = new Phrase();
            phraset5.add(new Chunk("Quantity", f4_bold));
            PdfPCell cellt5 = new PdfPCell(phraset5);
            cellt5.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt5.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt5.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt5.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt5.setBorderWidth(0.7f);

            Phrase phraset6 = new Phrase();
            phraset6.add(new Chunk("Historical BCE Equivalent", f4_bold));

            PdfPCell cellt6 = new PdfPCell(phraset6);
            cellt6.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt6.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt6.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt6.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt6.setBorderWidth(0.7f);

            table2.addCell(cellt1);
            table2.addCell(cellt2);
            table2.addCell(cellt3);
            table2.addCell(cellt4);
            table2.addCell(cellt5);
            table2.addCell(cellt6);

            Phrase phraset7 = new Phrase();
            phraset7.add(new Chunk("Delta Equivalent", f4_bold));

            PdfPCell cellt7 = new PdfPCell(phraset7);
            cellt7.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt7.setVerticalAlignment(Element.ALIGN_MIDDLE);
            cellt7.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
            cellt7.setBackgroundColor(BaseColor.LIGHT_GRAY);
            cellt7.setBorderWidth(0.7f);
            table2.addCell(cellt7);

            document.add(table2);

        } catch (DocumentException ex) {
            ex.printStackTrace();
        }
    }

    /**
     *
     * @param document
     * @param prossimo
     */
    public void scriviIntestazioneSupporto(Document document, StockPrice_value prossimo) {
        try {
            PdfPTable table4 = new PdfPTable(6);

            table4.setWidths(columnWidths2);

            table4 = new PdfPTable(7);
            table4.setWidths(columnWidths3);
            table4.setWidthPercentage(100);
            Phrase phraset = new Phrase();
            phraset.add(new Chunk(prossimo.getSupportodesc(), f4_bold));
            PdfPCell cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table4.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk("", f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table4.addCell(cellt);

            phraset = new Phrase();
            phraset.add(new Chunk("", f4_bold));
            cellt = new PdfPCell(phraset);
            cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
            cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
            cellt.setBorder(Rectangle.NO_BORDER);
            table4.addCell(cellt);

            table4.addCell(cellavuota);
            table4.addCell(cellavuota);
            table4.addCell(cellavuota);

            table4.addCell(cellavuota);

            document.add(table4);

        } catch (DocumentException ex) {
            Logger.getLogger(StockPrice.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param path
     * @param list_siq
     * @param datereport
     * @return
     */
    public String receipt_multi(String path, ArrayList<StockPrice_value> list_siq, String datereport) {

        List<File> pdflist = new ArrayList<>();

        AtomicDouble totalmac_equivalent = new AtomicDouble(0.0);
        AtomicDouble totalmac_delta = new AtomicDouble(0.0);

        list_siq.forEach(siq -> {
            File pdf = new File(path + generaId() + "StockPriceReport.pdf");
            Document document = new Document(PageSize.A4, 20, 20, 20, 20);
            try (

                    OutputStream ou = new FileOutputStream(pdf)) {
                PdfWriter wr = PdfWriter.getInstance(document, ou);
                document.open();

                PdfPTable table = new PdfPTable(2);
                table.setWidths(columnWidths0);
                table.setWidthPercentage(100);

                Phrase phrase1 = new Phrase();
                phrase1.add(new Chunk(intestazionePdf + "  " + datereport, f3_bold));
                PdfPCell cell1 = new PdfPCell(phrase1);
                cell1.setBorder(Rectangle.NO_BORDER);

                Paragraph pa1 = new Paragraph(new Phrase("", f3_normal));
                pa1.setAlignment(Element.ALIGN_RIGHT);
                PdfPCell cell2 = new PdfPCell(pa1);
                cell2.setBorder(Rectangle.NO_BORDER);
                cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);

                Phrase phrase3 = new Phrase();
                phrase3.add(new Chunk("\n " + siq.getId_filiale() + " " + siq.getDe_filiale(), f3_normal));
                PdfPCell cell3 = new PdfPCell(phrase3);
                cell3.setBorder(Rectangle.NO_BORDER);

                Phrase phrase4 = new Phrase();
                phrase4.add(new Chunk("", f3_normal));
                PdfPCell cell4 = new PdfPCell(phrase4);
                cell4.setBorder(Rectangle.NO_BORDER);

                table.addCell(cell1);
                table.addCell(cell2);
                table.addCell(cell3);
                table.addCell(cell4);
                document.add(table);

                vuoto.setFont(f3_normal);
                document.add(cellavuota);

                scriviIntestazioneColonne(document);

                // Popolo la tabella
                PdfPTable table3, table4, table5, table6;
                ArrayList<StockPrice_value> dati = siq.getDati();
                Phrase phraset;
                PdfPCell cellt;

                ArrayList<String> qtyValuta = new ArrayList<>();
                ArrayList<String> rateValuta = new ArrayList<>();
                ArrayList<String> rateSupporto = new ArrayList<>();
                ArrayList<String> controvaloreValuta = new ArrayList<>();
                ArrayList<String> totalefinalecontrovalore = new ArrayList<>();
                ArrayList<String> qtySupporto = new ArrayList<>();
                ArrayList<String> controvaloreSupporto = new ArrayList<>();
                ArrayList<String> deltaValuta = new ArrayList<>();
                ArrayList<String> deltaSupporto = new ArrayList<>();
                ArrayList<String> deltaFinale = new ArrayList<>();

                boolean firstime = true;

                for (int i = 0; i < dati.size(); i++) {

                    table3 = new PdfPTable(6);
                    table3.setWidths(columnWidths2);
                    table3 = new PdfPTable(7);
                    table3.setWidths(columnWidths3);

                    table3.setWidthPercentage(100);

                    StockPrice_value actual = dati.get(i);
                    StockPrice_value prossimo;

                    if (i == dati.size() - 1) {
                        prossimo = dati.get(i);
                    } else {

                        prossimo = dati.get(i + 1);
                    }

                    if (firstime) {
                        firstime = false;

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getCurrency(), f4_bold));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table3.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getDe_currency(), f4_bold));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table3.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getDateactual(), f4_bold));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table3.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getHistoricalBCE()), f4_bold));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table3.addCell(cellt);

                        table3.addCell(cellavuota);
                        table3.addCell(cellavuota);

                        table3.addCell(cellavuota);

                        document.add(table3);

                        scriviIntestazioneSupporto(document, actual);

                    }

                    if (actual.getCurrency().equals(prossimo.getCurrency())) {

                        qtyValuta.add(actual.getQuantitaSenzaFormattazione());

                        controvaloreValuta.add(actual.getControvaloreSenzaFormattazione());
                        totalefinalecontrovalore.add(actual.getControvaloreSenzaFormattazione());

                        qtySupporto.add(actual.getQuantitaSenzaFormattazione());
                        controvaloreSupporto.add(actual.getControvaloreSenzaFormattazione());

                        deltaValuta.add(actual.getDeltaEquivalent());
                        deltaSupporto.add(actual.getDeltaEquivalent());
                        deltaFinale.add(actual.getDeltaEquivalent());

                        rateValuta.add(actual.getCambio());
                        rateSupporto.add(actual.getCambio());

                        table6 = new PdfPTable(6);
                        table6.setWidths(columnWidths2);

                        table6 = new PdfPTable(7);
                        table6.setWidths(columnWidths3);

                        table6.setWidthPercentage(100);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getBcanconote1(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getSupportovalue(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getBanconote2(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getCambio()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getQuantita()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getControvalore()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getDeltaEquivalent()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.BOTTOM);
                        table6.addCell(cellt);

                        document.add(table6);

                        if (!actual.getSupportocod().equals(prossimo.getSupportocod())) {
                            // se la valuta è uguale ma il supporto è diverso

                            table4 = new PdfPTable(6);
                            table4.setWidths(columnWidths2);
                            table4 = new PdfPTable(7);
                            table4.setWidths(columnWidths3);
                            table4.setWidthPercentage(100);

                            scriviTotaleSupporto(qtySupporto, controvaloreSupporto, deltaSupporto, rateSupporto, actual,
                                    document);
                            qtySupporto.removeAll(qtySupporto);
                            controvaloreSupporto.removeAll(controvaloreSupporto);
                            deltaSupporto.removeAll(deltaSupporto);
                            rateSupporto.removeAll(rateSupporto);

                            scriviIntestazioneSupporto(document, prossimo);

                        }

                    } else {

                        firstime = true;
                        qtyValuta.add(actual.getQuantitaSenzaFormattazione());

                        controvaloreValuta.add(actual.getControvaloreSenzaFormattazione());
                        totalefinalecontrovalore.add(actual.getControvaloreSenzaFormattazione());

                        qtySupporto.add(actual.getQuantitaSenzaFormattazione());
                        controvaloreSupporto.add(actual.getControvaloreSenzaFormattazione());

                        deltaSupporto.add(actual.getDeltaEquivalent());
                        deltaValuta.add(actual.getDeltaEquivalent());
                        deltaFinale.add(actual.getDeltaEquivalent());

                        rateSupporto.add(actual.getCambio());
                        rateValuta.add(actual.getCambio());

                        table6 = new PdfPTable(6);
                        table6.setWidths(columnWidths2);
                        table6 = new PdfPTable(7);
                        table6.setWidths(columnWidths3);
                        table6.setWidthPercentage(100);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getBcanconote1(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getSupportovalue(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(actual.getBanconote2(), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getCambio()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getQuantita()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getControvalore()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        phraset = new Phrase();
                        phraset.add(new Chunk(formatMysqltoDisplay(actual.getDeltaEquivalent()), f3_normal));
                        cellt = new PdfPCell(phraset);
                        cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                        cellt.setBorder(Rectangle.NO_BORDER);
                        table6.addCell(cellt);

                        document.add(table6);

                        scriviTotaleSupporto(qtySupporto, controvaloreSupporto, deltaSupporto, rateSupporto, actual,
                                document);

                        scriviTotaleValuta(qtyValuta, controvaloreValuta, deltaValuta, rateValuta, actual, document);

                        qtyValuta.removeAll(qtyValuta);
                        rateValuta.removeAll(rateValuta);
                        deltaValuta.removeAll(deltaValuta);
                        controvaloreValuta.removeAll(controvaloreValuta);

                        qtySupporto.removeAll(qtySupporto);
                        rateSupporto.removeAll(rateSupporto);
                        controvaloreSupporto.removeAll(controvaloreSupporto);
                        deltaSupporto.removeAll(deltaSupporto);

                    }

                    if (i == dati.size() - 1) {// sono alla fine
                        table5 = new PdfPTable(6);
                        table5.setWidths(columnWidths2);
                        table5.setWidthPercentage(100);

                        scriviTotaleSupporto(qtySupporto, controvaloreSupporto, deltaSupporto, rateSupporto, actual,
                                document);
                        scriviTotaleValuta(qtyValuta, controvaloreValuta, deltaValuta, rateValuta, actual, document);

                        LineSeparator ls = new LineSeparator();
                        ls.setLineWidth((float) 0.7);
                        document.add(ls);

                        qtyValuta.removeAll(qtyValuta);
                        rateValuta.removeAll(rateValuta);
                        controvaloreValuta.removeAll(controvaloreValuta);
                        deltaValuta.removeAll(deltaValuta);

                        qtySupporto.removeAll(qtySupporto);
                        rateSupporto.removeAll(rateSupporto);
                        controvaloreSupporto.removeAll(controvaloreSupporto);

                        deltaSupporto.removeAll(deltaSupporto);

                    }

                }

                // qui scrivo l'ultima riga con la somma totale del controvalore
                table6 = new PdfPTable(6);
                table6.setWidths(columnWidths2);

                table6 = new PdfPTable(7);
                table6.setWidths(columnWidths3);
                table6.setWidthPercentage(100);

                phraset = new Phrase();
                phraset.add(new Chunk("Totale Filiale " + siq.getId_filiale(), f4_bold));
                cellt = new PdfPCell(phraset);
                cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                cellt.setBorder(Rectangle.NO_BORDER);
                table6.addCell(cellt);

                table6.addCell(cellavuota);
                table6.addCell(cellavuota);
                table6.addCell(cellavuota);
                table6.addCell(cellavuota);

                double totfincontrovalore = 0;
                for (int k = 0; k < totalefinalecontrovalore.size(); k++) {
                    totfincontrovalore += fd(totalefinalecontrovalore.get(k));
                }

                double totfindelta = 0;
                for (int k = 0; k < deltaFinale.size(); k++) {
                    totfindelta += fd(deltaFinale.get(k));
                }

                phraset = new Phrase();
                phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totfincontrovalore, 2)), f4_bold));
                cellt = new PdfPCell(phraset);
                cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                cellt.setBorder(Rectangle.NO_BORDER);
                table6.addCell(cellt);

                phraset = new Phrase();
                phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totfindelta, 2)), f4_bold));
                cellt = new PdfPCell(phraset);
                cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                cellt.setBorder(Rectangle.NO_BORDER);
                table6.addCell(cellt);

                LineSeparator ls = new LineSeparator();
                ls.setLineWidth((float) 0.7);
                document.add(ls);

                document.add(table6);

                ls = new LineSeparator();
                ls.setLineWidth((float) 1);
                document.add(ls);

                //
                document.close();
                wr.close();
                ou.close();

                pdflist.add(pdf);
                totalmac_equivalent.addAndGet(totfincontrovalore);
                totalmac_delta.addAndGet(totfindelta);

                siq.setEquivalent(totfincontrovalore);
                siq.setDelta(totfindelta);

            } catch (Exception ex) {
                ex.printStackTrace();
            }

        });

        if (pdflist.isEmpty()) {
            return null;
        } else {

            File pdftemp = new File(path + Utility.generaId() + "HistoricalStockPriceReport_last.pdf");
            Document document = new Document(PageSize.A4, 20, 20, 20, 20);
            try (

                    // CREO ultima pagina
                    OutputStream ou = new FileOutputStream(pdftemp)) {

                PdfWriter wr = PdfWriter.getInstance(document, ou);
                document.open();
                PdfPTable table = new PdfPTable(3);
                table.setWidths(columnWidthsf);
                table.setWidthPercentage(100);
                Phrase phrase1 = new Phrase();
                String title = intestazionePdf + "  " + datereport + " - RECAP";
                phrase1.add(new Chunk(title, f3_bold));
                PdfPCell cell1 = new PdfPCell(phrase1);
                cell1.setBorder(Rectangle.NO_BORDER);
                Paragraph pa1 = new Paragraph("");
                pa1.setAlignment(Element.ALIGN_LEFT);
                PdfPCell cell2 = new PdfPCell(pa1);
                cell2.setBorder(Rectangle.NO_BORDER);
                cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);
                table.addCell(cell1);
                table.addCell(cell2);
                table.addCell(cell2);
                document.add(table);
                vuoto.setFont(f3_normal);
                document.add(vuoto);

                table = new PdfPTable(3);
                table.setWidths(columnWidthsf);
                table.setWidthPercentage(100);
                Phrase phraset1 = new Phrase();
                phraset1.add(new Chunk("BRANCH", f4_bold));
                PdfPCell cellt1 = new PdfPCell(phraset1);
                cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
                cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt1.setBorderWidth(0.7f);

                Phrase phraset6 = new Phrase();
                phraset6.add(new Chunk("Historical BCE Equivalent", f4_bold));

                PdfPCell cellt6 = new PdfPCell(phraset6);
                cellt6.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt6.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt6.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt6.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt6.setBorderWidth(0.7f);

                Phrase phraset7 = new Phrase();
                phraset7.add(new Chunk("Delta Equivalent", f4_bold));

                PdfPCell cellt7 = new PdfPCell(phraset7);
                cellt7.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt7.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt7.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt7.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt7.setBorderWidth(0.7f);
                table.addCell(cellt1);
                table.addCell(cellt6);
                table.addCell(cellt7);

                document.add(table);

                PdfPTable table1 = new PdfPTable(3);
                table1.setWidths(columnWidthsf);
                table1.setWidthPercentage(100);

                list_siq.forEach(siq -> {

                    Phrase phraset = new Phrase();
                    phraset.add(new Chunk(siq.getId_filiale() + " " + siq.getDe_filiale(), f4_bold));
                    PdfPCell cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                    cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                    cellt.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                    table1.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(siq.getEquivalent(), 2)), f4_bold));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                    cellt.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                    table1.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(siq.getDelta(), 2)), f4_bold));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setVerticalAlignment(Element.ALIGN_BOTTOM);
                    cellt.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                    table1.addCell(cellt);

                });

                phraset1 = new Phrase();
                phraset1.add(new Chunk("TOTAL SYSTEM", f4_bold));
                cellt1 = new PdfPCell(phraset1);
                cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
                cellt1.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt1.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt1.setBorderWidth(0.7f);
                phraset6 = new Phrase();
                phraset6.add(
                        new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totalmac_equivalent.get(), 2)), f4_bold));
                cellt6 = new PdfPCell(phraset6);
                cellt6.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt6.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt6.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt6.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt6.setBorderWidth(0.7f);
                phraset7 = new Phrase();
                phraset7.add(new Chunk(formatMysqltoDisplay(roundDoubleandFormat(totalmac_delta.get(), 2)), f4_bold));
                cellt7 = new PdfPCell(phraset7);
                cellt7.setHorizontalAlignment(Element.ALIGN_RIGHT);
                cellt7.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cellt7.setBorder(Rectangle.TOP | Rectangle.BOTTOM);
                cellt7.setBackgroundColor(BaseColor.LIGHT_GRAY);
                cellt7.setBorderWidth(0.7f);
                table1.addCell(cellt1);
                table1.addCell(cellt6);
                table1.addCell(cellt7);
                document.add(table1);
                document.close();
                wr.close();
                ou.close();
                File pdf = new File(path + Utility.generaId() + "HistoricalStockPriceReport.pdf");
                PDFMergerUtility obj = new PDFMergerUtility();
                obj.setDestinationFileName(pdf.getPath());
                obj.addSource(pdftemp);
                pdflist.forEach(pdf1 -> {
                    try {
                        obj.addSource(pdf1);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
                obj.mergeDocuments(MemoryUsageSetting.setupTempFileOnly());
                String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(pdf)));
                return base64;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return null;
        }

    }

    public static final String formatdataCell = "#,#.00";

    /**
     *
     */
    public static final String decimalformat = "#0.00";

    /**
     *
     */
    public static final String formatdataCellRate = "#,#.00000000";

    /**
     *
     */
    public static final String formatdataCellint = "#,#";

    /**
     *
     * @param path
     * @param list_siq
     * @param datereport
     * @return
     */
    public String receiptexcel_multi(String path, ArrayList<StockPrice_value> list_siq, String datereport) {

        File pdf = new File(path + generaId() + "HistoricalStockPriceReport.xlsx");
        XSSFWorkbook workbook = new XSSFWorkbook();

        XSSFDataFormat hssfDataFormat = workbook.createDataFormat();
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
        style3.setAlignment(HorizontalAlignment.RIGHT);
        style3.setBorderTop(BorderStyle.THIN);
        style3.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle style3num = workbook.createCellStyle();
        style3num.setFont(font3);
        style3num.setAlignment(HorizontalAlignment.RIGHT);
        style3num.setBorderTop(BorderStyle.THIN);
        style3num.setBorderBottom(BorderStyle.THIN);
        style3num.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

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
        style4.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle style4num = workbook.createCellStyle();
        style4num.setAlignment(HorizontalAlignment.RIGHT);
        style4num.setBorderTop(BorderStyle.THIN);
        style4num.setBorderBottom(BorderStyle.THIN);
        style4num.setDataFormat(hssfDataFormat.getFormat(formatdataCell));

        XSSFCellStyle style4left = workbook.createCellStyle();
        style4left.setAlignment(HorizontalAlignment.LEFT);
        style4left.setBorderTop(BorderStyle.THIN);
        style4left.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle cellStylenumRATE = workbook.createCellStyle();
        cellStylenumRATE.setDataFormat(hssfDataFormat.getFormat(formatdataCellRate));
        cellStylenumRATE.setAlignment(HorizontalAlignment.RIGHT);
        cellStylenumRATE.setBorderTop(BorderStyle.THIN);
        cellStylenumRATE.setBorderBottom(BorderStyle.THIN);

        XSSFCellStyle cellStylenumRATEbold = workbook.createCellStyle();
        cellStylenumRATEbold.setFont(font3);
        cellStylenumRATEbold.setDataFormat(hssfDataFormat.getFormat(formatdataCellRate));
        cellStylenumRATEbold.setAlignment(HorizontalAlignment.RIGHT);
        cellStylenumRATEbold.setBorderTop(BorderStyle.THIN);
        cellStylenumRATEbold.setBorderBottom(BorderStyle.THIN);

        XSSFSheet recap = workbook.createSheet("RECAP");
        AtomicDouble totalmac_equivalent = new AtomicDouble(0.0);
        AtomicDouble totalmac_delta = new AtomicDouble(0.0);

        list_siq.forEach(siq -> {

            XSSFSheet sheet = workbook.createSheet(siq.getId_filiale());

            Row rowP = sheet.createRow((short) 1);

            Cell cl = rowP.createCell(1);
            cl.setCellStyle(style1);
            cl.setCellValue(intestazionePdf + " " + datereport);

            Row row = sheet.createRow((short) 3);
            row.createCell(1).setCellValue(siq.getId_filiale() + " " + siq.getDe_filiale());

            Row row66 = sheet.createRow((short) 7);

            int cntriga = 7;

            Cell f2 = row66.createCell(1);
            f2.setCellStyle(style3left);
            f2.setCellValue("Type");

            Cell f3 = row66.createCell(2);
            f3.setCellStyle(style3);
            f3.setCellValue("Operation");

            Cell f4 = row66.createCell(3);
            f4.setCellStyle(style3left);
            f4.setCellValue("Date");

            Cell f5 = row66.createCell(4);
            f5.setCellStyle(style3);
            f5.setCellValue("Historical BCE Rate");
            Cell f6 = row66.createCell(5);
            f6.setCellStyle(style3);
            f6.setCellValue("Quantity");

            Cell f7 = row66.createCell(6);
            f7.setCellStyle(style3);
            f7.setCellValue("Historical BCE Equivalent");
            Cell f71 = row66.createCell(7);
            f71.setCellStyle(style3);
            f71.setCellValue("Delta Equivalent");

            // Popolo la tabella
            ArrayList<StockPrice_value> dati = siq.getDati();

            ArrayList<String> qtyValuta = new ArrayList<>();
            ArrayList<String> rateValuta = new ArrayList<>();
            ArrayList<String> rateSupporto = new ArrayList<>();

            ArrayList<String> controvaloreValuta = new ArrayList<>();
            ArrayList<String> totalefinalecontrovalore = new ArrayList<>();
            ArrayList<String> qtySupporto = new ArrayList<>();
            ArrayList<String> controvaloreSupporto = new ArrayList<>();
            ArrayList<String> deltaValuta = new ArrayList<>();
            ArrayList<String> deltaSupporto = new ArrayList<>();
            ArrayList<String> deltaFinale = new ArrayList<>();

            boolean firstime = true;

            for (int i = 0; i < dati.size(); i++) {

                cntriga++;
                cntriga++;
                Row row6 = sheet.createRow((short) cntriga);

                StockPrice_value actual = dati.get(i);
                StockPrice_value prossimo;

                if (i == dati.size() - 1) {
                    prossimo = dati.get(i);
                } else {

                    prossimo = dati.get(i + 1);
                }

                if (firstime) {
                    firstime = false;

                    Cell f1bis = row6.createCell(1);
                    f1bis.setCellStyle(style3left);
                    f1bis.setCellValue(actual.getCurrency());

                    Cell f2bis = row6.createCell(2);
                    f2bis.setCellStyle(style3left);
                    f2bis.setCellValue(actual.getDe_currency());

                    Cell fn1 = row6.createCell(3);
                    fn1.setCellStyle(style3left);
                    fn1.setCellValue(actual.getDateactual());
                    Cell fn2 = row6.createCell(4);
                    fn2.setCellStyle(cellStylenumRATEbold);
                    fn2.setCellValue(fd(actual.getHistoricalBCE()));

                    cntriga++;
                    Row row77 = sheet.createRow((short) cntriga);

                    Cell f10 = row77.createCell(1);
                    f10.setCellStyle(style3left);
                    f10.setCellValue(actual.getSupportodesc());

                    Cell f11 = row77.createCell(2);
                    f11.setCellStyle(style4);
                    f11.setCellValue("");

                    Cell f12 = row77.createCell(3);
                    f12.setCellStyle(style4);
                    f12.setCellValue("");

                }

                if (actual.getCurrency().equals(prossimo.getCurrency())) {

                    cntriga++;
                    Row row88 = sheet.createRow((short) cntriga);

                    qtyValuta.add(actual.getQuantitaSenzaFormattazione());
                    controvaloreValuta.add(actual.getControvaloreSenzaFormattazione());
                    totalefinalecontrovalore.add(actual.getControvaloreSenzaFormattazione());

                    qtySupporto.add(actual.getQuantitaSenzaFormattazione());
                    controvaloreSupporto.add(actual.getControvaloreSenzaFormattazione());

                    deltaValuta.add(actual.getDeltaEquivalent());
                    deltaSupporto.add(actual.getDeltaEquivalent());
                    deltaFinale.add(actual.getDeltaEquivalent());

                    rateValuta.add(actual.getCambio());
                    rateSupporto.add(actual.getCambio());

                    Cell f20 = row88.createCell(1);
                    f20.setCellStyle(style4left);
                    f20.setCellValue(actual.getBcanconote1());

                    Cell f21 = row88.createCell(2);
                    f21.setCellStyle(style4);
                    f21.setCellValue(actual.getSupportovalue());

                    Cell f22 = row88.createCell(3);
                    f22.setCellStyle(style4left);
                    f22.setCellValue(actual.getBanconote2());

                    Cell f23 = row88.createCell(4);
                    f23.setCellStyle(cellStylenumRATE);
                    f23.setCellValue(fd(actual.getCambio()));

                    Cell f24 = row88.createCell(5);
                    f24.setCellStyle(style4num);
                    f24.setCellValue(fd(actual.getQuantita()));

                    Cell f25 = row88.createCell(6);
                    f25.setCellStyle(style4num);
                    f25.setCellValue(fd(actual.getControvalore()));

                    Cell f25a = row88.createCell(7);
                    f25a.setCellStyle(style4num);
                    f25a.setCellValue(fd(actual.getDeltaEquivalent()));

                    if (!actual.getSupportocod().equals(prossimo.getSupportocod())) {
                        // se la valuta è uguale ma il supporto è diverso

                        cntriga++;
                        Row row99 = sheet.createRow((short) cntriga);

                        double totqty = 0;
                        double totcontrovalore = 0;
                        double totrate = 0;
                        double totdelta = 0;

                        for (int k = 0; k < qtySupporto.size(); k++) {
                            totrate += (fd(rateSupporto.get(k)) * fd(qtySupporto.get(k)));
                            totqty += fd(qtySupporto.get(k));
                            totcontrovalore += fd(controvaloreSupporto.get(k));
                            totdelta += fd(deltaSupporto.get(k));
                        }

                        totqty = roundDouble(totqty, 2);
                        totcontrovalore = roundDouble(totcontrovalore, 2);
                        totdelta = roundDouble(totdelta, 2);
                        if (totqty != 0) {
                            totrate = roundDouble(totrate / totqty, 8);
                        } else {
                        }

                        Cell f30 = row99.createCell(1);
                        f30.setCellStyle(style3);
                        f30.setCellValue("Totale Supporto " + actual.getSupportocod());

                        Cell f31 = row99.createCell(4);
                        f31.setCellStyle(cellStylenumRATEbold);
                        f31.setCellValue(totrate);

                        Cell f31aa = row99.createCell(5);
                        f31aa.setCellStyle(style3num);
                        f31aa.setCellValue(totqty);

                        Cell f32 = row99.createCell(6);
                        f32.setCellStyle(style3num);
                        f32.setCellValue(totcontrovalore);

                        Cell f31a = row99.createCell(7);
                        f31a.setCellStyle(style3num);
                        f31a.setCellValue(totdelta);

                        qtySupporto.removeAll(qtySupporto);
                        deltaSupporto.removeAll(deltaSupporto);
                        rateSupporto.removeAll(rateSupporto);
                        controvaloreSupporto.removeAll(controvaloreSupporto);

                        cntriga++;
                        Row row100 = sheet.createRow((short) cntriga);

                        Cell f40 = row100.createCell(1);
                        f40.setCellStyle(style4left);
                        f40.setCellValue(prossimo.getSupportodesc());

                        Cell f41 = row100.createCell(2);
                        f41.setCellStyle(style4);
                        f41.setCellValue("");

                        Cell f42 = row100.createCell(3);
                        f42.setCellStyle(style4);
                        f42.setCellValue("");

                        // scriviIntestazioneSupporto(document,prossimo);
                    }

                } else {

                    firstime = true;
                    qtyValuta.add(actual.getQuantitaSenzaFormattazione());
                    rateValuta.add(actual.getCambio());
                    deltaValuta.add(actual.getDeltaEquivalent());
                    deltaFinale.add(actual.getDeltaEquivalent());

                    controvaloreValuta.add(actual.getControvaloreSenzaFormattazione());
                    totalefinalecontrovalore.add(actual.getControvaloreSenzaFormattazione());

                    qtySupporto.add(actual.getQuantitaSenzaFormattazione());
                    rateSupporto.add(actual.getCambio());
                    deltaSupporto.add(actual.getDeltaEquivalent());
                    controvaloreSupporto.add(actual.getControvaloreSenzaFormattazione());

                    cntriga++;
                    Row row88 = sheet.createRow((short) cntriga);

                    Cell f20 = row88.createCell(1);
                    f20.setCellStyle(style4left);
                    f20.setCellValue(actual.getBcanconote1());

                    Cell f21 = row88.createCell(2);
                    f21.setCellStyle(style4);
                    f21.setCellValue(actual.getSupportovalue());

                    Cell f22 = row88.createCell(3);
                    f22.setCellStyle(style4left);
                    f22.setCellValue(actual.getBanconote2());

                    Cell f23 = row88.createCell(4);
                    f23.setCellStyle(cellStylenumRATE);
                    f23.setCellValue(fd(actual.getCambio()));

                    Cell f24 = row88.createCell(5);
                    f24.setCellStyle(style4num);
                    f24.setCellValue(fd(actual.getQuantita()));

                    Cell f25 = row88.createCell(6);
                    f25.setCellStyle(style4num);
                    f25.setCellValue(fd(actual.getControvalore()));

                    Cell f25a = row88.createCell(7);
                    f25a.setCellStyle(style4num);
                    f25a.setCellValue(fd(actual.getDeltaEquivalent()));

                    cntriga++;
                    Row row99 = sheet.createRow((short) cntriga);

                    double totqty = 0;
                    double totcontrovalore = 0;
                    double totrate = 0;
                    double totdelta = 0;

                    for (int k = 0; k < qtySupporto.size(); k++) {
                        totrate += (fd(rateSupporto.get(k)) * fd(qtySupporto.get(k)));
                        totqty += fd(qtySupporto.get(k));
                        totcontrovalore += fd(controvaloreSupporto.get(k));
                        totdelta += fd(deltaSupporto.get(k));
                    }

                    totqty = roundDouble(totqty, 2);
                    totcontrovalore = roundDouble(totcontrovalore, 2);
                    totdelta = roundDouble(totdelta, 2);
                    if (totqty != 0) {
                        totrate = roundDouble(totrate / totqty, 8);
                    } else {
                    }

                    Cell f30 = row99.createCell(1);
                    f30.setCellStyle(style3);
                    f30.setCellValue("Totale Supporto " + actual.getSupportocod());

                    Cell f31 = row99.createCell(4);
                    f31.setCellStyle(cellStylenumRATEbold);
                    f31.setCellValue(totrate);

                    Cell f31aa = row99.createCell(5);
                    f31aa.setCellStyle(style3num);
                    f31aa.setCellValue(totqty);

                    Cell f32 = row99.createCell(6);
                    f32.setCellStyle(style3num);
                    f32.setCellValue(totcontrovalore);

                    Cell f31a = row99.createCell(7);
                    f31a.setCellStyle(style3num);
                    f31a.setCellValue(totdelta);

                    totqty = 0;
                    totcontrovalore = 0;
                    totdelta = 0;
                    totrate = 0;

                    for (int k = 0; k < qtyValuta.size(); k++) {

                        totrate += (fd(rateValuta.get(k)) * fd(qtyValuta.get(k)));
                        totqty += fd(qtyValuta.get(k));
                        totcontrovalore += fd(controvaloreValuta.get(k));
                        totdelta += fd(deltaValuta.get(k));
                    }

                    totqty = Utility.roundDouble(totqty, 2);
                    totcontrovalore = Utility.roundDouble(totcontrovalore, 2);
                    totdelta = Utility.roundDouble(totdelta, 2);
                    if (totqty != 0) {
                        totrate = Utility.roundDouble(totrate / totqty, 8);
                    } else {
                    }

                    cntriga++;
                    Row row100 = sheet.createRow((short) cntriga);

                    Cell f40 = row100.createCell(1);
                    f40.setCellStyle(style3);
                    f40.setCellValue("Totale Valuta " + actual.getCurrency());

                    Cell f31as = row100.createCell(4);
                    f31as.setCellStyle(cellStylenumRATEbold);
                    f31as.setCellValue(totrate);

                    Cell f41 = row100.createCell(5);
                    f41.setCellStyle(style3num);
                    f41.setCellValue(totqty);

                    Cell f42 = row100.createCell(6);
                    f42.setCellStyle(style3num);
                    f42.setCellValue(totcontrovalore);

                    Cell f31aaa = row100.createCell(7);
                    f31aaa.setCellStyle(style3num);
                    f31aaa.setCellValue(totdelta);

                    // scriviTotaleValuta(qtyValuta,controvaloreValuta,actual,document);
                    qtyValuta.removeAll(qtyValuta);
                    rateValuta.removeAll(rateValuta);
                    deltaValuta.removeAll(deltaValuta);
                    controvaloreValuta.removeAll(controvaloreValuta);

                    qtySupporto.removeAll(qtySupporto);
                    controvaloreSupporto.removeAll(controvaloreSupporto);
                    rateSupporto.removeAll(rateSupporto);
                    deltaSupporto.removeAll(deltaSupporto);

                }

                if (i == dati.size() - 1) {// sono alla fine

                    cntriga++;

                    cntriga++;
                    Row row99 = sheet.createRow((short) cntriga);

                    double totqty = 0;
                    double totcontrovalore = 0;
                    double totdelta = 0;
                    for (int k = 0; k < qtySupporto.size(); k++) {

                        totqty += fd(qtySupporto.get(k));
                        totcontrovalore += fd(controvaloreSupporto.get(k));
                        totdelta += fd(deltaSupporto.get(k));
                    }

                    Cell f30 = row99.createCell(1);
                    f30.setCellStyle(style3);
                    f30.setCellValue("Totale Supporto " + actual.getSupportocod());

                    Cell f31 = row99.createCell(5);
                    f31.setCellStyle(style3num);
                    f31.setCellValue(totqty);

                    Cell f32 = row99.createCell(6);
                    f32.setCellStyle(style3);
                    f32.setCellValue(totcontrovalore);

                    Cell f31a = row99.createCell(7);
                    f31a.setCellStyle(style3num);
                    f31a.setCellValue(totdelta);

                    // scriviTotaleSupporto(qtySupporto,controvaloreSupporto,actual,document);
                    totqty = 0;
                    totcontrovalore = 0;
                    totdelta = 0;
                    for (int k = 0; k < qtyValuta.size(); k++) {

                        totqty += fd(qtyValuta.get(k));
                        totcontrovalore += fd(controvaloreValuta.get(k));
                        totdelta += fd(deltaValuta.get(k));
                    }

                    cntriga++;
                    Row row100 = sheet.createRow((short) cntriga);

                    Cell f40 = row100.createCell(1);
                    f40.setCellStyle(style3);
                    f40.setCellValue("Totale Valuta " + actual.getCurrency());

                    Cell f41 = row100.createCell(5);
                    f41.setCellStyle(style3num);
                    f41.setCellValue(totqty);

                    Cell f42 = row100.createCell(6);
                    f42.setCellStyle(style3num);
                    f42.setCellValue(totcontrovalore);

                    Cell f31aa = row100.createCell(7);
                    f31aa.setCellStyle(style3num);
                    f31aa.setCellValue(totdelta);

                    // scriviTotaleValuta(qtyValuta,controvaloreValuta,actual,document);
                    qtyValuta.removeAll(qtyValuta);
                    controvaloreValuta.removeAll(controvaloreValuta);
                    rateValuta.removeAll(rateValuta);
                    deltaValuta.removeAll(deltaValuta);

                    qtySupporto.removeAll(qtySupporto);
                    controvaloreSupporto.removeAll(controvaloreSupporto);
                    rateSupporto.removeAll(rateSupporto);
                    deltaSupporto.removeAll(deltaSupporto);
                    cntriga++;
                    cntriga++;
                }

            }

            // qui scrivo l'ultima riga con la somma totale del controvalore
            cntriga++;
            cntriga++;
            Row row7 = sheet.createRow((short) cntriga);

            Cell f18 = row7.createCell(1);
            f18.setCellStyle(style3);
            f18.setCellValue("Totale Filiale" + siq.getId_filiale());

            double totfincontrovalore = 0;
            for (int k = 0; k < totalefinalecontrovalore.size(); k++) {
                totfincontrovalore += fd(totalefinalecontrovalore.get(k));
            }

            double totFindelta = 0;
            for (int k = 0; k < deltaFinale.size(); k++) {
                totFindelta += fd(deltaFinale.get(k));
            }

            Cell f19 = row7.createCell(6);
            f19.setCellStyle(style3num);
            f19.setCellValue(totfincontrovalore);

            Cell f19a = row7.createCell(7);
            f19a.setCellStyle(style3num);
            f19a.setCellValue(totFindelta);

            sheet.autoSizeColumn(0, true);
            sheet.autoSizeColumn(1, true);
            sheet.autoSizeColumn(2, true);
            sheet.autoSizeColumn(3, true);
            sheet.autoSizeColumn(4, true);
            sheet.autoSizeColumn(5, true);
            sheet.autoSizeColumn(6, true);
            sheet.autoSizeColumn(7, true);
            sheet.autoSizeColumn(8, true);
            sheet.autoSizeColumn(9, true);
            sheet.autoSizeColumn(10, true);
            sheet.autoSizeColumn(11, true);
            sheet.autoSizeColumn(12, true);
            sheet.autoSizeColumn(13, true);
            sheet.autoSizeColumn(14, true);

            totalmac_equivalent.addAndGet(totfincontrovalore);
            totalmac_delta.addAndGet(totFindelta);

            siq.setEquivalent(totfincontrovalore);
            siq.setDelta(totFindelta);

        });

        // PRIMO FOGLIO
        Row rowP = recap.createRow((short) 1);

        Cell cl = rowP.createCell(1);
        cl.setCellStyle(style1);
        cl.setCellValue(intestazionePdf + " " + datereport + " - RECAP");

        Row row66 = recap.createRow((short) 7);

        AtomicInteger cntriga = new AtomicInteger(9);

        Cell f2 = row66.createCell(1);
        f2.setCellStyle(style1);
        f2.setCellValue("BRANCH");

        Cell f7 = row66.createCell(2);
        f7.setCellStyle(style1);
        f7.setCellValue("Historical BCE Equivalent");
        Cell f71 = row66.createCell(3);
        f71.setCellStyle(style1);
        f71.setCellValue("Delta Equivalent");

        list_siq.forEach(siq -> {
            Row row99 = recap.createRow((short) cntriga.get());
            Cell f30 = row99.createCell(1);
            f30.setCellStyle(style3left);
            f30.setCellValue(siq.getId_filiale() + " " + siq.getDe_filiale());

            Cell f32 = row99.createCell(2);
            f32.setCellStyle(style3num);
            f32.setCellValue(siq.getEquivalent());

            Cell f31a = row99.createCell(3);
            f31a.setCellStyle(style3num);
            f31a.setCellValue(siq.getDelta());
            cntriga.addAndGet(1);
        });
        cntriga.addAndGet(1);
        Row row99 = recap.createRow((short) cntriga.get());
        Cell f30 = row99.createCell(1);
        f30.setCellStyle(style3left);
        f30.setCellValue("TOTAL SYSTEM");

        Cell f32 = row99.createCell(2);
        f32.setCellStyle(style3num);
        f32.setCellValue(totalmac_equivalent.get());

        Cell f31a = row99.createCell(3);
        f31a.setCellStyle(style3num);
        f31a.setCellValue(totalmac_delta.get());

        recap.autoSizeColumn(0, true);
        recap.autoSizeColumn(1, true);
        recap.autoSizeColumn(2, true);
        recap.autoSizeColumn(3, true);

        try (FileOutputStream out = new FileOutputStream(pdf)) {
            workbook.write(out);
            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(pdf)));
            System.out.println(pdf.getPath());
            // pdf.delete(); // Se desideri eliminare il file dopo aver ottenuto la stringa
            // Base64, rimuovi il commento a questa linea
            return base64;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
