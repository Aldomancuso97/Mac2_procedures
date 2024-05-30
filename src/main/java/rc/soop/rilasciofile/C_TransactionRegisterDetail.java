/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

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
import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author fplacanica
 */
public class C_TransactionRegisterDetail {

    // column
    /**
     *
     */
    public static final float[] columnWidths0 = new float[] { 70f, 30f };

    /**
     *
     */
    public static final float[] columnWidths1 = new float[] { 60f, 30f };

    /**
     *
     */
    public static float[] columnWidths2 = new float[] { 20f, 10f, 10f, 15f, 50f, 15f, 40f, 30f, 30f, 30f, 25f, 25f, 25f,
            25f };

    /**
     *
     */
    public static float[] columnWidths3 = new float[] { 5f, 20f, 50f, 10f, 10f, 15f, 10f, 10f };

    /**
     *
     */
    public static float[] columnWidths4 = new float[] { 30f, 20f, 15f, 15f, 10f, 15f };

    /**
     *
     */
    public static float[] columnWidths5 = new float[] { 30f, 20f, 15f, 15f, 10f, 15f };

    /**
     *
     */
    public static float[] columnWidths6 = new float[] { 30f, 20f, 15f, 15f, 10f, 15f };

    public static final String intestazionePdf = "Maccorp Italiana S.p.A. P.I. 12951210157 - Registro Transazioni";
    Phrase vuoto = new Phrase("\n");

    Color c = new Color(23, 32, 43);

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
     ** Constructor
     */
    public C_TransactionRegisterDetail() {

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
     * @param path
     * @param cmfb
     * @param colonne
     * @param progressivostart
     * @param pagestart
     * @return
     */
    public String receipt_2020(String path, C_TransactionRegisterDetail_value cmfb, ArrayList<String> colonne,
            int progressivostart, int pagestart) {
        File pdffile = new File(path + Utility.generaId() + "C_TransactionRegisterDetail.pdf");
        try (
                OutputStream ou = new FileOutputStream(pdffile)) {
            Document document = new Document(PageSize.A4, 20, 20, 20, 20);
            PdfWriter wr = PdfWriter.getInstance(document, ou);
            document.open();
            ArrayList<C_TransactionRegisterDetail_value> dati = cmfb.getDati();
            String anno = StringUtils.substring(dati.get(0).getDate().split(" ")[0], 6);
            PdfPTable table0 = new PdfPTable(2);
            table0.setWidths(columnWidths0);
            table0.setWidthPercentage(100);
            Phrase phrase1 = new Phrase();
            phrase1.add(new Chunk(intestazionePdf, f3_bold));
            PdfPCell cell1 = new PdfPCell(phrase1);
            cell1.setBorder(Rectangle.NO_BORDER);
            Paragraph pa1 = new Paragraph(
                    new Phrase("Pagina " + anno + " / " + String.format("%d", pagestart + 1), f3_bold));
            pa1.setAlignment(Element.ALIGN_RIGHT);
            PdfPCell cell2 = new PdfPCell(pa1);
            cell2.setBorder(Rectangle.NO_BORDER);
            cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);

            Phrase phrase4 = new Phrase();
            phrase4.add(new Chunk("", f3_normal));
            PdfPCell cell4 = new PdfPCell(phrase4);
            cell4.setBorder(Rectangle.NO_BORDER);
            table0.addCell(cell1);
            table0.addCell(cell2);
            table0.addCell(cell4);
            document.add(table0);
            vuoto.setFont(f3_normal);
            document.add(vuoto);

            int startrow = 1;

            if (dati.size() >= startrow) {

                Phrase phraset;
                PdfPCell cellt;
                PdfPTable table3;

                LineSeparator sep = new LineSeparator();
                sep.setOffset(-2);
                sep.setLineWidth((float) 0.5);

                PdfPTable table4 = new PdfPTable(2);
                table4.setWidths(columnWidths0);
                table4.setWidthPercentage(100);

                phraset = new Phrase();
                phraset.add(new Chunk("", f3_bold));
                cellt = new PdfPCell(phraset);
                cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                cellt.setBorder(Rectangle.BOTTOM);
                table4.addCell(cellt);

                phraset = new Phrase();
                phraset.add(new Chunk("", f4_bold));
                cellt = new PdfPCell(phraset);
                cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                cellt.setBorder(Rectangle.BOTTOM);
                table4.addCell(cellt);

                document.add(table4);

                PdfPTable table2 = new PdfPTable(colonne.size());
                table2.setWidths(columnWidths2);
                table2.setWidthPercentage(100);

                PdfPCell[] list = new PdfPCell[colonne.size()];
                // mi scandisco le colonne
                for (int c = 0; c < colonne.size(); c++) {
                    Phrase phraset1 = new Phrase();
                    phraset1.add(new Chunk(colonne.get(c), f4_bold));
                    PdfPCell cellt1 = new PdfPCell(phraset1);
                    cellt1.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt1.setBorder(Rectangle.BOTTOM | Rectangle.TOP);
                    cellt1.setFixedHeight(20f);
                    cellt1.setBackgroundColor(BaseColor.LIGHT_GRAY);
                    // cellt1.setBorderWidth(0.7f);
                    if (c == 4 || c == 5 || c == 6) {
                        cellt1.setHorizontalAlignment(Element.ALIGN_LEFT);
                    }
                    list[c] = cellt1;
                }

                table3 = new PdfPTable(colonne.size());
                table3.setWidths(columnWidths2);
                table3.setWidthPercentage(100);

                for (int z = 0; z < list.length; z++) {
                    PdfPCell temp = (PdfPCell) (list[z]);
                    table3.addCell(temp);
                }

                document.add(table3);

                float availableSpace;

                table4 = new PdfPTable(14);
                table4.setWidths(columnWidths2);
                table4.setWidthPercentage(100);

                // for (int j = startrow - 1; j < dati.size(); j++) {
                for (int j = 0; j < dati.size(); j++) {

                    availableSpace = wr.getVerticalPosition(true) - document.bottomMargin();

                    C_TransactionRegisterDetail_value actual = (C_TransactionRegisterDetail_value) dati.get(j);

                    table4 = new PdfPTable(14);
                    table4.setWidths(columnWidths2);
                    table4.setWidthPercentage(100);

                    phraset = new Phrase();
                    phraset.add(new Chunk(String.valueOf(progressivostart + 1), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getFiliale(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getTill(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getUser(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getDate(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getCur(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(actual.getKind(), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_LEFT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getAmountqty()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getRate()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getTotal()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getPerc()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getCommfee()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getRefundoff()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    phraset = new Phrase();
                    phraset.add(new Chunk(formatMysqltoDisplay(actual.getPayinout()), f3_normal));
                    cellt = new PdfPCell(phraset);
                    cellt.setHorizontalAlignment(Element.ALIGN_RIGHT);
                    cellt.setBorder(Rectangle.BOTTOM);
                    table4.addCell(cellt);

                    document.add(table4);

                    if ((availableSpace < 40) || (j == (dati.size() - 1))) {
                        pagestart++;
                        document.newPage();
                        if ((j != (dati.size() - 1))) {
                            table0 = new PdfPTable(2);
                            table0.setWidths(columnWidths0);
                            table0.setWidthPercentage(100);
                            phrase1 = new Phrase();
                            phrase1.add(new Chunk(intestazionePdf, f3_bold));
                            cell1 = new PdfPCell(phrase1);
                            cell1.setBorder(Rectangle.NO_BORDER);
                            pa1 = new Paragraph(
                                    new Phrase("Pagina " + anno + " / " + String.format("%d", pagestart + 1), f3_bold));
                            pa1.setAlignment(Element.ALIGN_RIGHT);
                            cell2 = new PdfPCell(pa1);
                            cell2.setBorder(Rectangle.NO_BORDER);
                            cell2.setHorizontalAlignment(Element.ALIGN_RIGHT);
                            phrase4 = new Phrase();
                            phrase4.add(new Chunk("", f3_normal));
                            cell4 = new PdfPCell(phrase4);
                            cell4.setBorder(Rectangle.NO_BORDER);
                            table0.addCell(cell1);
                            table0.addCell(cell2);
                            table0.addCell(cell4);
                            document.add(table0);
                            vuoto.setFont(f3_normal);
                            document.add(vuoto);
                            document.add(table3);
                        }
                    }
                    progressivostart++;
                }

            }
            // chiusura documento
            document.close();
            wr.close();
            ou.close();

            String base64 = new String(Base64.encodeBase64(FileUtils.readFileToByteArray(pdffile)));
            // pdffile.delete();

            return base64;
        } catch (DocumentException | IOException ex) {
            ex.printStackTrace();
        }
        return null;

    }

}
