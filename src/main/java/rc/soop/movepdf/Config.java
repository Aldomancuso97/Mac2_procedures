/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.movepdf;

import com.itextpdf.text.pdf.PdfReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.rb;

public class Config {

    public static final String startfile = "FILE[";
    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";
    public static final String apptype = "application/json";
    public static final String patternD1 = "yyyy-MM-dd";
    public static final String patternD2 = "yyMMddHHmmssSSS";
    public static final String ext1 = "txt";

    public static final Logger log = createLog("Mac2.0_MOVEPDF_", rb.getString("pdf.path.log"), pattern4);

    public static Logger createLog(String appname, String folderini, String patterndatefolder) {
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + dt.toString(pattern3) + ".log";
            File dirING = new File(folderini);
            dirING.mkdirs();
            if (patterndatefolder != null) {
                File dirINGNew = new File(dirING.getPath() + File.separator + dt.toString(patterndatefolder));
                dirINGNew.mkdirs();
                filename = dirINGNew.getPath() + File.separator + filename;
            } else {
                filename = dirING.getPath() + File.separator + filename;
            }
            Handler fileHandler = new FileHandler(filename);
            LOGGER.addHandler(fileHandler);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException ex) {
        }
        return LOGGER;
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static double fd(String si_t_old) {
        double d1;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (NumberFormatException e) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{e.getStackTrace()[0].getMethodName(), e.getMessage()});
            d1 = 0.0D;
        }
        return d1;
    }

    public static int parseIntR(String value) {
        value = value.replaceAll("-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        int d1;
        try {
            d1 = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{e.getStackTrace()[0].getMethodName(), e.getMessage()});
            d1 = 0;
        }
        return d1;
    }

    public static String generaToken(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString(patternD2) + random;
    }

    public static boolean checkPDF(File pdffile) {
        if (pdffile != null) {
            if (pdffile.exists()) {
                if (pdffile.getName().toLowerCase().endsWith(ext1)) {
                    return pdffile.canRead() && pdffile.length() > 0;
                }
                try {
                    int pag;
                    try (InputStream is = new FileInputStream(pdffile)) {
                        PdfReader pdfReader = new PdfReader(is);
                        pag = pdfReader.getNumberOfPages();
                        pdfReader.close();
                    }
                    return pag > 0;
                } catch (IOException ex) {
                }
            }
        }
        return false;
    }

}
