/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.maintenance.Monitor;

/**
 *
 * @author rcosco
 */
public class Util {

    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternita = "dd/MM/yyyy";
    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";
    public static final Logger log = createLog("Mac2.0_RIALLINEA", Monitor.rb.getString("path.log"), pattern4);
    public static final String patternsqlcomplete = "yyyy-MM-dd HH:mm:SS";

    public static String visualizzaStringaMySQL(String ing) {
        if (ing == null) {
            return "";
        }
        ing = StringUtils.replace(ing, "\\'", "'");
        ing = StringUtils.replace(ing, "\'", "'");
        ing = StringUtils.replace(ing, "\"", "'");
        //ing = StringUtils.replace(ing, "€", "&#0128;");
        return ing.trim();
    }

    public static String formatStringtoStringDate(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return dat;
    }

    public static double roundDouble(double d, int scale) {
        d = new BigDecimal(d).setScale(scale, RoundingMode.HALF_UP).doubleValue();
        return d;
    }

    public static double getControvalore(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return primo * secondo;
        }
    }

    public static double getControvaloreOFFSET(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return secondo / primo;
        }
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static double fd(String si_t_old) {
        double d1;
        try {
            si_t_old = si_t_old.replace(",", "").trim();
            if (si_t_old.endsWith(".")) {
                si_t_old = StringUtils.removeEnd(si_t_old, ".");
            }
            d1 = Double.parseDouble(si_t_old);
        } catch (Exception ex) {
            d1 = 0.0D;
            System.out.println(si_t_old + " (FD ERR:) " + ExceptionUtils.getStackTrace(ex));
            //ex.printStackTrace();
        }
        return d1;
    }

    public static Logger createLog(String appname, String folderini, String patterndatefolder) {
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + "-" + dt.toString(pattern3) + ".log";
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
        } catch (IOException localIOException) {
        }

        return LOGGER;
    }

    public static String estraiEccezione(Exception ec1) {
        try {
            String stack_nam = ec1.getStackTrace()[0].getMethodName();
            String stack_msg = ExceptionUtils.getStackTrace(ec1);
            return stack_nam + " - " + stack_msg;
        } catch (Exception e) {
        }
        return ec1.getMessage();

    }
}
