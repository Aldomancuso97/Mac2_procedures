/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.spreadexcel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class Utility {
    public static final String pattern3 = "HHmmssSSS";
    public static final String pattern4 = "yyyyMMdd";
    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternnormdate_filter = "dd/MM/yyyy";
    
    public static final ResourceBundle rb = ResourceBundle.getBundle("spreadexcel.conf");
    
    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }
    
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
        } catch (IllegalArgumentException ex) {
        }
        return dat;
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
    }

    public static String formatIndex(ArrayList<String> valori) {
        String out = "";
        String start = "(";
        String end = ")";
        for (int i = 0; i < valori.size(); i++) {
            out = out + "`" + valori.get(i) + "`, ";
        }
        out = out.trim();
        if (out.contains(",")) {
            out = out.substring(0, out.length() - 1);
        }
        return start + out + end;
    }

    public static String[] formatAL(String cod, ArrayList<String[]> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i)[0])) {
                return array.get(i);
            }
        }
        return null;
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
}
