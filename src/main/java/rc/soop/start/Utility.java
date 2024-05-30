/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import static rc.soop.start.Central_Branch.host_h;
import static rc.soop.start.Central_Branch.pwd_h;
import static rc.soop.start.Central_Branch.user_h;

/**
 *
 * @author rcosco
 */
public class Utility {

    public static final String comma = ",";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String pattern1 = "dd/MM/yyyy HH:mm:ss";
    public static final String pattern2 = "yyyy_MM_dd_HH_mm_ss";
    public static final String pattern3 = "HHmmssSSS";
    public static final String pattern4 = "yyyyMMdd";
    public static final String iptime = "193.204.114.232";
    
    public static final ResourceBundle rb = ResourceBundle.getBundle("start.conf");

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

    public static String formatType(String sql) {
        if (sql.toUpperCase().startsWith("INSERT")) {
            return "INS";
        } else if (sql.toUpperCase().startsWith("DELETE")) {
            return "DEL";
        } else if (sql.toUpperCase().startsWith("UPDATE")) {
            return "UPD";
        }
        return "";
    }

    public static DateTime parseStringDate(String dat, String pattern1) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                return formatter.parseDateTime(dat);
            }
        } catch (Exception localException) {
        }
        return null;
    }

    public static String generaId() {
        String random = RandomStringUtils.randomAlphanumeric(5).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String formatStringtoStringDate(String dat, String pattern1, String pattern2, Logger log) {
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
            log.severe(ex.getMessage());
        }
        return null;
    }

    public static String generateUsername(String nome, String cognome, String cod) {
        nome = StringUtils.stripAccents(nome).replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
        cognome = StringUtils.stripAccents(cognome).replaceAll("[^a-zA-Z0-9]", "").replaceAll(" ", "").toLowerCase();
        String result;
        if (nome.length() > 1) {
            result = nome.substring(0, 1);
        } else {
            result = RandomStringUtils.randomAlphabetic(1).toLowerCase();
        }
        result = result + cognome + cod;
        return result;
    }

    public static void printAL(ArrayList l) {
        Iterator p = l.iterator();
        while (p.hasNext()) {
            Object op = p.next();
            System.out.println(op.toString());
        }
    }

    public static double fd(String si_t_old) {
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            return Double.parseDouble(si_t_old);
        } catch (NumberFormatException e) {
            return 0.0D;
        }
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static String formatMysqltoDisplay(String ing) {
        String decimal = ",";
        String thousand = ".";
        if (host_h.contains("uk")) {
            decimal = ".";
            thousand = ",";
        }

        if (ing == null) {
            return "";
        }
        if (ing.trim().equals("") || ing.trim().equals("-")) {
            return "";
        }
        if (ing.length() == 0) {
            return "";
        }
        if (ing.trim().startsWith(".") || ing.trim().startsWith(",")) {
            return "0" + decimal + "00";
        }

        String start = ing.substring(0, 1);
        if (start.equals("-") || start.equals("+")) {
            ing = ing.replaceAll(start, "");
        } else {
            start = "";
        }

        String out = "";
        if (ing.contains(",")) {
            ing = ing.replaceAll(",", "");
        }
        if (ing.contains(".")) {
            String[] inter1 = splitStringEvery(ing.split("\\.")[0], 3);
            if (inter1.length > 1) {
                for (int i = 0; i < inter1.length; i++) {
                    out = out + inter1[i] + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
            String dec = ing.split("\\.")[1];
            out = out + decimal + dec;
        } else {

            String[] inter1 = splitStringEvery(ing, 3);
            if (inter1.length > 1) {
                for (int i = 0; i < inter1.length; i++) {
                    out = out + inter1[i] + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
        }
        return start + out;
    }

    public static String[] splitStringEvery(String s, int interval) {
        int arrayLength = (int) Math.ceil(s.length() / (double) interval);
        String[] result = new String[arrayLength];
        int endIndex = s.length();
        
        for (int i = arrayLength - 1; i >= 0; i--) {
            int startIndex = Math.max(endIndex - interval, 0);
            result[i] = s.substring(startIndex, endIndex);
            endIndex -= interval;
        }
        
        return result;
    }

    public static int parseIntR(String value) {
        value = value.replaceAll("-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static DateTime getTime(Logger log) {
        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        DateTime time = db.now();
        db.closeDB();
        return time;
    }

}
