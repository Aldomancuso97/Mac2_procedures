/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.gs;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Config {

    private static final ResourceBundle rb = ResourceBundle.getBundle("gs.conf");
    
    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";
    public static final String apptype = "application/json";
    public static final String patternD1 = "yyyy-MM-dd";
    public static final String link_TEST = rb.getString("link_TEST");
    public static final String link_PROD = rb.getString("link_PROD");
    public static final String codiceTenant = rb.getString("codiceTenant");
    public static final String categoria = rb.getString("categoria");
    public static final String usernameWS = rb.getString("usernameWS");
    public static final String passwordWS = rb.getString("passwordWS");
    
    public static final int totalDays = Integer.parseInt(rb.getString("totalDays"));

    public static final Logger log = createLog("Mac2.0_GS_", rb.getString("path.log"), pattern4);

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

    public static void printJSONObject(JSONObject obj) {
        try {
            Iterator i = obj.keys();
            log.warning("PRINT JSON OBJECT:");
            while (i.hasNext()) {
                String v = i.next().toString();
                if (obj.get(v) instanceof JSONArray) {
                    log.log(Level.WARNING, "name: {0} - type: JSONArray - Size: {1}", new Object[]{v, obj.getJSONArray(v).length()});
                } else if (obj.get(v) instanceof Boolean) {
                    log.log(Level.WARNING, "name: {0} - type: Boolean - Value: {1}", new Object[]{v, obj.getBoolean(v)});
                } else if (obj.get(v) instanceof String) {
                    log.log(Level.WARNING, "name: {0} - type: String - Value: {1}", new Object[]{v, obj.getString(v)});
                } else {
                    log.log(Level.WARNING, "name: {0} - CLASS: {1}", new Object[]{v, obj.get(v).getClass()});
                }
            }
        } catch (JSONException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
    }

}
