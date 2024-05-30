/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import static rc.soop.crm.CRM_batch.rb;
import static rc.soop.crm.CRM_batch.test;
import static rc.soop.crm.MailObject.mailerror_Cliente;
import static rc.soop.esolver.ESolver.sanitizeInput;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author rcosco
 */
public class Action {

    public static final String pat1 = "dd/MM/yyyy HH:mm:ss";
    public static final String pat2 = "yyyy-MM-dd HH:mm:ss";
    public static final String pat3 = "dd/MM/yyyy";
    public static final String pat4 = "yyyy-MM-dd";
    public static final String pat5 = "yyyyMMdd";
    public static final String pat6 = "HHmmssSSS";
    public static final String pat7 = "dd-MM-yyyy";

    public static final String pat8 = "yyMMddHHmmssSSS";

    public static final Logger log = createLog(rb.getString("path.log"), pat5);

    private static Logger createLog(String folderini, String patterndatefolder) {
        String appname = "Mac2.0_CRMPROD_";
        if (test) {
            appname = "Mac2.0_CRMTEST_";
        }
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + dt.toString(pat6) + ".log";
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

    public static Booking getBookingbyCod(String cod) {
        Database db = new Database();
        Booking out = db.getBookingbyCod(cod);
        db.closeDB();
        return out;
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
            log.log(Level.SEVERE, "formatStringtoStringDate {0}", ex.getMessage());
        }
        return dat;
    }

    public static String get_descr_Branch(String cod) {
        Database db = new Database();
        String es = db.get_descr_Branch(cod);
        db.closeDB();
        return es;
    }

    public static String formatMysqltoDisplay(String ing) {
        String decimal = ",";
        String thousand = ".";
    
        if (StringUtils.isBlank(ing) || ing.trim().equals("-")) {
            return "";
        }
    
        if (ing.trim().startsWith(".") || ing.trim().startsWith(",")) {
            return "0" + decimal + "00";
        }
    
        String start = StringUtils.left(ing, 1);
        if (start.equals("-") || start.equals("+")) {
            ing = StringUtils.removeStart(ing, start);
        } else {
            start = "";
        }
    
        String out = "";
        ing = StringUtils.remove(ing, ",");
        if (ing.contains(".")) {
            String[] inter1 = splitStringEvery(StringUtils.substringBefore(ing, "."), 3);
            out = StringUtils.join(inter1, thousand);
            String dec = StringUtils.substringAfter(ing, ".");
            out = out + decimal + dec;
        } else {
            String[] inter1 = splitStringEvery(ing, 3);
            out = StringUtils.join(inter1, thousand);
        }
        return start + out;
    }

    public static String[] splitStringEvery(String s, int interval) {
        int arrayLength = (int) Math.ceil(((s.length() / (double) interval)));
        String[] result = new String[arrayLength];
        int j = 0; 
        for (int i = 0; i < arrayLength; i++) {
            if (j + interval <= s.length()) {
                result[i] = s.substring(j, j + interval);
            } else {
                result[i] = s.substring(j);
            }
            j += interval; 
        }
        return result;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString(pat8) + random;
    }

    public static Settings get_Settings(String id) {
        Crm_Db db = new Crm_Db();
        Settings out = db.get_Settings(id);
        db.closeDB();
        return out;
    }

    // public static Preauth POSTRequest_GETBOOKING(String metod, String request,
    // String idprenotazione) {
    // Preauth output = new Preauth(idprenotazione);
    // output.setRequest(request);
    // try {
    // String linktest = "https://forexchangedev.moneym.eu/api/ext/" + metod;
    // String linkprod = "https://forexchange.it/api/ext/" + metod;
    //
    // String request_date = new DateTime().toString(pat2);
    // URL obj;
    // if (test) {
    // obj = new URL(linktest);
    // } else {
    // obj = new URL(linkprod);
    // }
    //
    // output.setRequest_date(request_date);
    // log.log(Level.INFO, "LINK REQUEST: {0}", obj.toString());
    // HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
    // postConnection.setRequestMethod("POST");
    // postConnection.setRequestProperty("userId", "a1bcdefgh");
    // postConnection.setRequestProperty("Content-Type", "application/json");
    // postConnection.setDoOutput(true);
    // try (OutputStream os = postConnection.getOutputStream()) {
    // os.write(request.getBytes());
    // os.flush();
    // }
    // int responseCode = postConnection.getResponseCode();
    // output.setResponse_code(String.valueOf(responseCode));
    // output.setResponse_message(postConnection.getResponseMessage());
    // if (responseCode == 200 || responseCode == 201) { StringBuilder response;
    // try ( //success
    // BufferedReader in = new BufferedReader(new InputStreamReader(
    // postConnection.getInputStream()))) {
    // String inputLine;
    // response = new StringBuilder();
    // while ((inputLine = in.readLine()) != null) {
    // response.append(inputLine);
    // }
    // }
    // log.log(Level.INFO, "RESPONSE PREAUTH: {0}", response.toString());
    // output.setResponse(response.toString());
    // output.setResponse_date(new DateTime().toString(pat2));
    // boolean result = Boolean.valueOf(getJsonString(response.toString(),
    // "result"));
    // boolean status = Boolean.valueOf(getJsonString(response.toString(),
    // "status"));
    // if (result && status) {
    // output.setStato("OK");
    // } else {
    // output.setStato("KO");
    // }
    // } else {
    // output.setResponse("ERROR");
    // output.setResponse_date(new DateTime().toString(pat2));
    // output.setStato("KO");
    // }
    // } catch (Exception ex) {
    // log.log(Level.SEVERE, "getBooking: {0}", ExceptionUtils.getStackTrace(ex));
    // output.setResponse("ERROR: " + ex.getMessage());
    // output.setResponse_date(new DateTime().toString(pat2));
    // output.setStato("KO");
    // }
    // return output;
    // }
    public static ApiResponse POSTRequestEDIT_CB(String json, String idprenotazione) {
        try {
            Crm_Db db = new Crm_Db();
            URL obj;
            if (test) {
                String ectValue = sanitizeInput(db.get_Settings("ECT").getValue());
                obj = new URL(ectValue);
            } else {
                String ecpValue = sanitizeInput(db.get_Settings("ECP").getValue());
                obj = new URL(ecpValue);
            }
            db.closeDB();

            log.info("POSTRequestEDIT CHEBANCA) " + sanitizeInput(obj.toString()));

            HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
            postConnection.setRequestMethod("POST");
            postConnection.setRequestProperty("userId", "a1bcdefgh");
            postConnection.setRequestProperty("Content-Type", "application/json");
            postConnection.setDoOutput(true);

            try (OutputStream os = postConnection.getOutputStream()) {
                os.write(json.getBytes());
                os.flush();
            }

            int responseCode = postConnection.getResponseCode();
            log.info("POSTRequestEDIT CHEBANCA) (RESPONSE CODE) " + responseCode);

            if (responseCode == 200) {
                StringBuilder response;
                try (BufferedReader in = new BufferedReader(new InputStreamReader(postConnection.getInputStream()))) {
                    String inputLine;
                    response = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }

                log.log(Level.INFO, "RESPONSE : {0}", sanitizeInput(response.toString()));
                Gson g = new Gson();
                return g.fromJson(response.toString(), ApiResponse.class);
            }

            log.info("POSTRequestEDIT CHEBANCA) RESPONSE ERROR: " + postConnection.getResponseCode());
            log.info("POSTRequestEDIT CHEBANCA) RESPONSE ERROR: " + sanitizeInput(postConnection.getResponseMessage()));
            mailerror_Cliente(idprenotazione);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "RESPONSE ERROR: {0}", ExceptionUtils.getStackTrace(ex));
            mailerror_Cliente(idprenotazione);
        }
        return null;
    }

    public static ApiResponse POSTRequestEDIT_WT(String json, String idprenotazione) {
        try {
            Crm_Db db = new Crm_Db();
            URL obj;
            if (test) {
                String ewtValue = sanitizeInput(db.get_Settings("EWT").getValue());
                obj = new URL(ewtValue);
            } else {
                String ewpValue = sanitizeInput(db.get_Settings("EWP").getValue());
                obj = new URL(ewpValue);
            }
            db.closeDB();

            log.info("POSTRequestEDIT WT) " + sanitizeInput(obj.toString()));

            HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
            postConnection.setRequestMethod("POST");
            postConnection.setRequestProperty("userId", "a1bcdefgh");
            postConnection.setRequestProperty("Content-Type", "application/json");
            postConnection.setDoOutput(true);

            try (OutputStream os = postConnection.getOutputStream()) {
                os.write(json.getBytes());
                os.flush();
            }

            int responseCode = postConnection.getResponseCode();
            log.info("POSTRequestEDIT WT) (RESPONSE CODE) " + responseCode);

            if (responseCode == 200) {
                StringBuilder response;
                try (BufferedReader in = new BufferedReader(new InputStreamReader(postConnection.getInputStream()))) {
                    String inputLine;
                    response = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }

                log.log(Level.INFO, "RESPONSE : {0}", sanitizeInput(response.toString()));
                Gson g = new Gson();
                return g.fromJson(response.toString(), ApiResponse.class);
            }

            log.info("POSTRequestEDIT WT) RESPONSE ERROR: " + postConnection.getResponseCode());
            log.info("POSTRequestEDIT WT) RESPONSE ERROR: " + sanitizeInput(postConnection.getResponseMessage()));
            mailerror_Cliente(idprenotazione);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "RESPONSE ERROR: {0}", ExceptionUtils.getStackTrace(ex));
            mailerror_Cliente(idprenotazione);
        }
        return null;
    }

    public static ApiResponse POSTRequestEDIT(String json, String idprenotazione) {
        try {
            Crm_Db db = new Crm_Db();
            URL obj;
            if (test) {
                String edtValue = sanitizeInput(db.get_Settings("EDT").getValue());
                obj = new URL(edtValue);
            } else {
                String edpValue = sanitizeInput(db.get_Settings("EDP").getValue());
                obj = new URL(edpValue);
            }
            db.closeDB();

            log.info("POSTRequestEDIT) " + sanitizeInput(obj.toString()));

            HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
            postConnection.setRequestMethod("POST");
            postConnection.setRequestProperty("userId", "a1bcdefgh");
            postConnection.setRequestProperty("Content-Type", "application/json");
            postConnection.setDoOutput(true);

            try (OutputStream os = postConnection.getOutputStream()) {
                os.write(json.getBytes());
                os.flush();
            }

            int responseCode = postConnection.getResponseCode();
            log.info("POSTRequestEDIT) (RESPONSE CODE) " + responseCode);

            if (responseCode == 200) {
                StringBuilder response;
                try (BufferedReader in = new BufferedReader(new InputStreamReader(postConnection.getInputStream()))) {
                    String inputLine;
                    response = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }

                log.log(Level.INFO, "RESPONSE : {0}", sanitizeInput(response.toString()));
                Gson g = new Gson();
                return g.fromJson(response.toString(), ApiResponse.class);
            }

            log.info("POSTRequestEDIT) RESPONSE ERROR: " + postConnection.getResponseCode());
            log.info("POSTRequestEDIT) RESPONSE ERROR: " + sanitizeInput(postConnection.getResponseMessage()));
            mailerror_Cliente(idprenotazione);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "RESPONSE ERROR: {0}", ExceptionUtils.getStackTrace(ex));
            mailerror_Cliente(idprenotazione);
        }
        return null;
    }

    public static String estraiServizi(Booking bo) {
        if (bo == null || bo.getServiziagg().trim().equals("")) {
            return "";
        }
        List<Items> servizi = servizi(null);
        StringBuilder seout = new StringBuilder("");
        List<String> se1 = Splitter.on(";").splitToList(bo.getServiziagg());
        if (!se1.isEmpty()) {
            se1.forEach(servizio -> {
                if (servizio.equals("")) {
                } else {
                    Items ag2 = servizi.stream().filter(br -> br.getCodice().equalsIgnoreCase(servizio))
                            .distinct().findFirst().orElse(new Items(servizio, servizio, "", "", "", "", "", ""));
                    seout.append(ag2.getDescrizione()).append(";");
                }
            });
        } else {
            seout.append("");
        }
        return seout.toString();
    }

    public static String estraiAgevolazioni(Booking bo) {
        if (bo == null || bo.getAgevolazioni().trim().equals("")) {
            return "";
        }
        StringBuilder agout = new StringBuilder("");
        List<Items> agevolazioni_varie = agevolazioni(null);
        List<String> ag1 = Splitter.on(";").splitToList(bo.getAgevolazioni());
        if (!ag1.isEmpty()) {
            ag1.forEach(agevolazione -> {
                Items ag2 = agevolazioni_varie.stream().filter(br -> br.getCodice().equalsIgnoreCase(agevolazione))
                        .distinct().findFirst().orElse(new Items(agevolazione, agevolazione, "", "", "", "", "", ""));
                agout.append(ag2.getDescrizione()).append(";");
            });
        } else {
            agout.append("");
        }
        return agout.toString();
    }

    public static ArrayList<Items> agevolazioni(Booking bo) {
        Database db = new Database();
        ArrayList<Items> result = db.agevolazioni_e_servizi(bo, "sito_agevolazioni_varie");
        db.closeDB();
        return result;
    }

    public static ArrayList<Items> servizi(Booking bo) {
        Database db = new Database();
        ArrayList<Items> result = db.agevolazioni_e_servizi(bo, "sito_servizi_agg");
        db.closeDB();
        return result;
    }

    public static String getJsonString(String input, String fieldname) {
        try {
            JSONObject base = new JSONObject(input);
            Object o1 = base.get(fieldname);
            if (o1 != null) {
                return o1.toString().trim();
            }
        } catch (Exception e) {
        }
        return "";
    }

    public static double fd(String si_t_old) {
        if (si_t_old == null) {
            return 0.0D;
        }
        double d1;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (NumberFormatException e) {
            d1 = 0.0D;
        }
        return d1;
    }

}
