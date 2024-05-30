/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import static rc.soop.crm.Action.log;
import java.util.ResourceBundle;
import java.util.logging.Level;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author rcosco
 */
public class CRM_batch {

    public static final ResourceBundle rb = ResourceBundle.getBundle("crm.conf");
    public static final boolean test = rb.getString("test").equals("SI");
    public static DateTimeFormatter formatter_N = DateTimeFormat.forPattern("yyyy-MM-dd");

//    public static void main(String[] args) {
//        RECAP_MAIL();
////        EXPIRED_NOSHOW();
//    }
//
//    public static void RECAP_MAIL() {
//        log.warning("START RECAP MAIL");
//        Crm_Db db1 = new Crm_Db();
////        final int days = 1;
//        final int days = db1.get_Day_RECAP();
//        db1.closeDB();
//        List<Booking_Date> total = new ArrayList<>();
//        log.log(Level.INFO, "DAYS BEFORE RECAP: {0}", days);
//        Database db = new Database();
//        try {
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale"
//                    + " FROM sito_prenotazioni WHERE stato='0' AND dt_ritiro = DATE_ADD(curdate(), INTERVAL " + days + " DAY)");
//
////            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale FROM sito_prenotazioni "
////                    + "WHERE stato='0' AND dt_ritiro = curdate()");
//
//            while (rs.next()) {
//                Booking_Date bd1 = new Booking_Date(rs.getString("cod"), rs.getString("dt_ritiro"),
//                        rs.getString("stato"), rs.getString("stato_crm"),
//                        formatter_N.parseDateTime(rs.getString("dt_ritiro")), rs.getString("filiale"));
//                total.add(bd1);
//            }
//        } catch (SQLException ex) {
//            log.log(Level.SEVERE, "RECAP_MAIL SQL: {0}", ex.getMessage());
//        }
//
//        db.closeDB();
//
//        log.log(Level.INFO, "BOOKING RECAP: {0}", total.size());
//
//        AtomicInteger indice = new AtomicInteger(1);
//
//        total.forEach(b1 -> {
//            log.log(Level.INFO, "{0}) BOOKING: {1}", new Object[]{indice.get(), b1.getCod()});
//            Booking b0 = Action.getBookingbyCod(b1.getCod());
//            if (b0.getCanale().equalsIgnoreCase("Website")) {
//                log.info("CANALE SITO");
//                String json = getJSON(b0);
//                log.log(Level.INFO, "REQUEST PREAUTH: {0}", json);
//                Preauth resp = POSTRequest_PREAUTH("preauth", json, b1.getCod());
//                log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{indice.get(), resp.toString()});
//                try {
//                    Crm_Db db2 = new Crm_Db();
//                    db2.insertpreauth(resp);
//                    db2.closeDB();
//                } catch (Exception ex) {
//                }
//            } else {
//                boolean es = send_Mail_REMINDER(b0, days);
//                log.info("CANALE CRM");
//                log.log(Level.INFO, "MAIL TO {0} : {1}", new Object[]{b0.getCl_email(), es});
//            }
//            indice.addAndGet(1);
//        });
//
//    }
//
//    
    public static String getJSON_06(Booking bo) {
        JSONObject json = new JSONObject();
        JSONObject json_cl = new JSONObject();
        try {
            json_cl.put("surname", bo.getCl_cognome());
            json_cl.put("name", bo.getCl_nome());
            json_cl.put("telephone", bo.getCl_telefono());
            json_cl.put("email", bo.getCl_email());
            json.put("commission", bo.getComm());
            json.put("agency", bo.getFiliale());
            json.put("from_import", "");
            json.put("from_rate", "");
            json.put("id", bo.getCod());
            json.put("note", bo.getNote());
            json.put("status", bo.getStato());//passre stato corretto
            json.put("total", bo.getEuro());
            json.put("agevolazioni", bo.getAgevolazioni());
            json.put("serviziagg", bo.getServiziagg());
            json.put("tipologia", bo.getCl_tipologia());
            json.put("canale", "06");
            json.put("to_rate", bo.getCurrency());
            json.put("quantita", bo.getQuantity());
            json.put("rate", bo.getRate());
            json.put("dt_tr", bo.getDt_ritiro());
            json.put("user_data", json_cl);

            Crm_Db crmdb = new Crm_Db();
            json.put("mail_filiale", crmdb.getEmail(bo.getFiliale()));
            json.put("mail_crm", crmdb.getEmail("CRM"));
            crmdb.closeDB();

            return json.toString(3);
        } catch (JSONException ex) {
            log.log(Level.SEVERE, "getJSON {0} -- {1}", new Object[]{bo.getCod(), ex.getMessage()});
        }
        return "ERR";
    }

    public static String getJSON(Booking bo) {
        JSONObject json = new JSONObject();
        JSONObject json_cl = new JSONObject();
        try {
            json_cl.put("surname", bo.getCl_cognome());
            json_cl.put("name", bo.getCl_nome());
            json_cl.put("telephone", bo.getCl_telefono());
            json_cl.put("email", bo.getCl_email());
            json.put("commission", bo.getComm());
            json.put("agency", bo.getFiliale());
            json.put("from_import", "");
            json.put("from_rate", "");
            json.put("id", bo.getCod());
            json.put("note", bo.getNote());
            json.put("status", bo.getStato());//passre stato corretto
            json.put("total", bo.getEuro());
            json.put("agevolazioni", bo.getAgevolazioni());
            json.put("serviziagg", bo.getServiziagg());
            json.put("tipologia", bo.getCl_tipologia());
            json.put("canale", bo.getCanale());
            json.put("to_rate", bo.getCurrency());
            json.put("quantita", bo.getQuantity());
            json.put("rate", bo.getRate());
            json.put("dt_tr", bo.getDt_ritiro());
            json.put("user_data", json_cl);

            Crm_Db crmdb = new Crm_Db();
            json.put("mail_filiale", crmdb.getEmail(bo.getFiliale()));
            json.put("mail_crm", crmdb.getEmail("CRM"));
            crmdb.closeDB();

            return json.toString(3);
        } catch (JSONException ex) {
            log.log(Level.SEVERE, "getJSON {0} -- {1}", new Object[]{bo.getCod(), ex.getMessage()});
        }
        return "ERR";
    }

}

class Booking_Date {

    String cod, dt_ritiro;

    String stato, stato_crm, filiale;

    DateTime dt1;

    public Booking_Date(String cod, String dt_ritiro, String stato, String stato_crm, DateTime dt1, String filiale) {
        this.cod = cod;
        this.dt_ritiro = dt_ritiro;
        this.stato = stato;
        this.stato_crm = stato_crm;
        this.dt1 = dt1;
        this.filiale = filiale;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    public String getStato_crm() {
        return stato_crm;
    }

    public void setStato_crm(String stato_crm) {
        this.stato_crm = stato_crm;
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getDt_ritiro() {
        return dt_ritiro;
    }

    public void setDt_ritiro(String dt_ritiro) {
        this.dt_ritiro = dt_ritiro;
    }

    public DateTime getDt1() {
        return dt1;
    }

    public void setDt1(DateTime dt1) {
        this.dt1 = dt1;
    }
}
