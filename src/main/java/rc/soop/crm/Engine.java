/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import static rc.soop.crm.Action.log;
import static rc.soop.crm.CRM_batch.formatter_N;
import static rc.soop.crm.CRM_batch.getJSON;
import static rc.soop.crm.MailObject.send_Mail_EXPIRED;
import static rc.soop.crm.MailObject.send_Mail_REMINDER;
import static rc.soop.rilasciofile.Utility.sanitizeInput;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class Engine {

    public static void recap_greenNumber() {
        log.warning("START RECAP MAIL - GREEN NUMBER");
        Crm_Db db1 = new Crm_Db();
        final int days = db1.get_Day_RECAP();
        db1.closeDB();
        List<Booking_Date> total = new ArrayList<>();
        log.log(Level.INFO, "DAYS BEFORE RECAP: {0}", days);
        Database db = new Database();
        try {
            String query = "SELECT cod, dt_ritiro, stato, stato_crm, dt_ritiro, filiale "
                    + "FROM sito_prenotazioni WHERE "
                    + "stato_crm IN ('2','3','4') "
                    // + "stato='0' "
                    + "AND canale LIKE '%5' AND dt_ritiro = DATE_ADD(curdate(), INTERVAL ? DAY)";

            try (PreparedStatement ps = db.getC().prepareStatement(query)) {
                ps.setInt(1, days);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Booking_Date bd1 = new Booking_Date(rs.getString("cod"),
                                rs.getString("dt_ritiro"),
                                rs.getString("stato"),
                                rs.getString("stato_crm"),
                                formatter_N.parseDateTime(rs.getString("dt_ritiro")),
                                rs.getString("filiale"));
                        total.add(bd1);
                    }
                }
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "RECAP_MAIL SQL: {0}", ex.getMessage());
        }

        db.closeDB();

        log.log(Level.INFO, "BOOKING GREEN NUMBER RECAP: {0}", total.size());

        AtomicInteger indice = new AtomicInteger(1);
        total.forEach(b1 -> {
            log.log(Level.INFO, "{0}) BOOKING: {1}", new Object[] { indice.get(), b1.getCod() });
            Booking b0 = Action.getBookingbyCod(sanitizeInput(b1.getCod()));
            boolean es = send_Mail_REMINDER(b0, days);
            log.info("CANALE CRM");
            log.log(Level.INFO, "MAIL TO {0} : {1}", new Object[] { b0.getCl_email(), es });
            indice.addAndGet(1);
        });
    }

    public static void set_expired_noshow_NEW() {
        log.warning("START EXPIRED/NOSHOW (NEW VERSION)");
        DateTime today = formatter_N.parseDateTime(new DateTime().toString("yyyy-MM-dd"));

        Database db = new Database();
        List<Booking_Date> total = db.list_total_booking();
        db.closeDB();

        Crm_Db db1 = new Crm_Db();
        final int days = db1.get_Day_NOSHOW();
        db1.closeDB();

        if (days >= 0) {
            AtomicInteger indice = new AtomicInteger(1);

            total.forEach(b1 -> {
                Booking b0 = Action.getBookingbyCod(sanitizeInput(b1.getCod()));
                log.log(Level.INFO, "{0}) BOOKING: {1}", new Object[] { indice.get(), b1.getCod() });
                DateTime end = b1.getDt1().plusDays(days);

                if (today.isAfter(end) || today.isEqual(end)) {

                    log.info("EXPIRED - NON SI POSSONO PIU' TRANSARE");

                    String stato = "8";
                    String statoCRM = "8";

                    if (b0.getCanale().startsWith("Web")) {
                        log.info("CANALE SITO");
                        String json = getJSON(b0);
                        ApiResponse resp = Action.POSTRequestEDIT(json, b1.getCod());

                        if (resp != null) {
                            log.log(Level.INFO, "RESPONSE : {0}", resp.isResult());
                            log.log(Level.INFO, "RESPONSE : {0}", resp.getMessage());
                            if (resp.isResult() && resp.isStatus()) {
                                Database db0 = new Database();
                                boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                                db0.closeDB();

                                if (es1) {
                                    log.info("CAMBIO STATO AVVENUTO");
                                } else {
                                    log.severe("ERRORE NEL CAMBIO STATO");
                                }

                            } else {
                                log.log(Level.SEVERE, "RESPONSE ERROR: {0}", resp.getMessage());
                            }
                        } else {
                            log.severe("RESPONSE NULL");
                        }
                    } else if (b0.getCanale().startsWith("Wel")) {
                        log.info("CANALE SITO WELCOME TRAVEL");
                        String json = getJSON(b0);
                        ApiResponse resp = Action.POSTRequestEDIT_WT(json, b1.getCod());

                        if (resp != null) {
                            log.log(Level.INFO, "RESPONSE : {0}", resp.isResult());
                            log.log(Level.INFO, "RESPONSE : {0}", resp.getMessage());
                            if (resp.isResult() && resp.isStatus()) {
                                Database db0 = new Database();
                                boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                                db0.closeDB();

                                if (es1) {
                                    log.info("CAMBIO STATO AVVENUTO");
                                } else {
                                    log.severe("ERRORE NEL CAMBIO STATO");
                                }

                            } else {
                                log.log(Level.SEVERE, "RESPONSE ERROR: {0}", resp.getMessage());
                            }
                        } else {
                            log.severe("RESPONSE NULL");
                        }
                    } else {
                        log.info("CANALE CRM");
                        Database db0 = new Database();
                        boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                        db0.closeDB();
                        if (es1) {
                            log.info("CAMBIO STATO AVVENUTO");
                        } else {
                            log.severe("ERRORE NEL CAMBIO STATO");
                        }
                    }

                } else if (today.isEqual(b1.getDt1().plusDays(1)) && !b1.getStato().equals("3")) {
                    log.info("NO SHOW - RIMANGONO PER UN GIORNO E SI POSSONO TRANSARE");
                    String stato = "3";
                    String statoCRM = "8";

                    if (b0.getCanale().startsWith("Web")) {
                        log.info("CANALE SITO");
                        String json = getJSON(b0);
                        ApiResponse resp = Action.POSTRequestEDIT(json, b1.getCod());
                        if (resp != null) {
                            log.log(Level.INFO, "RESPONSE : {0}", resp.isResult());
                            log.log(Level.INFO, "RESPONSE : {0}", resp.getMessage());
                            if (resp.isResult() && resp.isStatus()) {
                                Database db0 = new Database();
                                boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                                db0.closeDB();
                                if (es1) {
                                    log.info("CAMBIO STATO AVVENUTO");
                                } else {
                                    log.severe("ERRORE NEL CAMBIO STATO");
                                }
                            } else {
                                log.log(Level.SEVERE, "RESPONSE ERROR: {0}", resp.getMessage());
                            }
                        } else {
                            log.severe("RESPONSE NULL");
                        }

                    } else if (b0.getCanale().startsWith("Wel")) {
                        log.info("CANALE SITO WELCOME TRAVEL");
                        String json = getJSON(b0);
                        ApiResponse resp = Action.POSTRequestEDIT_WT(json, b1.getCod());
                        if (resp != null) {
                            log.log(Level.INFO, "RESPONSE : {0}", resp.isResult());
                            log.log(Level.INFO, "RESPONSE : {0}", resp.getMessage());
                            if (resp.isResult() && resp.isStatus()) {
                                Database db0 = new Database();
                                boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                                db0.closeDB();
                                if (es1) {
                                    log.info("CAMBIO STATO AVVENUTO");
                                } else {
                                    log.severe("ERRORE NEL CAMBIO STATO");
                                }
                            } else {
                                log.log(Level.SEVERE, "RESPONSE ERROR: {0}", resp.getMessage());
                            }
                        } else {
                            log.severe("RESPONSE NULL");
                        }
                    } else if (b0.getCanale().startsWith("Che")) {
                        log.info("CANALE SITO CHE BANCA");
                        String json = getJSON(b0);
                        ApiResponse resp = Action.POSTRequestEDIT_CB(json, b1.getCod());
                        if (resp != null) {
                            log.log(Level.INFO, "RESPONSE : {0}", resp.isResult());
                            log.log(Level.INFO, "RESPONSE : {0}", resp.getMessage());
                            if (resp.isResult() && resp.isStatus()) {
                                Database db0 = new Database();
                                boolean es1 = db0.update_status_sito(b1.getCod(), stato, statoCRM, b1.getFiliale());
                                db0.closeDB();
                                if (es1) {
                                    log.info("CAMBIO STATO AVVENUTO");
                                } else {
                                    log.severe("ERRORE NEL CAMBIO STATO");
                                }
                            } else {
                                log.log(Level.SEVERE, "RESPONSE ERROR: {0}", resp.getMessage());
                            }
                        } else {
                            log.severe("RESPONSE NULL");
                        }
                    } else {
                        log.info("CANALE CRM");

                        Database db0 = new Database();
                        boolean es1 = db0.update_status_sito(sanitizeInput(b1.getCod()), sanitizeInput(stato),
                                sanitizeInput(statoCRM), sanitizeInput(b1.getFiliale()));
                        db0.closeDB();
                        if (es1) {
                            log.info("CAMBIO STATO AVVENUTO");
                            boolean es = send_Mail_EXPIRED(b0);
                            if (es) {
                                log.log(Level.INFO, "MAIL TO {0} : OK",
                                        new Object[] { sanitizeInput(b0.getCl_email()) });
                            } else {
                                log.log(Level.SEVERE, "ERRORE INVIO MAIL TO {0}", sanitizeInput(b0.getCl_email()));
                            }
                        } else {
                            log.severe("ERRORE NEL CAMBIO STATO");
                        }
                    }
                }
            });
        }

    }

    @SuppressWarnings("empty-statement")
    public static void refresh_branch() {
        Database dbmac = new Database();
        List<Items> all = dbmac.list_Branch_Active();
        dbmac.closeDB();

        Crm_Db crm = new Crm_Db();
        List<String> web = crm.list_Branch_Website().stream().map(p -> p.getCod()).collect(Collectors.toList());
        ;
        all.forEach(br1 -> {
            if (!web.contains(br1.getCod())) {
                crm.insert_Branch(br1.getCod(), br1.getDescr());
            }
        });
        crm.closeDB();
    }

    public static void updateSpreadSito() {
        Database dbm0 = new Database();
        dbm0.updateSpreadSito();
        dbm0.closeDB();
    }

    // public static void main(String[] args) {
    //
    // String cmd;
    // try {
    // cmd = args[0];
    // } catch (Exception e) {
    // cmd = "SITO";
    // }
    //
    // if (cmd.equals("SITO")) {
    // // CANALE SITO
    //// preauth_withVerify();
    // // CANALE NUMERO VERDE
    // recap_greenNumber();
    // // NUOVO CANALE - SPORTELLO
    //// recap_nopreauth_06();
    //
    // refresh_branch();
    // updateSpreadSito();
    // } else if (cmd.equals("VERIFICASCADUTE")) {
    // set_expired_noshow_NEW();
    // }
    //
    // // EXPIRED - NOSHOW
    // // set_expired_noshow();
    // // TEST NUOVE API
    // // test_getbooking("139EIWX7DU");
    // }
}
