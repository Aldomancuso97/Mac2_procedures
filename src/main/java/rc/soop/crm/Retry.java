/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author rcosco
 */
public class Retry {

//    public static void main(String[] args) {
//        Crm_Db db1 = new Crm_Db();
//        final int days = db1.get_Day_RECAP();
//        db1.closeDB();
//        List<Booking_Date> total = new ArrayList<>();
//        log.log(Level.INFO, "DAYS BEFORE RECAP: {0}", days);
//        Database db = new Database();
//        try {
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale"
//                    + " FROM sito_prenotazioni WHERE stato='0' AND dt_ritiro = DATE_ADD(curdate(), INTERVAL " + days + " DAY) AND canale LIKE '%1' LIMIT 10");
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
//        total.forEach(b1 -> {
//            try {
//                log.log(Level.INFO, "{0}) BOOKING: {1}", new Object[]{indice.get(), b1.getCod()});
//                Booking bo = Action.getBookingbyCod(b1.getCod());
//                JSONObject json = new JSONObject();
//                json.put("idbooking", b1.getCod());
//                log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json.toString(3));
//                Preauth resp = POSTRequest_GETBOOKING("getbooking", json.toString(3), b1.getCod());
//                log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp.toString()});
//                HashMap<String, String> hashMap = new HashMap<>();
//                hashMap = estr(resp.getResponse(), hashMap);
//                boolean ok1 = getKey(hashMap, "status").equals("AUTHORIZED");
//                boolean ok2 = getKey(hashMap, "completed").equals("true");
//                boolean ok3 = getKey(hashMap, "error").equals("false");
//                boolean ok4 = fd(formatAmount(getKey(hashMap, "amount"))) == fd(bo.getEuro());
//                if (ok1 && ok2 && ok3 && ok4) {
//                    log.log(Level.INFO, "IDBOOKING: {0} OK {1}", new Object[]{bo.getCod(), hashMap.get("statusMessage")});
//                } else {
//                    log.log(Level.SEVERE, "ERRORE IDBOOKING: {0} ESEGUIRE PREAUTH DI NUOVO "+ok1+" - "+ok2+" - "+ok3+" - "+ok4, bo.getCod());
//                }
//            } catch (Exception ex) {
//                log.log(Level.SEVERE, "RETRY {0} -- {1}", new Object[]{b1.getCod(), ex.getMessage()});
//            }
//        });
//
////        try {
////            String codicebooking = "159AEHMSV5";
////            Booking bo = Action.getBookingbyCod(codicebooking);
//////
////            JSONObject json = new JSONObject();
////            json.put("idbooking", codicebooking);
////            log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json.toString(3));
////            Preauth resp = POSTRequest_GETBOOKING("getbooking", json.toString(3), codicebooking);
////            log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp.toString()});
////
////            HashMap<String, String> hashMap = new HashMap<>();
////            hashMap = estr(resp.getResponse(), hashMap);
////
////            boolean ok1 = getKey(hashMap, "status").equals("AUTHORIZED");
////            boolean ok2 = getKey(hashMap, "completed").equals("true");
////            boolean ok3 = getKey(hashMap, "error").equals("false");
////            boolean ok4 = fd(getKey(hashMap, "amount")) == fd(bo.getEuro());
////
////            if (ok1 && ok2 && ok3 && ok4) {
////                log.info("IDBOOKING: " + bo.getCod() + " OK " + hashMap.get("statusMessage"));
////            } else {
////                log.severe("ERRORE IDBOOKING: " + bo.getCod() + " ESEGUIRE PREAUTH DI NUOVO");
////            }
////        } catch (JSONException ex) {
////            ex.printStackTrace();
////        }
////        System.out.println("crm_batch.Retry.main() "+formatAmount("40076"));
////        System.out.println("crm_batch.Retry.main() "+formatAmount("2"));
////        System.out.println("crm_batch.Retry.main() "+formatAmount("22"));
//    }

    public static HashMap<String, String> estr(String source, HashMap<String, String> input) {
        try {
            JSONObject base = new JSONObject(source);
            for (Iterator<String> it2 = base.keys(); it2.hasNext();) {
                String key2 = it2.next();
                Object o2 = base.get(key2);
                if (o2.getClass().getCanonicalName().equalsIgnoreCase("org.json.JSONArray")) {
                    JSONArray jsonArray = (JSONArray) o2;
                    for (int i = 0; i < jsonArray.length(); i++) {
                        estr(jsonArray.getJSONObject(i).toString(), input);
                    }
                } else if (o2.getClass().getCanonicalName().equalsIgnoreCase("org.json.JSONObject")) {
                    estr(new JSONObject(base.get(key2).toString()).toString(), input);
                } else {
                    input.put(key2, o2.toString().trim());
                }
            }
        } catch (JSONException ex) {
            ex.printStackTrace();
        }
        return input;
    }

    public static String getKey(HashMap<String, String> hashMap, String key) {
        try {
            String v = hashMap.get(key);
            if (v != null) {
                return v.trim();
            }
        } catch (Exception ex) {

        }
        return "";
    }

    public static String formatAmount(String amountNEXI) {
        amountNEXI = StringUtils.leftPad(amountNEXI, 3, "0");
        String integer = StringUtils.substring(amountNEXI, 0, amountNEXI.length() - 2);
        String decimal = StringUtils.substring(amountNEXI, amountNEXI.length() - 2, amountNEXI.length());
        return integer + "." + decimal;
    }

}
