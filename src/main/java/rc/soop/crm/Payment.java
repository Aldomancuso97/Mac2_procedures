/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

/**
 *
 * @author rcosco
 */
public class Payment {

////    private static String getBookingJSON(Booking old_b, String stato, String totale) {
////        JSONObject json = new JSONObject();
////        JSONObject json_cl = new JSONObject();
////        try {
////            json_cl.put("surname", old_b.getCl_cognome());
////            json_cl.put("name", old_b.getCl_nome());
////            json_cl.put("telephone", old_b.getCl_telefono());
////            json_cl.put("email", old_b.getCl_email());
////            json.put("commission", old_b.getComm());
////            json.put("agency", old_b.getFiliale());
////            json.put("from_import", "");
////            json.put("from_rate", "");
////            json.put("id", old_b.getCod());
////            json.put("note", old_b.getNote());
////            json.put("status", stato);
////            json.put("total", totale);
////            json.put("agevolazioni", old_b.getAgevolazioni());
////            json.put("serviziagg", old_b.getServiziagg());
////            json.put("tipologia", old_b.getCl_tipologia());
////            json.put("canale", old_b.getCanale());
////            json.put("to_rate", old_b.getCurrency());
////            json.put("quantita", old_b.getQuantity());
////            json.put("rate", old_b.getRate());
////            json.put("dt_tr", old_b.getDt_ritiro());
////            json.put("user_data", json_cl);
////            return json.toString();
////        } catch (Exception ex) {
////            System.err.println("ERROR: " + ex.getMessage());
////        }
////        return null;
////    }
////
////    private static void force_pay(String codice, String importo) {
////        Database db0 = new Database();
////        Booking bo = db0.getBookingbyCod(codice);
////        db0.closeDB();
////        String json = getBookingJSON(bo, "7", importo);
////        System.out.println("REQUEST PAY: " + json);
////        POSTRequest("editbooking", json, codice);
////    }
////
////    private static void force_check(String codice) {
////        try {
////            JSONObject json = new JSONObject();
////            json.put("idbooking", codice);
////            log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json.toString(3));
////            Preauth resp = POSTRequest_GETBOOKING("getbooking", json.toString(3), codice);
////            log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp.toString()});
////        } catch (JSONException ex) {
////            ex.printStackTrace();
////        }
////
////    }
////
////    private static void force_preauth(String codice) {
////        Booking b0 = Action.getBookingbyCod(codice);
////        String json = getJSON(b0);
////        System.out.println("REQUEST PREAUTH: " + json);
////        Preauth resp = POSTRequest_PREAUTH("preauth", json, codice);
////        System.out.println("ESITO: " + resp.toString());
////    }

}
