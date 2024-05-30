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
public class UpdateDateOLD {
    
//    public static ApiResponse POSTRequest(String metod, String json, String idprenotazione) {
//
//        String linktest = "https://forexchangedev.moneym.eu/api/ext/" + metod;
//        String linkprod = "https://forexchange.it/api/ext/" + metod;
//        String request_date = new DateTime().toString(pat2);
//        URL obj;
//        try {
//            if (test) {
//                obj = new URL(linktest);
//            } else {
//                obj = new URL(linkprod);
//            }
//
//            HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
//            postConnection.setRequestMethod("POST");
//            postConnection.setRequestProperty("userId", "a1bcdefgh");
//            postConnection.setRequestProperty("Content-Type", "application/json");
//            postConnection.setDoOutput(true);
//            OutputStream os = postConnection.getOutputStream();
//            os.write(json.getBytes());
//            os.flush();
//            os.close();
//            int responseCode = postConnection.getResponseCode();
//
//            if (responseCode == 200) { //success
//                BufferedReader in = new BufferedReader(new InputStreamReader(
//                        postConnection.getInputStream()));
//                String inputLine;
//                StringBuilder response = new StringBuilder();
//                while ((inputLine = in.readLine()) != null) {
//                    response.append(inputLine);
//                }
//                in.close();
//                log.log(Level.INFO, "RESPONSE PREAUTH: {0}", response.toString());
//                Gson g = new Gson();
//                return g.fromJson(response.toString(), ApiResponse.class);
//            }
//            log.log(Level.SEVERE, "RESPONSE PREAUTH: ERROR: {0}", postConnection.getResponseCode());
//            log.log(Level.SEVERE, "RESPONSE PREAUTH: ERROR: {0}", postConnection.getResponseMessage());
//            mailerror_Cliente(idprenotazione);
//        } catch (Exception ex) {
//            log.log(Level.SEVERE, "RESPONSE PREAUTH: ERROR: {0}", ExceptionUtils.getStackTrace(ex));
//            mailerror_Cliente(idprenotazione);
//        }
//        return null;
//    }

//    public static Preauth POSTRequest_PREAUTH(String metod, String request, String idprenotazione) {
//        Preauth output = new Preauth(idprenotazione);
//        output.setRequest(request);
//        try {
//            String linktest = "https://forexchangedev.moneym.eu/api/ext/" + metod;
//            String linkprod = "https://forexchange.it/api/ext/" + metod;
//
//            String request_date = new DateTime().toString(pat2);
//            URL obj;
//            if (test) {
//                obj = new URL(linktest);
//            } else {
//                obj = new URL(linkprod);
//            }
//            output.setRequest_date(request_date);
//            log.log(Level.INFO, "LINK REQUEST: {0}", obj.toString());
//            HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
//            postConnection.setRequestMethod("POST");
//            postConnection.setRequestProperty("userId", "a1bcdefgh");
//            postConnection.setRequestProperty("Content-Type", "application/json");
//            postConnection.setDoOutput(true);
//            OutputStream os = postConnection.getOutputStream();
//            os.write(request.getBytes());
//            os.flush();
//            os.close();
//            int responseCode = postConnection.getResponseCode();
//            output.setResponse_code(String.valueOf(responseCode));
//            output.setResponse_message(postConnection.getResponseMessage());
//            if (responseCode == 200 || responseCode == 201) { //success
//                BufferedReader in = new BufferedReader(new InputStreamReader(
//                        postConnection.getInputStream()));
//                String inputLine;
//                StringBuilder response = new StringBuilder();
//                while ((inputLine = in.readLine()) != null) {
//                    response.append(inputLine);
//                }
//                in.close();
//                log.log(Level.INFO, "RESPONSE PREAUTH: {0}", response.toString());
//                output.setResponse(response.toString());
//                output.setResponse_date(new DateTime().toString(pat2));
//                boolean result = Boolean.valueOf(getJsonString(response.toString(), "result"));
//                boolean status = Boolean.valueOf(getJsonString(response.toString(), "status"));
//                if (result && status) {
//                    output.setStato("OK");
//                } else {
//                    mailerror_Cliente(idprenotazione);
//                    output.setStato("KO");
//                }
//            } else {
//                mailerror_Cliente(idprenotazione);
//                output.setResponse("ERROR");
//                output.setResponse_date(new DateTime().toString(pat2));
//                output.setStato("KO");
//            }
//        } catch (Exception ex) {
//            mailerror_Cliente(idprenotazione);
//            log.log(Level.SEVERE, "preauth: {0}", ExceptionUtils.getStackTrace(ex));
//            output.setResponse("ERROR: " + ex.getMessage());
//            output.setResponse_date(new DateTime().toString(pat2));
//            output.setStato("KO");
//        }
//        return output;
//    }
//
//    private static void recap_nopreauth_06() {
//        log.warning("START RECAP MAIL - PAGAMENTO A SPORTELLO");
//        Crm_Db db1 = new Crm_Db();
//        final int days = db1.get_Day_RECAP();
//        db1.closeDB();
//
//        List<Booking_Date> total = new ArrayList<>();
//        log.log(Level.INFO, "DAYS BEFORE RECAP: {0}", days);
//        Database db = new Database();
//        try {
//            String sql = "SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale"
//                    + " FROM sito_prenotazioni WHERE stato='0' AND canale LIKE '%6'"
//                    + " AND dt_ritiro = DATE_ADD(CURDATE(), INTERVAL " + days + " DAY)";
//            ResultSet rs = db.getC().createStatement().executeQuery(sql);
//            System.out.println(sql);
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
//        log.log(Level.INFO, "BOOKING PAGAMENTO A SPORTELLO RECAP: {0}", total.size());
//
//        AtomicInteger indice = new AtomicInteger(1);
//        total.forEach(b1 -> {
//            Booking b0 = Action.getBookingbyCod(b1.getCod());
//            log.log(Level.INFO, "{0}) BOOKING: {1}", new Object[]{indice.get(), b1.getCod()});
//            String json = getJSON_06(b0);
//            log.log(Level.INFO, "REQUEST PAGAMENTO A SPORTELLO: {0}", json);
//            Preauth resp = POSTRequest_PREAUTH("preauth", json, b1.getCod());
//            log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{indice.get(), resp.toString()});
//            indice.addAndGet(1);
//        });
//
//    }
//       private static void preauth_withVerify_today() {
//
//        List<Booking_Date> total = new ArrayList<>();
//        Database db = new Database();
//
//        try {
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale "
//                    + "FROM sito_prenotazioni WHERE stato='0' AND canale LIKE '%1' AND dt_ritiro = (CURDATE());");
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
//        log.log(Level.INFO, "BOOKING PREAUTH: {0}", total.size());
//
//        total.forEach((Booking_Date b1) -> {
//            if (!b1.getCod().equals("038GCJMHVC")) {
//                Booking b0 = Action.getBookingbyCod(b1.getCod());
//                try {
//                    JSONObject json_check = new JSONObject();
//                    json_check.put("idbooking", b1.getCod());
//                    log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json_check.toString(3));
//                    Preauth resp_one = POSTRequest_GETBOOKING("getbooking", json_check.toString(3), b1.getCod());
//                    log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp_one.toString()});
//                    HashMap<String, String> hashMap = new HashMap<>();
//                    hashMap = estr(resp_one.getResponse(), hashMap);
//                    boolean ok1 = getKey(hashMap, "status").equals("AUTHORIZED");
//                    boolean ok2 = getKey(hashMap, "completed").equals("true");
//                    boolean ok3 = getKey(hashMap, "error").equals("false");
//                    boolean ok4 = fd((getKey(hashMap, "amount"))) == fd(b0.getEuro());
//
//                    log.log(Level.INFO, "Status: {0}", getKey(hashMap, "status"));
//                    log.log(Level.INFO, "Completed: {0}", getKey(hashMap, "completed"));
//                    log.log(Level.INFO, "Error: {0}", getKey(hashMap, "error"));
//                    log.log(Level.INFO, "Amount: {0}", fd((getKey(hashMap, "amount"))));
//
//                    if (ok1 && ok2 && ok3 && ok4) {
//                        log.log(Level.INFO, "IDBOOKING: {0} PREAUTORIZZATA {1}", new Object[]{b0.getCod(), getKey(hashMap, "statusMessage")});
//                    } else {
//                        log.log(Level.INFO, "IDBOOKING: {0} DA PREAUTORIZZARE ", new Object[]{b0.getCod()});
//
//                        String json = getJSON(b0);
//                        log.log(Level.INFO, "REQUEST PREAUTH: {0}", json);
//                        Preauth resp = POSTRequest_PREAUTH("preauth", json, b1.getCod());
//                        log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{"0", resp.toString()});
//                        try {
//                            Crm_Db db2 = new Crm_Db();
//                            db2.insertpreauth(resp);
//                            db2.closeDB();
//                        } catch (Exception ex) {
//                            log.log(Level.SEVERE, "ERROR: {0} -- {1}", new Object[]{b1.getCod(), ex.getMessage()});
//                        }
//                    }
//                } catch (Exception ex) {
//                    log.log(Level.SEVERE, "ERROR: {0} -- {1}", new Object[]{b1.getCod(), ex.getMessage()});
//                }
//            }
//        });
//
//    }
//
//    private static void preauth_withVerify() {
//
//        Crm_Db db1 = new Crm_Db();
//        final int days = db1.get_Day_RECAP();
//        db1.closeDB();
//
//        List<Booking_Date> total = new ArrayList<>();
//        log.log(Level.INFO, "DAYS BEFORE PREAUTH: {0}", days);
//        Database db = new Database();
//        try {
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,dt_ritiro,stato,stato_crm,dt_ritiro,filiale"
//                    + " FROM sito_prenotazioni WHERE stato='0' AND canale LIKE '%1' "
//                    + "AND dt_ritiro = DATE_ADD(curdate(), INTERVAL " + days + " DAY)");
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
//        log.log(Level.INFO, "BOOKING PREAUTH: {0}", total.size());
//
//        AtomicInteger indice = new AtomicInteger(1);
//
//        total.forEach(b1 -> {
//            Booking b0 = Action.getBookingbyCod(b1.getCod());
//            try {
//                JSONObject json_check = new JSONObject();
//                json_check.put("idbooking", b1.getCod());
//                log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json_check.toString(3));
//                Preauth resp_one = POSTRequest_GETBOOKING("getbooking", json_check.toString(3), b1.getCod());
//                log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp_one.toString()});
//                HashMap<String, String> hashMap = new HashMap<>();
//                hashMap = estr(resp_one.getResponse(), hashMap);
//                boolean ok1 = getKey(hashMap, "status").equals("AUTHORIZED");
//                boolean ok2 = getKey(hashMap, "completed").equals("true");
//                boolean ok3 = getKey(hashMap, "error").equals("false");
//                boolean ok4 = fd((getKey(hashMap, "amount"))) == fd(b0.getEuro());
//                if (ok1 && ok2 && ok3 && ok4) {
//                    log.log(Level.INFO, "IDBOOKING: {0} PREAUTORIZZATA {1}", new Object[]{b0.getCod(), getKey(hashMap, "statusMessage")});
//                } else {
//                    log.log(Level.INFO, "IDBOOKING: {0} DA PREAUTORIZZARE ", new Object[]{b0.getCod()});
//
//                    String json = getJSON(b0);
//                    log.log(Level.INFO, "REQUEST PREAUTH: {0}", json);
//                    Preauth resp = POSTRequest_PREAUTH("preauth", json, b1.getCod());
//                    log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{indice.get(), resp.toString()});
//                    try {
//                        Crm_Db db2 = new Crm_Db();
//                        db2.insertpreauth(resp);
//                        db2.closeDB();
//                    } catch (Exception ex) {
//                        log.log(Level.SEVERE, "ERROR: {0} -- {1}", new Object[]{b1.getCod(), ex.getMessage()});
//                    }
//                    indice.addAndGet(1);
//                }
//            } catch (Exception ex) {
//                log.log(Level.SEVERE, "ERROR: {0} -- {1}", new Object[]{b1.getCod(), ex.getMessage()});
//            }
//        });
//
//    }
//
// 
    
//    private static void test_getbooking(String codice) {
//        Booking b0 = Action.getBookingbyCod(codice);
//        try {
//            JSONObject json_check = new JSONObject();
//            json_check.put("idbooking", codice);
//            log.log(Level.INFO, "REQUEST IDBOOKING: {0}", json_check.toString(3));
//            Preauth resp_one = POSTRequest_GETBOOKING("getbooking", json_check.toString(3), codice);
//            log.log(Level.WARNING, "{0}) ESITO: {1}", new Object[]{1, resp_one.toString()});
//            HashMap<String, String> hashMap = new HashMap<>();
//            hashMap = estr(resp_one.getResponse(), hashMap);
//
////          Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator("=");
////          System.out.println(mapJoiner.join(hashMap));
//            String status = getKey(hashMap, "status");
//            String completed = getKey(hashMap, "completed");
//            String error = getKey(hashMap, "error");
//            String amount = getKey(hashMap, "amount");
//            String statusMessage = getKey(hashMap, "statusMessage");
//
//            System.out.println("1 " + status);
//            System.out.println("2 " + completed);
//            System.out.println("3 " + error);
//            System.out.println("4 " + fd((amount)));
//            System.out.println("5 " + fd(b0.getEuro()));
//            System.out.println("6 " + statusMessage);
//
////            boolean ok1 = getKey(hashMap, "status").equals("AUTHORIZED");
////            boolean ok2 = getKey(hashMap, "completed").equals("true");
////            boolean ok3 = getKey(hashMap, "error").equals("false");
////            boolean ok4 = fd(formatAmount(getKey(hashMap, "amount"))) == fd(b0.getEuro());
////            if (ok1 && ok2 && ok3 && ok4) {
////                log.log(Level.INFO, "IDBOOKING: {0} PREAUTORIZZATA {1}", new Object[]{b0.getCod(), getKey(hashMap, "statusMessage")});
////            } else {
////                log.log(Level.INFO, "IDBOOKING: {0} DA PREAUTORIZZARE ", new Object[]{b0.getCod()});
////            }
//        } catch (Exception ex) {
//            log.log(Level.SEVERE, "ERROR: {0} --- {1}", new Object[]{codice, ex.getMessage()});
//        }
//
//    }
//    public static void main(String[] args) {
//
//        List<String[]> corretti = new ArrayList<>();
//        String csvFile = "C:\\mnt\\temp\\EXPORT_DATA.csv";
//        String line = "";
//        String cvsSplitBy = ",";
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(csvFile));
//            while ((line = br.readLine()) != null) {
//                corretti.add(line.split(cvsSplitBy));
//            }
//            br.close();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        List<String[]> errati = new ArrayList<>();
//
//        Database db = new Database();
//        try {
//            ResultSet rs = db.getC().createStatement().executeQuery("SELECT cod,timestamp FROM sito_prenotazioni WHERE canale LIKE '%1' ORDER BY cod");
//
//            while (rs.next()) {
//                String[] d1 = {rs.getString("cod"), rs.getString("timestamp")};
//                errati.add(d1);
//            }
//
//            DateTimeFormatter formatterTS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
//
//            AtomicInteger ind = new AtomicInteger(0);
//            for (int x = 0; x < errati.size(); x++) {
//                for (int i = 0; i < corretti.size(); i++) {
//                    if (corretti.get(i)[0].equalsIgnoreCase(errati.get(x)[0])) {
//
//                        DateTime err_date = formatterTS.parseDateTime(StringUtils.substring(errati.get(x)[1], 0, 19).replaceAll("T", " "));
//                        DateTime ok_date = formatterTS.parseDateTime(StringUtils.substring(corretti.get(i)[1], 0, 19).replaceAll("T", " "));
//                        ind.addAndGet(1);
//                        if (!err_date.equals(ok_date)) {
//                            
//                            String upd = "UPDATE sito_prenotazioni SET timestamp ='" + ok_date.toString("yyyy-MM-dd HH:mm:ss") + "' WHERE cod = '" + corretti.get(i)[0] + "'";
////                            System.out.println(ind.get() + ") " + upd);
//                            int y = db.getC().createStatement().executeUpdate(upd);
//                            System.out.println(ind.get() + ") " + (y > 0));
//                            
//                            
//                        } else {
//                            System.out.println(ind.get() + ") " + corretti.get(i)[0] + " OK");
//                        }
//
//                    }
//                }
//            }
//
//        } catch (SQLException ex) {
//            log.log(Level.SEVERE, "RECAP_MAIL SQL: {0}", ex.getMessage());
//        }
//
//        db.closeDB();
//
//    }

}
