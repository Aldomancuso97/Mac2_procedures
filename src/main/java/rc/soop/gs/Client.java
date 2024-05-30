/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.gs;

import com.fasterxml.jackson.databind.ObjectMapper;
import static rc.soop.gs.Config.fd;
import static rc.soop.gs.Config.parseIntR;
import static rc.soop.cora.MacCORA.rb;
import static rc.soop.gs.Config.apptype;
import static rc.soop.gs.Config.log;
import static rc.soop.gs.Config.printJSONObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.CharsetUtils;
import org.joda.time.DateTime;
import org.json.JSONObject;

/**
 *
 * @author rcosco
 */
public class Client {

//    private static void test_INVIO() {
//        
//        DateTime dt1 = getDateRif("01/01/2019");
//        DateTime dt2 = getDateRif("31/12/2019");
//        
//        List<Filiale> input = getInputList();
//        
//        
//        
//        Db_Master db1 = new Db_Master();
//        List<DatiInvio> dati = db1.query_datiinvio(input, dt1, dt2);
//        
//        
//        
//        
//        db1.closeDB();
//        invia(dati);
//    }
//    private static void test() {
//        DateTime dt1 = getDateRif("01/01/2019");
//        DateTime dt2 = getDateRif("31/12/2019");
//        List<Filiale> input = getInputList();
//        Db_Master db1 = new Db_Master();
//        List<DatiInvio> dati = db1.query_datiinvio(input, dt1, dt2);
//        db1.closeDB();
//        print(dati);
//    }
//    private static void print(List<DatiInvio> dati) {
//        System.out.println("codiceTenant;codiceContratto;dataVendite;categoria;vendutoNetto;vendutoLordo;numScontrini;");
//        dati.forEach(di -> {
//            System.out.println(di.getCodiceTenant() + ";" + di.getCodiceContratto() + ";"
//                    + di.getDataVendite() + ";" + di.getCategoria() + ";" + di.getVendutoNetto() + ";" + di.getVendutoLordo() + ";" + di.getNumScontrini() + ";");
//        });
//    }
//
//    public static void invia(List<DatiInvio> dati) {
//        try {
//            if (dati.isEmpty()) {
//                log.severe("NESSUN DATO DA INVIARE");
//                return;
//            }
//            String encoding = Base64.encodeBase64String((usernameWS + ":" + passwordWS).getBytes());
//            try ( CloseableHttpClient client = HttpClientBuilder.create().build()) {
//                HttpPost httppost = new HttpPost(link_PROD);
//                httppost.setHeader("Authorization", "Basic " + encoding);
//                JSONArray ja = new JSONArray();
//                for (int x = 0; x < dati.size(); x++) {
//                    JSONObject json = new JSONObject();
//                    DatiInvio di = dati.get(x);
//                    json.put("codiceTenant", di.getCodiceTenant());
//                    json.put("codiceContratto", di.getCodiceContratto());
//                    json.put("dataVendite", di.getDataVendite());
//                    json.put("categoria", di.getCategoria());
//                    json.put("vendutoNetto", di.getVendutoNetto());
//                    json.put("vendutoLordo", di.getVendutoLordo());
//                    json.put("numScontrini", di.getNumScontrini());
//                    ja.put(json);
//                }
//                StringEntity se = new StringEntity(ja.toString());
//                se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, apptype));
//                httppost.setEntity(se);
//                HttpResponse response = client.execute(httppost);
//                BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
//                String JSON_DATA = "";
//                String line;
//                if ((line = rd.readLine()) != null) {
//                    JSON_DATA = line;
//                }
////          JSONObject obj = new JSONObject(JSON_DATA);
////            printJSONObject(obj);
//            }
//        } catch (IOException | UnsupportedOperationException | JSONException ex) {
//            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
//        }
//    }
//
//    private static final String patternnormdate_filter = "dd/MM/yyyy";
//
//    private static DateTime getDateRif(String from) {
//        try {
//            DateTimeFormatter formatter = DateTimeFormat.forPattern(patternnormdate_filter);
//            DateTime dt = formatter.parseDateTime(from);
//            return dt;
//        } catch (Exception ex) {
//            return null;
//        }
//    }
//
//    private static List<Filiale> getInputList() {
//
//        List<Filiale> input = new ArrayList<>();
//
////        input.add(new Filiale("180", "1000000001427"));
////        input.add(new Filiale("042", "1000000000877"));
////        input.add(new Filiale("179", "1000000001415"));
////        input.add(new Filiale("097", "1000000001252"));
////        input.add(new Filiale("160", "1000000001124"));
////        input.add(new Filiale("090", "1000000001336"));
//////        input.add(new Filiale("167", "1000000001328")); //RIMOSSI IN DATA 09/05/2022
//////        input.add(new Filiale("162", "1000000001253")); //RIMOSSI IN DATA 09/05/2022
////        input.add(new Filiale("188", "1000000001250"));
////        input.add(new Filiale("040", "1000000000146"));
////        input.add(new Filiale("041", "1000000000149"));
////        input.add(new Filiale("164", "1000000001270"));
////        input.add(new Filiale("145", "1000000000973"));
////        input.add(new Filiale("038", "1000000000562"));
////        input.add(new Filiale("178", "3000000000236"));
////        input.add(new Filiale("190", "1000000001043"));
////        input.add(new Filiale("019", "1000000000345"));
////        input.add(new Filiale("051", "1000000001042"));
////        input.add(new Filiale("043", "1000000001281"));
////        input.add(new Filiale("034", "1000000000662"));
////        input.add(new Filiale("029", "1000000000974"));
////        input.add(new Filiale("144", "1000000000975"));
////        input.add(new Filiale("063", "1000000001254"));
////        input.add(new Filiale("021", "1000000001408"));
//        input.sort(Comparator.comparing(Filiale::getCod));
//        return input;
//    }
    //2022-06-30
//    private static final String LINKTEST = "https://gsretail.synergica.tech/tsd-service/rest/v2/dativendite";
//    private static final String LINKPROD = "https://tsd.gsretail.it/tsd-service/rest/v2/dativendite";
    private static final String LINKPROD22 = "https://tsd.gsretail.it/tsd-service/rest/v2/altvendite";
    private static final String US_REST = rb.getString("db.UsRest");
    private static final String PASS_REST = rb.getString("db.PassRest");

    public static void print2022(List<DatiInvio> dati) {
        Entity2022 ent = new Entity2022();
        ent.setUsername(US_REST);
        ent.setPassword(PASS_REST);
        List<Vendite2022> vendite = new ArrayList<>();
        dati.forEach(di -> {
            vendite.add(new Vendite2022(di.getCodiceContratto(), di.getDataVendite(),
                    di.getCategoria(),
                    fd(di.getVendutoNetto()),
                    fd(di.getVendutoLordo()),
                    parseIntR(di.getNumScontrini())));
        });
        ent.setVendite(vendite);
        try {
            FileUtils.writeStringToFile(new File("C:\\mnt\\temp\\rs.txt"),
                    new ObjectMapper().writeValueAsString(ent),
                    CharsetUtils.get("UTF-8"));
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
    }

    public static void invia2022(List<DatiInvio> dati) {
        try {

            if (dati.isEmpty()) {
                log.severe("NESSUN DATO DA INVIARE");
                return;
            }
            
            try ( CloseableHttpClient client = HttpClientBuilder.create().build()) {
                HttpPost httppost = new HttpPost(LINKPROD22);
                Entity2022 ent = new Entity2022();
                ent.setUsername(US_REST);
                ent.setPassword(PASS_REST);
                List<Vendite2022> vendite = new ArrayList<>();
                dati.forEach(di -> {
                    vendite.add(new Vendite2022(di.getCodiceContratto(),
                            di.getDataVendite(),
                            di.getCategoria(),
                            fd(di.getVendutoNetto()),
                            fd(di.getVendutoLordo()),
                            parseIntR(di.getNumScontrini())));
                });
                ent.setVendite(vendite);
                StringEntity se = new StringEntity(new ObjectMapper().writeValueAsString(ent));
                se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, apptype));
                httppost.setEntity(se);
                HttpResponse response = client.execute(httppost);
                BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                String JSON_DATA = "";
                String line;
                if ((line = rd.readLine()) != null) {
                    JSON_DATA = line;
                }
                JSONObject obj = new JSONObject(JSON_DATA);
                printJSONObject(obj);
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
    }

//    public static void main(String[] args) {
//        DateTime dt = new DateTime();
//        Db_Master db1 = new Db_Master();
//        List<Filiale> input = db1.getConfList();
//        List<DatiInvio> dati = db1.query_datiinvio(input, dt);
////////        ArrayList<DatiInvio> dati = db1.query_datiinvio_annuale(ing);
//        db1.closeDB();
////        invia2022(dati);
//        print2022(dati);
//    }

}
