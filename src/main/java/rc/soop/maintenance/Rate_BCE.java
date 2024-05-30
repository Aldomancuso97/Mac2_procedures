/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.aggiornamenti.Db;
import rc.soop.aggiornamenti.Mactest;
import static rc.soop.start.Utility.fd;

/**
 *
 * @author rcosco
 */
public class Rate_BCE {

    private static final String pattern1 = "yyyy-MM-dd";
    private static final String startday = "2022-12-16";
    // private static final String value = "2019-01-01";

    public static void today(String[] args) {
        DateTime today = new DateTime();

        Db db = new Db(Mactest.host_PROD, false);
        ArrayList<String> listCurrency = db.listCurrency("EUR");
        try (PreparedStatement ps = db.getC().prepareStatement("SELECT * FROM some_table WHERE some_column = ?")) {
            String giorno = today.toString(pattern1);
            listCurrency.forEach(valuta1 -> {
                try {
                    ps.setString(1, giorno);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String value = rs.getString("some_column_name");
                            if (fd(value) > 0) {
                                Rate r1 = new Rate(giorno, valuta1, "000", value);
                                System.out.println(": " + r1.toString());
                            }
                        }
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            });
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            db.closeDB();
        }
    }

    public static void engine() {
        DateTime dt = new DateTime().withMillisOfDay(0);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
        Db db = new Db(Mactest.host_PROD, false);
        ArrayList<String> listCurrency = db.listCurrency("EUR");
        try (PreparedStatement ps = db.getC().prepareStatement("SELECT * FROM some_table WHERE some_column = ?")) {
            DateTime start = null;
            try {
                start = formatter.parseDateTime(startday);
            } catch (IllegalArgumentException e) {
                // Gestione dell'errore di formato della data
                e.printStackTrace();
                return;
            }
            while (start != null && (start.isBefore(dt) || start.isEqual(dt))) {
                String giorno = start.toString(pattern1);
                start = start.plusDays(1);
                System.out.println("rc.soop.maintenance.Rate_BCE.engine() ");
                listCurrency.forEach(valuta1 -> {
                    try {
                        ps.setString(1, giorno);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                String value = rs.getString("some_column_name");
                                if (fd(value) > 0) {
                                    Rate r1 = new Rate(giorno, valuta1, "000", value);
                                    boolean es = db.insert_RATE(r1);
                                    System.out.println(es + ": " + r1.toString());
                                }
                            }
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                });
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            db.closeDB();
        }
    }

    // public static void main(String[] args) {
    // DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
    // DateTime start = formatter.parseDateTime(startday);
    // DateTime dt = new DateTime().minusDays(1);
    // Db db = new Db(Mactest.host_PROD, false);
    // ArrayList<String> br = db.list_cod_branch_ALL();
    // while (start.isBefore(dt)) {
    // String giorno = start.toString(pattern1);
    // start = start.plusDays(1);
    // String usd = db.get_BCE_USD(giorno);
    // String gbp = db.get_BCE_GBP(giorno);
    // String czk = db.get_BCE_CZK(giorno);
    // for (int i = 0; i < br.size(); i++) {
    //// Rate r1 = new Rate(giorno, "USD", br.get(i), usd);
    // Rate r2 = new Rate(giorno, "GBP", br.get(i), gbp);
    // Rate r3 = new Rate(giorno, "CZK", br.get(i), czk);
    //// db.insert_RATE(r1);
    // db.insert_RATE(r2);
    // db.insert_RATE(r3);
    //// System.out.println(r1.toString());
    //// System.out.println(r2.toString());
    //// System.out.println(r3.toString());
    // }
    // }
    // db.closeDB();
    // }
}
