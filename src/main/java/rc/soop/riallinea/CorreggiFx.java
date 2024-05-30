/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import rc.soop.riallinea.BCE;
import rc.soop.riallinea.Db_Master;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.patternsqldate;
import static rc.soop.riallinea.Util.roundDouble;
import static rc.soop.riallinea.Util.roundDoubleandFormat;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.substring;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author srotella
 */
public class CorreggiFx {

//    public static void main(String[] args) {
//
//        List<String> lista = new ArrayList<>();
////        lista.add("148");
//        try {
//            Db_Master db0 = new Db_Master();
//            try (Statement st1 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//                    ResultSet rs1 = st1.executeQuery("SELECT cod FROM maccorpita.branch WHERE cod<>'000' AND "
//                            + "(fg_annullato = '0' OR (fg_annullato ='1' AND da_annull > '2020-01-01'))")) {
//                while (rs1.next()) {
//                    lista.add(rs1.getString(1));
//                }
//            }
//            db0.closeDB();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsqldate);
//        List<BCE> present = new ArrayList<>();
//
//        lista.forEach(filiale -> {
//            try {
//                DateTime start = new DateTime(2021, 1, 1, 0, 0).withMillisOfDay(0);
//                DateTime end = new DateTime(2021, 2, 1, 0, 0).withMillisOfDay(0);
//                while (start.isBefore(end)) {
//                    LinkedList<Dati> result = ReloadingDati.main(filiale, start);
//                    if (result.get(1).getOFP_FX() != 0) {
//                        String dateLOSP = StringUtils.substring(result.get(1).getCODICE_OFP(), 4, 10);
//                        String yea = "20" + StringUtils.substring(dateLOSP, 0, 2);
//                        String mon = StringUtils.substring(dateLOSP, 2, 4);
//                        String day = StringUtils.substring(dateLOSP, 4);
//
//                        double br_si = 0.0;
//                        double br_fx = 0.0;
//
//                        Db_Master db1 = new Db_Master();
//
//                        String datad1 = yea + "-" + mon + "-" + day + " 23:59:59";
//
//                        String sql = "SELECT f.cod,f.data,f.id,f.user,f.fg_tipo,f.till "
//                                + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<'" + datad1 + "'  AND filiale = '" + filiale + "' GROUP BY till) "
//                                + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = '" + filiale
//                                + "' AND f.data<'" + datad1 + "'"
//                                + " ORDER BY f.till";
//
//                        ResultSet rs = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql);
//
//                        while (rs.next()) {
//                            String sql2 = "SELECT total,kind,cod_value,rate,data FROM stock_report where total<>'0.00' AND filiale='" + filiale + "' "
//                                    + "AND data<'" + datad1 + "' AND tipo='CH' "
//                                    + "AND (codiceopenclose = '" + rs.getString("f.cod") + "' OR codtr = '" + rs.getString("f.cod") + "') "
//                                    + "AND till='" + rs.getString("f.till") + "'";
//                            ResultSet rs2 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql2);
//                            while (rs2.next()) {
//
//                                DateTime dt = formatter.parseDateTime(org.apache.commons.lang3.StringUtils.substring(
//                                        rs2.getString("data"), 0, 19));
//                                String valuta = rs2.getString("cod_value");
//                                String datarif = substring(rs2.getString("data"), 0, 10);
//
//                                if (rs2.getString("kind").equals("01") && valuta.equals("EUR")) {
//                                    br_si = br_si + fd(rs2.getString(1));
//                                } else {
//
//                                    boolean ratefromlist = false;
//                                    if (!present.isEmpty()) {
//                                        ratefromlist = present.stream().filter(c1 -> c1.getData().equalsIgnoreCase(datarif)
//                                                && c1.getValuta().equalsIgnoreCase(valuta)).findAny().orElse(null) != null;
//                                    }
//
//                                    double oldrate;
//
//                                    if (ratefromlist) {
//                                        oldrate = present.stream().filter(c1 -> c1.getData().equalsIgnoreCase(datarif) && c1.getValuta().equalsIgnoreCase(valuta)).findAny().get().getRif_bce();
//                                    } else {
//                                        oldrate = fd(db1.get_BCE(dt, valuta));
//                                    }
//
//                                    double controv_fx = roundDouble(fd(rs2.getString(1)) / oldrate, 2);
//
//                                    System.out.println(rs2.getString("cod_value") + " -) " + fd(rs2.getString(1)) + " -- " + oldrate + " -- " + controv_fx);
//                                    br_fx = br_fx + controv_fx;
//                                }
//                            }
//                        }
//
//
//                        if (br_fx == 0) {
//
//                            System.out.println("CORREGGERE DA " + result.get(1).getOFP_FX() + " A 0");
//
//                            String sql1 = "SELECT * FROM office_sp where codice = '" + result.get(1).getCODICE_OFP() + "'";
//                            ResultSet rs1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql1);
//                            if (rs1.next()) {
//                                double total_fx = fd(rs1.getString("total_fx"));
//                                double total_cod = fd(rs1.getString("total_cod"));
//
//                                double total_grand = fd(rs1.getString("total_grand"));
//                                String new_total_grand = roundDoubleandFormat(total_cod + br_fx, 2);
//
//                                System.out.println("VECCHIO --  FX " + total_fx + " - COP " + total_cod + " - " + total_grand);
//                                System.out.println("NUOVO --  FX " + br_fx + " - COP " + total_cod + " - " + new_total_grand);
//                                String upd = "UPDATE office_sp SET total_fx = '"
//                                        + roundDoubleandFormat(br_fx, 2) + "', total_grand = '"
//                                        + new_total_grand + "' WHERE codice = '" + result.get(1).getCODICE_OFP() + "'";
//
//                                boolean ex = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeUpdate(upd) > 0;
//                                System.out.println(upd + " -> " + ex);
//                            }
//                        } else {
//
//                            String sql1 = "SELECT * FROM office_sp where codice = '" + result.get(1).getCODICE_OFP() + "'";
//                            ResultSet rs1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql1);
//                            if (rs1.next()) {
//                                double total_fx = fd(rs1.getString("total_fx"));
//                                double total_cod = fd(rs1.getString("total_cod"));
//                                double total_grand = fd(rs1.getString("total_grand"));
//                                String new_total_grand = roundDoubleandFormat(total_cod + br_fx, 2);
//
//                                double perc = ((total_fx * 100.00) / br_fx) - 100.00;
//
//                                if (perc > -20 && perc < 20) {
//                                    ///NULLA
//                                } else {
//                                    System.out.println("VECCHIO --  FX " + total_fx + " - COP " + total_cod + " - " + total_grand);
//                                    System.out.println("NUOVO --  FX " + br_fx + " - COP " + total_cod + " - " + new_total_grand);
//                                    System.out.println("SCOSTAMENTO --  % " + perc);
//                                    String upd = "UPDATE office_sp SET total_fx = '"
//                                            + roundDoubleandFormat(br_fx, 2) + "', total_grand = '"
//                                            + new_total_grand + "' WHERE codice = '" + result.get(1).getCODICE_OFP() + "'";
//                                    boolean ex = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeUpdate(upd) > 0;
//                                    System.out.println(upd + " -> " + ex);
//
//                                }
//
//                            }
//
//                        }
//                        db1.closeDB();
//                    }
//                    System.out.println(result.get(1).toString());
//                    start = start.plusDays(1);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        });
//
//    }
}
