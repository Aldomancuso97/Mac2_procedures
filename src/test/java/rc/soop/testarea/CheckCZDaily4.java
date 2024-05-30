/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.testarea;

import com.google.common.util.concurrent.AtomicDouble;
import java.sql.Statement;
import java.util.ArrayList;
import org.joda.time.DateTime;
import rc.soop.riallinea.BranchStockInquiry_value;
import rc.soop.riallinea.Db_Master;
import rc.soop.riallinea.OfficeStockPrice_value;
import rc.soop.riallinea.Office_sp;
import rc.soop.riallinea.Util;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.patternsql;

/**
 *
 * @author Administrator
 */
public class CheckCZDaily4 {

    public static void main(String[] args) {

//        String fil_cod = "306";
        String fil_cod = "307";
        DateTime start = new DateTime(2021, 12, 31, 0, 0);
        DateTime end = new DateTime(2022, 1, 27, 0, 0);

//        Db_Master db = new Db_Master(true, "192.168.9.18");
        Db_Master db = new Db_Master(true, false);

        while (start.isBefore(end)) {
            String stdate = start.toString(patternsql) + " 23:59:59";
            String fil[] = {fil_cod, fil_cod};
            ArrayList<BranchStockInquiry_value> dati = db.list_BranchStockInquiry_value(fil, stdate, "CH");
            if (!dati.isEmpty()) {
                Office_sp sp = db.list_query_officesp2(fil[0], stdate.substring(0, 10)).get(0);
                ArrayList<OfficeStockPrice_value> last = db.list_OfficeStockPrice_value(
                        sp.getCodice(), fil[0]);
                //1053398.00

                AtomicDouble sum1 = new AtomicDouble(0.0);

                last.forEach(a1 -> {
                    if (!a1.getCurrency().equals("CZK")) {
                        sum1.addAndGet(Util.roundDouble(fd(a1.getControvalore()), 2));
                    }
                });
//
                String upd1 = "UPDATE office_sp SET total_fx='" + rc.soop.esolver.Util.roundDoubleandFormat(sum1.get(), 2)
                        + "' WHERE codice='" + sp.getCodice() + "'";
//
                try {
                    try ( Statement st1 = db.getC().createStatement()) {
                        st1.executeUpdate(upd1);
                    }
                    System.out.println(sp.getData() + " () " + upd1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                            
                System.out.println(sp.getData() + " rc.soop.testarea.CheckCZDaily3.main() " + sum1.get());
//                for (int i = 0; i < dati.size(); i++) {
//                    boolean czk = dati.get(i).getCurrency().equals("CZK");
//                    boolean eur = dati.get(i).getCurrency().equals("EUR");
//                    if (czk) {
//                        OfficeStockPrice_value od = last.stream().filter(f1 -> f1.getCurrency().equals("CZK")).findAny().orElse(null);
//                        if (od != null) {
//
//                            String upd1 = "UPDATE office_sp SET total_cod='" + dati.get(i).getDati().get(0) + "' WHERE codice='" + sp.getCodice() + "'";
//                            String upd2 = "UPDATE office_sp_valori SET quantity='" + dati.get(i).getDati().get(0)
//                                    + "',controv='" + dati.get(i).getDati().get(0) +
//                                    "' WHERE cod='" + sp.getCodice() + "' AND currency='CZK'";
//                            try {
//                                try ( Statement st1 = db.getC().createStatement()) {
//                                    st1.executeUpdate(upd1);
//                                }
//                                try ( Statement st2 = db.getC().createStatement()) {
//                                    st2.executeUpdate(upd2);
//                                }
//
//                                System.out.println(stdate + " (CZK BSI) " + dati.get(i).getDati().get(0)
//                                        + " OSP1 " + sp.getTotal_cod() + " OSP2 "
//                                        + od.getQtaSenzaFormattazione() + "--" + od.getControvalore());
//
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//
//                        }
//                    } else if (eur) {
//                        OfficeStockPrice_value od = last.stream().filter(f1 -> f1.getCurrency().equals("EUR")).findAny().orElse(null);
//                        if (od != null) {
//
////                            String upd1 = "UPDATE office_sp SET total_cod='" + dati.get(i).getDati().get(0) + "' WHERE codice='" + sp.getCodice() + "'";
////                            String upd2 = "UPDATE office_sp_valori SET quantity='" + dati.get(i).getDati().get(0) + "',controv='" + dati.get(i).getDati().get(0) + "' WHERE cod='" + sp.getCodice() + "'";
////                            try {
////                                try (Statement st1 = db.getC().createStatement()) {
////                                    st1.executeUpdate(upd1);
////                                }
////                                try (Statement st2 = db.getC().createStatement()) {
////                                    st2.executeUpdate(upd2);
////                                }
//
//                            double q_ok = fd(String.valueOf(dati.get(i).getDati().get(0)));
//                            double rate = fd(od.getMedioacq());
//
//                            double controv_originale = fd(od.getControvalore());
//                            double controv_nuovo = Util.roundDouble(q_ok * rate, 2);
//                            double diff = controv_nuovo - controv_originale;
//
//                            double totalfx_or = fd(sp.getTotal_fx());
//                            double totalfx_new = totalfx_or + diff;
//
//                            String upd1 = "UPDATE office_sp SET total_FX='" + Util.roundDoubleandFormat(totalfx_new, 2)
//                                    + "' WHERE codice='" + sp.getCodice() + "'";
//                            String upd2 = "UPDATE office_sp_valori SET quantity='" + dati.get(i).getDati().get(0)
//                                    + "',controv='" + Util.roundDoubleandFormat(controv_nuovo, 2)
//                                    + "' WHERE cod='" + sp.getCodice() + "' AND currency = 'EUR'";
////                            
//                            try {
//                                try (Statement st1 = db.getC().createStatement()) {
//                                    st1.executeUpdate(upd1);
//                                }
//                                try ( Statement st2 = db.getC().createStatement()) {
//                                    st2.executeUpdate(upd2);
//                                }
//
//                                System.out.println(stdate + " (EUR) "
//                                        + q_ok
//                                        + " OSP1 "
//                                        + totalfx_or
//                                        + " DIVENTA " + Util.roundDoubleandFormat(totalfx_new, 2)
//                                        + " OSP2 "
//                                        + od.getQtaSenzaFormattazione()
//                                        + " -- " + Util.roundDoubleandFormat(controv_originale, 2)
//                                        + " -- " + Util.roundDoubleandFormat(controv_nuovo, 2)
//                                        + " -- DA AGGIUNGERE AL TOTAL FX " + Util.roundDoubleandFormat(diff, 2)
//                                );
//
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//                }
            }
            start = start.plusDays(1);
        }
        db.closeDB();
    }

}
