/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.testarea;

import com.google.common.util.concurrent.AtomicDouble;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import rc.soop.riallinea.Db_Master;
import rc.soop.riallinea.Util;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.roundDoubleandFormat;
import rc.soop.rilasciofile.Ch_transaction;

/**
 *
 * @author Administrator
 */
public class CorreggiErroreJS {

    public static void main(String[] args) {

//        String codice = "111230901092827481jrCErse";
        List<String> elencocodici = new ArrayList<>();
        elencocodici.add("195240413165535101cdSBzeY");
        elencocodici.add("195240413170144186Ono4gTn");
        elencocodici.add("195240413170429853YNw0PsR");
        elencocodici.add("195240413174540640EnrYU2E");
        
        
        
        Db_Master db1 = new Db_Master();
        for (String codice : elencocodici) {
            String sql1 = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.cod='" + codice + "'";
            try {
                ResultSet rs1 = db1.getC().createStatement().executeQuery(sql1);
                while (rs1.next()) {
                    boolean buy = rs1.getString("tipotr").equals("B");
                    String sql2 = "SELECT * FROM ch_transaction_valori tr1 WHERE  tr1.cod_tr='" + codice + "'";
                    ResultSet rs2 = db1.getC().createStatement().executeQuery(sql2);
                    AtomicDouble tr_pay = new AtomicDouble(0.0);
                    AtomicDouble tr_total = new AtomicDouble(0.0);
                    AtomicDouble tr_fix = new AtomicDouble(0.0);
                    AtomicDouble tr_com = new AtomicDouble(0.0);
                    AtomicDouble tr_commission = new AtomicDouble(0.0);

                    while (rs2.next()) {

                        String idriga = rs2.getString("id");
                        String quantita = rs2.getString("quantita");
                        String rate = rs2.getString("rate");
                        String com_perc = rs2.getString("com_perc");
//                        String com_perc_tot = rs2.getString("com_perc_tot");
                        String fx_com = rs2.getString("fx_com");
//                        String tot_com = rs2.getString("tot_com");
//                        String net = rs2.getString("net");
                        String total = rs2.getString("total");

                        double calc_total = fd(quantita) / fd(rate);
                        String set_total = roundDoubleandFormat(calc_total, 2);
                        double calc_com_perc_tot = calc_total * fd(com_perc) / 100.0;
                        String set_com_perc_tot = roundDoubleandFormat(calc_com_perc_tot, 2);
                        double calc_tot_com = fd(fx_com) + calc_com_perc_tot;
                        String set_tot_com = roundDoubleandFormat(calc_tot_com, 2);
                        double calc_net = buy ? (calc_total - calc_tot_com) : (calc_total + calc_tot_com);
                        String set_net = roundDoubleandFormat(calc_net, 2);
//                        System.out.println(rs1.getString(1) + " -- " + total + " -> " + set_total);
//                        System.out.println(rs1.getString(1) + " -- " + com_perc_tot + " -> " + set_com_perc_tot);
//                        System.out.println(rs1.getString(1) + " -- " + tot_com + " -> " + set_tot_com);
//                        System.out.println(rs1.getString(1) + " -- " + net + " -> " + set_net);

                        tr_pay.addAndGet(calc_net);
                        tr_total.addAndGet(calc_total);
                        tr_fix.addAndGet(fd(fx_com));
                        tr_com.addAndGet(fd(set_com_perc_tot));
                        tr_commission.addAndGet(fd(set_tot_com));
                        if (total.equals(set_total)) {
                            System.out.println("RIGA CORRETTA: " + idriga);
                        } else {
//                            System.out.println("RIGA ERRATA: " + idriga);
                            String upd_riga = "UPDATE ch_transaction_valori SET com_perc_tot = '" + set_com_perc_tot + "', tot_com = '" + set_tot_com + "', net = '"
                                    + set_net + "', total = '" + set_total + "' WHERE id = '" + idriga + "';";
                            System.out.println(upd_riga);
                        }
                    }

                    String upd_tr = "UPDATE ch_transaction SET pay = '" + roundDoubleandFormat(tr_pay.get(), 2) + "', total = '" + roundDoubleandFormat(tr_total.get(), 2)
                            + "', fix = '" + roundDoubleandFormat(tr_fix.get(), 2) + "', com = '" + roundDoubleandFormat(tr_com.get(), 2)
                            + "', commission = '" + roundDoubleandFormat(tr_commission.get(), 2) + "' WHERE cod = '" + codice + "';";
                    System.out.println(upd_tr);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        db1.closeDB();

    }

}
