/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import static rc.soop.maintenance.Db_Master.fd;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import java.sql.PreparedStatement;

/**
 *
 * @author rcosco
 */
public class AllineaOSP {

    
    
    public static void exe() {
    
        Db_Master db = new Db_Master();
        List<IpFiliale> fi1 = db.getIpFiliale();
        DateTime start = DateTime.parse("2020-02-01", DateTimeFormat.forPattern("yyyy-MM-dd"));
    
        DateTime end = new DateTime().withMillisOfDay(0);
    
        while (start.isBefore(end)) {
            String stdate = start.toString(ProceduraDaily.patternsql);
            String ds1 = stdate + " 23:59:59";
            fi1.forEach(filial -> {
                if (filial.getFiliale().equals("183")) {
                    String fil[] = {filial.getFiliale(), filial.getFiliale()};
                    ArrayList<BranchStockInquiry_value> dati = db.list_BranchStockInquiry_value(fil, ds1, "CH");
                    if (!dati.isEmpty()) {
                        Office_sp sp = db.list_query_officesp2(fil[0], ds1.substring(0, 10)).get(0);
                        ArrayList<OfficeStockPrice_value> last = db.list_OfficeStockPrice_value(sp.getCodice(), fil[0]);
                        for (int x = 0; x < last.size(); x++) {
                            OfficeStockPrice_value od = last.get(x);
                            for (int i = 0; i < dati.size(); i++) {
                                if (dati.get(i).getCurrency().equalsIgnoreCase(od.getCurrency()) && !od.getQta().equals(dati.get(i).getDati().get(0).toString())) {
                                    System.out.println("macmonitor.AllineaOSP.main(0) " + ds1.substring(0, 10));
                                    System.out.println("macmonitor.AllineaOSP.main(0) " + fil[0]);
                                    System.out.println("macmonitor.AllineaOSP.main(0) " + sp.getCodice());
                                    System.out.println("macmonitor.AllineaOSP.main(0) " + od.getCurrency());
                                    System.out.println("macmonitor.AllineaOSP.main(0) " + od.getQta());
                                    double nc = fd(dati.get(i).getDati().get(0).toString()) * fd(od.getMedioacq());
                                    String upd = "UPDATE office_sp_valori SET quantity = ?, controv = ? WHERE cod = ? AND currency = ? AND kind = '01'";
                                    if (filial.getFiliale().equals("183")) {
                                        System.out.println("macmonitor.AllineaOSP.main(2)  " + upd);
                                        try (PreparedStatement ps = db.getC().prepareStatement(upd)) {
                                            ps.setString(1, dati.get(i).getDati().get(0).toString());
                                            ps.setString(2, Db_Master.roundDoubleandFormat(nc, 2));
                                            ps.setString(3, sp.getCodice());
                                            ps.setString(4, od.getCurrency());
                                            ps.executeUpdate();
                                        } catch (SQLException ex) {
                                            ex.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
            start = start.plusDays(1);
        }
    
        db.closeDB();
    }
    
}