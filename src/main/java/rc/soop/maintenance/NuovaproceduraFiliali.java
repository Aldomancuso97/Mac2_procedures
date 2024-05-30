/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static rc.soop.maintenance.ProceduraDaily.patternnormdate_filter;
import static rc.soop.maintenance.ProceduraDaily.patternsql;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class NuovaproceduraFiliali {
    
    public static void correggi(String startdate, String filialedafare) {
        Db_Master dbm = new Db_Master();
        ArrayList<String> filiali = dbm.list_cod_branch_enabled();
        List<IpFiliale> listaip = dbm.getIpFiliale();
        String valutalocale = dbm.get_local_currency()[0];
        dbm.closeDB();
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(patternnormdate_filter);
        final DateTime today = new DateTime();
        try {
            filiali.stream().filter(x -> !x.equals("000")).forEach(filiale -> {
                boolean ver = true;
                if (filialedafare != null) {
                    ver = filiale.equals(filialedafare);
                }
                if (ver) {
                    IpFiliale ip = listaip.stream().filter(b2 -> b2.getFiliale().equals(filiale)).findAny().orElse(null);
                    if (ip != null) {
                        String sanitizedFiliale = sanitizeInput(filiale); // Sanifica l'input della filiale
                        String fil[] = {sanitizedFiliale, sanitizedFiliale};
                        ArrayList<Processed> lista = new ArrayList<>();
                        DateTime start = formatter.parseDateTime(startdate);
                        AtomicInteger indice = new AtomicInteger(0);
                        while (start.isBefore(today)) {
                            String data1 = start.toString(patternsql) + " 00:00";
                            String data2 = start.toString(patternsql) + " 23:59";
                            Db_Master dbfil = new Db_Master(true, ip.getIp());
                            if (dbfil.getC() == null) {
                                System.out.println(filiale + " NON CONNESSA " + data1);
                                start = start.minusDays(1);
                                throw new BreakException();
                            } else {
                                Daily_value_proc dv = dbfil.list_Daily_value_NEW(fil, data1, data2, valutalocale);
                                dbfil.closeDB();
                                if (dv != null) {
                                    Processed v1 = new Processed(
                                            start.toString(patternnormdate_filter),
                                            dv.getCashOnPrem(),
                                            dv.getOfficesp(),
                                            dv.getLastCashOnPrem());
                                    lista.add(v1);
                                    if (indice.get() > 0) {
                                        Processed ieri = lista.get(indice.get() - 1);
                                        if (!v1.getLcop().equals(ieri.getCop())) {
                                            System.out.println(filiale + " --- " + v1.getD1() + " - ERRATO - va cambiato valore da " + v1.getLcop() + " A " + ieri.getCop());
                                            Db_Master dbfil1 = new Db_Master(true, ip.getIp());
                                            if (dbfil1.getC() == null) {
                                                System.out.println(filiale + " NON CONNESSA " + data1);
                                                start = start.minusDays(1);
                                                throw new BreakException();
                                            } else {
                                                boolean es = dbfil1.updateCOP(v1.getOsp(), ieri.getCop());
                                                dbfil1.closeDB();
                                                if (es) {
                                                    System.out.println(filiale + " --- " + v1.getD1() + " CORRETTO");
                                                    correggi(v1.getD1(), filiale);
                                                }
                                            }
                                        } else {
                                            System.out.println(filiale + " --- " + v1.getD1() + " OK");
    
                                        }
                                    }
                                    indice.addAndGet(1);
                                    start = start.plusDays(1);
                                }
                            }
                        }
                    }
                }
            });
        } catch (BreakException e) {
            // Stopped
        }
    
    }
    
    
    private static String sanitizeInput(String input) {
        return input.replaceAll("[^a-zA-Z0-9]", "");
    }
    
    

    private static void createBat(boolean pause) {
        try {
            Db_Master dbm = new Db_Master();
            ArrayList<String> brlist = dbm.list_cod_branch_enabled();
            dbm.closeDB();
            for (int i = 0; i < brlist.size(); i++) {
                String fil = brlist.get(i);
                File f1 = new File("ALLINEA_" + fil + ".bat");
                FileOutputStream is = new FileOutputStream(f1);
                OutputStreamWriter osw = new OutputStreamWriter(is);
                BufferedWriter w = new BufferedWriter(osw);
                w.write("java -jar MacMonitor.jar " + fil + " 01/01/02018");
                w.newLine();
                if (pause) {
                    w.write("pause");
                    w.newLine();
                }
                w.close();
                osw.close();
                is.close();
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

//    public static void main(String[] args) {
////        createBat(true);
//        String fil;
//        String data;
//        try {
//            fil = args[0];
//            data = args[1];
//        } catch (Exception e) {
//            fil = "805";
//            data = "01/01/2018";
//        }
//        correggi(data, fil);
//
////        String fil[] = {"802", "802"};
////        Db_Master dbfil = new Db_Master(true, "192.168.84.2");
////        Daily_value_proc dv = dbfil.list_Daily_value_NEW(fil, "2018-07-31", "2018-07-31", "EUR");
////        dbfil.closeDB();
////        System.out.println("macmonitor.NuovaproceduraFiliali.main() "+ dv.getLastCashOnPrem());
////        System.out.println("macmonitor.NuovaproceduraFiliali.main() "+ dv.getOfficesp());
////        System.out.println("macmonitor.NuovaproceduraFiliali.main() "+ dv.getCashOnPrem());
//    }
}

class IpFiliale {

    String filiale, ip;

    public IpFiliale(String filiale, String ip) {
        this.filiale = filiale;
        this.ip = ip;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}

class Processed {

    String d1, cop, osp, lcop;

    public Processed(String d1, String cop, String osp, String lcop) {
        this.d1 = d1;
        this.cop = cop;
        this.osp = osp;
        this.lcop = lcop;
    }

    public String getD1() {
        return d1;
    }

    public void setD1(String d1) {
        this.d1 = d1;
    }

    public String getCop() {
        return cop;
    }

    public void setCop(String cop) {
        this.cop = cop;
    }

    public String getOsp() {
        return osp;
    }

    public void setOsp(String osp) {
        this.osp = osp;
    }

    public String getLcop() {
        return lcop;
    }

    public void setLcop(String lcop) {
        this.lcop = lcop;
    }

}
