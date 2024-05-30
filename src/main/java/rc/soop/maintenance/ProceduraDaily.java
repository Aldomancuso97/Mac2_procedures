/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class ProceduraDaily {

    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternsqlcomplete = "yyyy-MM-dd HH:mm:SS";
    public static final String patternnormdate_filter = "dd/MM/yyyy";

    public static double getControvalore(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return primo * secondo;
        }

    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
    }

    public static NC_causal getNC_causal(ArrayList<NC_causal> li, String nc_code) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getCausale_nc().equals(nc_code)) {
                return li.get(i);
            }
        }
        return null;
    }

    public static String subDays(String start, String pattern, int days) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
        DateTime dt = formatter.parseDateTime(start);
        return dt.minusDays(days).toString(pattern);
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static double fd(String si_t_old) {
        double d1 = 0.0D;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (Exception e) {
            d1 = 0.0D;
        }
        return d1;
    }

    public static void allinea(Db_Master dbm, Branch b1, ArrayList<Branch> li) {

        String filiale = b1.getCod();
        System.out.println("processo filiale: " + filiale);
        String fil[] = new String[2];
//        String valutalocale = dbm.get_local_currency()[0];
        fil[0] = filiale;
        fil[1] = filiale;

        ArrayList<String[]> lista = new ArrayList<>();

        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternnormdate_filter);
        DateTime start = formatter.parseDateTime("01/01/2020");
        DateTime today = new DateTime().plusDays(1);

        while (!start.toString(patternsql).equals(today.toString(patternsql))) {
            String data1 = start.toString(patternsql) + " 00:00";
            String data2 = start.toString(patternsql) + " 23:59";
//            Daily_value dv = dbm.list_Daily_value(fil, data1, data2, false, true);
            Daily_value dv = dbm.list_Daily_value(fil, data1, data2, false, false);
            double setLastCashOnPrem = 0.00;

            Office_sp oo = dbm.query_officesp(dv.getOfficesp());
            if (oo != null) {
                setLastCashOnPrem = fd(oo.getTotal_cod());
            }

            String[] v = {start.toString(patternsql), dv.getCashOnPrem(), dv.getOfficesp(), roundDoubleandFormat(setLastCashOnPrem, 2)};
            lista.add(v);
            start = start.plusDays(1);
        }
        for (int i = 1; i < lista.size(); i++) {
            String[] oggi = lista.get(i);
            String[] ieri = lista.get(i - 1);
//            System.out.println(filiale + " --- " + Arrays.asList(oggi).toString());
//            System.out.println(filiale + " --- " + Arrays.asList(ieri).toString());
            if (!oggi[3].equals(ieri[1])) {
                System.out.println(filiale + " --- " + oggi[0] + ": - " + oggi[2] + " ERRATO - va cambiato valore da " + oggi[3] + " A " + ieri[1]);
                boolean es = dbm.updateCOP(oggi[2], ieri[1]);
                if (es) {
                    System.out.println(filiale + " --- " + oggi[0] + " CORRETTO");
                }
                allinea(dbm, b1, li);
                break;
            }
        }
    }

//    public static void allineafiliale() {
//        Db_Master dbm = new Db_Master();
//        ArrayList<String> filiali = dbm.list_cod_branch_enabled();
//        ArrayList<String[]> fi1 = dbm.getIpFiliale();
//        ArrayList<Branch> li = dbm.list_branch_enabled();
//        dbm.closeDB();
//
//        for (int x = 0; x < filiali.size(); x++) {
//            String filiale = filiali.get(x);
//            String ip = formatAL(filiale, fi1, 1);
//            Db_Master dbf = new Db_Master(true, ip);
//
//            if (dbf.getC() == null) {
//                System.err.println(filiale + " NON RAGGIUNGIBILE");
//            } else {
//                for (int y = 0; y < li.size(); y++) {
//                    if (li.get(y).getCod().equals(filiale)) {
////                        System.out.println(filiale + ": " + ip);
//                        allinea(dbf, li.get(y), li);
//                    }
//                }
//
//                dbf.closeDB();
//            }
//        }
//    }
    public static void allineaSingola(String codice) {
        Db_Master dbm = new Db_Master();
        ArrayList<Branch> li = dbm.list_branch_enabled();
        for (int y = 0; y < li.size(); y++) {
            if (li.get(y).getCod().equals(codice)) {
                allinea(dbm, li.get(y), li);
                break;
            }
        }
        dbm.closeDB();
    }

//    public static void main(String[] args) {
//
////        allineafilialeCZ("317", "192.168.9.27");
////        if (args[0].equals("FIL")) {
////        allineafiliale();
////        } else if (args[0].equals("CEN")) {
//        Db_Master dbm = new Db_Master();
////        Db_Master dbm = new Db_Master(false, true);
//        ArrayList<Branch> li = dbm.list_branch_enabled();
////        ArrayList<Branch> li = dbm.list_branch_enabled();
//        for (int y = 0; y < li.size(); y++) {
//            if (li.get(y).getCod().equals("165")) {
//                allinea(dbm, li.get(y), li);
//            }
//        }
//        dbm.closeDB();
//
////        } else if (args[0].equals("SIN")) {
////            allineaSingola(args[1]);
////        }
//    }

    public static double parseDoubleR(String value) {
        value = value.replaceAll("-", "").trim();
        double d1 = 0.0D;
        try {
            d1 = Double.parseDouble(value);
        } catch (Exception e) {
            value = formatDoubleforMysql(value);
            //Constant.log.log(Level.SEVERE, "ERROR: {0} VALUE: {1}", new Object[]{e.getMessage(), value});
            return parseDoubleR(value);
//            d1 = 0.0D;
        }
        return d1;
    }

    public static String formatDoubleforMysql(String value) {
        if (value == null) {
            return "0.00";
        }
        if (value.equals("-") || value.equals("")) {
            return "0.00";
        }
        String add = "";
        if (value.contains("-")) {
            add = "-";
            value = value.replaceAll("-", "").trim();
        }

        if (!value.equals("0.00")) {
            if (thousand.equals(".")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(",")) {
                        value = value.replaceAll("\\.", "");
                        value = value.replaceAll(",", ".");
                    }
                } else {
                    value = value.replaceAll("\\.", "");

                    return value + ".00";
                }
            } else if (thousand.equals(",")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(".")) {
                        value = value.replaceAll(",", "");
                    }

                } else {
                    value = value.replaceAll(",", "");
                    return value + "00";
                }

            }
        }
        return add + value;

    }

    public static final String thousand = ".";
    public static final String decimal = ",";
}
