/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.util.ArrayList;
import static rc.soop.maintenance.ProceduraDaily.allinea;

/**
 *
 * @author rcosco
 */
public class ProceduraDaily_CZ {

//    public static void allineafilialeCZ(String filiale, String ip) {
//        Db_Master dbm = new Db_Master(true, false);
//        ArrayList<Branch> li = dbm.list_branch_enabled();
//        dbm.closeDB();
//
//        Db_Master dbf = new Db_Master(true, ip);
//
//        if (dbf.getC() == null) {
//            System.err.println(filiale + " NON RAGGIUNGIBILE");
//        } else {
//            for (int y = 0; y < li.size(); y++) {
//                if (li.get(y).getCod().equals(filiale)) {
//                    allinea(dbf, li.get(y), li);
//                }
//            }
//            dbf.closeDB();
//        }
//    }

    public static void main(String[] args) {

        Db_Master dbm = new Db_Master(false, true);
        ArrayList<Branch> li = dbm.list_branch_enabled();
        for (int y = 0; y < li.size(); y++) {
                allinea(dbm, li.get(y), li);
        }
        dbm.closeDB();

    }

}
