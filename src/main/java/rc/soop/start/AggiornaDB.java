/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import java.util.logging.Level;
import java.util.logging.Logger;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class AggiornaDB {

    private static final String user_h = rb.getString("user_h");
    private static final String pwd_h = rb.getString("pwd_h");
    private static final String host_h = rb.getString("host_h");
    private static final String user_f = rb.getString("user_f");
    private static final String pwd_f = rb.getString("pwd_f");

    public static boolean aggiornaDB(Logger log, String filiale, Oper oper) {
        String type = "ST";
        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        String myip = db.getIpFiliale(filiale);
        db.closeDB();
        boolean errore = false;
        DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
        if (dbfiliale.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
            errore = true;
        } else {
            boolean es = dbfiliale.execute_agg(type, oper);
            if(!es){
                errore=true;
            }
            dbfiliale.closeDB();
        }
        return errore;
    }

}