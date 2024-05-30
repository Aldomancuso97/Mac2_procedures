/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.createLog;
import static rc.soop.start.Utility.getTime;
import static rc.soop.start.Utility.pattern4;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class UnlockOperation{

    private static final String user_h = rb.getString("user_h");
    private static final String pwd_h = rb.getString("pwd_h");
    private static final String host_h = rb.getString("host_h");
    private static final String user_f = rb.getString("user_f");
    private static final String pwd_f = rb.getString("pwd_f");

    public void engine() {
        Logger log = createLog("Mac2.0_UNLOCK", rb.getString("path.log"), pattern4);
        DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
        int min = db2.get_national_office_minutes();
        DateTime now = getTime(log);
        ArrayList<String> brlist = db2.list_branch_enabled();
        
        brlist.remove("000");
        
        DateTime lastC = db2.get_last_date_blocked();
        //CENTRALE
        if (lastC != null) {
            if (lastC.plusMinutes(min).isBefore(now)) {
                boolean es = db2.updateBlockedOperation("0000 | Service", "0");
                if (es) {
                    log.log(Level.WARNING, "BRANCH: {0} OPERAZIONE SBLOCCATA.", "000");
                } else {
                    log.log(Level.SEVERE, "BRANCH: {0} IMPOSSIBILE SBLOCCARE OPERAZIONE.", "000");
                }
            }
        }
        db2.closeDB();

        //FILIALI
        Iterator p = brlist.iterator();
        while (p.hasNext()) {
            String bra = p.next().toString();
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            String myip = db.getIpFiliale(bra);
            db.closeDB();
            if (myip != null && !myip.equals("")) {
                DBFiliale dbfiliale = new DBFiliale(myip, bra, user_f, pwd_f, log);
                if (dbfiliale.getConnectionDB() == null) {
                    log.log(Level.SEVERE, "BRANCH: {0} - IP: {1} non raggiungibile.", new Object[]{bra, myip});
                    continue;
                } else {
//                    log.log(Level.INFO, "BRANCH: {0} RAGGIUNGIBILE.", bra);
                }
                DateTime last = dbfiliale.get_last_date_blocked();
                if (last != null) {
                    if (last.plusMinutes(min).isBefore(now)) {
                        boolean es = dbfiliale.updateBlockedOperation("0000 | Service", "0");
                        if (es) {
                            log.log(Level.WARNING, "BRANCH: {0} OPERAZIONE SBLOCCATA.", bra);
                        } else {
                            log.log(Level.SEVERE, "BRANCH: {0} IMPOSSIBILE SBLOCCARE OPERAZIONE.", bra);
                        }
                    } else {
//                        log.log(Level.INFO, "BRANCH: {0} NESSUNA OPERAZIONE DA SBLOCCARE - ANCORA TROPPO PRESTO.", bra);
                    }
                } else {
//                    log.log(Level.INFO, "BRANCH: {0} NESSUNA OPERAZIONE DA SBLOCCARE.", bra);
                }
                dbfiliale.closeDB();
            }

        }
    }

////    public static void main(String[] args) {
////       new UnlockOperation().unlock();
////    }

}
