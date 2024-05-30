package rc.soop.newsletters;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static rc.soop.start.Utility.createLog;
import static rc.soop.start.Utility.pattern4;
import static rc.soop.start.Utility.rb;

public class Newsletters {

    String cod;
    String titolo;
    String descr;
    String fileout;
    String dest;
    String dt_updatestart;
    String dt_upload;
    String user;
    String status;
    String dt_read;

    public static ArrayList<String[]> list_status_news() {
        ArrayList<String[]> out = new ArrayList<>();
        String[] s1 = {"U", "Unreaded"};
        String[] s2 = {"R", "Readed"};
        out.add(s1);
        out.add(s2);
        return out;
    }

    public String formatStatus(String status) {
        if (status == null) {
            status = "";
        }
        if (status.equals("R")) {
            return "<div class='font-green-jungle'>Read <i class='fa fa-check'></i></div>";
        }
        if (status.equals("U")) {
            return "<div class='font-blue'>Unreaded <i class='fa fa-hourglass-start'></i></div>";
        }

        return "Error";
    }

    public String formatStatus_cru(String status) {
        if (status == null) {
            status = "";
        }
        if (status.equals("R")) {
            return "Read";
        }
        if (status.equals("U")) {
            return "Unreaded";
        }

        return "Error";
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDt_read() {
        return this.dt_read;
    }

    public void setDt_read(String dt_read) {
        this.dt_read = dt_read;
    }

    public String getCod() {
        return this.cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getTitolo() {
        return this.titolo;
    }

    public void setTitolo(String titolo) {
        this.titolo = titolo;
    }

    public String getDescr() {
        return this.descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }

    public String getFileout() {
        return this.fileout;
    }

    public void setFileout(String fileout) {
        this.fileout = fileout;
    }

    public String getDest() {
        return this.dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getDt_updatestart() {
        return this.dt_updatestart;
    }

    public void setDt_updatestart(String dt_updatestart) {
        this.dt_updatestart = dt_updatestart;
    }

    public String getDt_upload() {
        return this.dt_upload;
    }

    public void setDt_upload(String dt_upload) {
        this.dt_upload = dt_upload;
    }

    private static final Logger log = createLog("Mac2.0_NEWSLETTERS_", rb.getString("path.log"), pattern4);

    public static void engine() {
        Db_Master dbm = new Db_Master();
        ArrayList<Newsletters> out = dbm.getNewsDaElaborare();
        List<String> result = dbm.list_all_users();
        dbm.closeDB();
        log.log(Level.INFO, "TROVATE {0} NEWSLETTERS DA ELABORARE", out.size());
        out.forEach(nw -> {
            String username = nw.getUser();
            Db_Master dbm0 = new Db_Master();
            boolean es = dbm0.insert_new_News(nw, username);
            dbm0.closeDB();
            if (es) {
                nw.setDt_read("-");
                nw.setStatus("U");
                if (nw.getDest().equals("---")) {
                    for (int i = 0; i < result.size(); i++) {
                        nw.setUser(result.get(i));
                        Db_Master dbm2 = new Db_Master();
                        dbm2.insert_new_News_recipients(nw, username);
                        dbm2.closeDB();
                    }
                } else {
                    Db_Master dbm1 = new Db_Master();
                    nw.setUser(nw.getDest());
                    dbm.insert_new_News_recipients(nw, username);
                    dbm1.closeDB();
                }
                Db_Master dbm1 = new Db_Master();
                dbm1.changeStatusNL(nw.getCod(), "1");
                dbm1.closeDB();
                log.info(nw.getCod());
            } else {
                log.log(Level.SEVERE, "ERRORE: {0}", nw.getCod());
            }
        });
        //NUOVA PARTE 18/05
        Db_Master db1 = new Db_Master();
        boolean es = db1.insert_user_Mancanti();
        db1.closeDB();
        if (!es) {
            log.severe("ERRORE DURANTE LA PROCEDURA DI INSERIMENTO NEWSLETTERS MANCANTI");
        }
    }
}
