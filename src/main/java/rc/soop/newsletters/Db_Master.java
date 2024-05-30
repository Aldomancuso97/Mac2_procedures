/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.newsletters;

import com.google.common.base.Splitter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.start.Aggiornamenti_mod;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Db_Master {

    public Connection c = null;

    public Db_Master() {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.user");
            String pwd = rb.getString("db.pwd");
            Class.forName(drivername).newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("connectTimeout", "1000");
            p.put("useUnicode", "true");
            p.put("useJDBCCompliantTimezoneShift", "true");
            p.put("useLegacyDatetimeCode", "false");
            p.put("serverTimezone", "Europe/Rome");
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + rb.getString("host_h"), p);
        } catch (Exception ex) {
            this.c = null;
        }
    }

    public Connection getC() {
        return c;
    }

    public void setC(Connection c) {
        this.c = c;
    }

    public void closeDB() {
        try {
            if (this.c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public String getConf(String id) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            String sql = "SELECT des FROM conf WHERE id = ?";
            ps = this.c.prepareStatement(sql);
            ps.setString(1, id);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1).trim();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return "-";
    }

    public String getPath(String cod) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            String sql = "SELECT descr FROM path WHERE cod = ?";
            ps = this.c.prepareStatement(sql);
            ps.setString(1, cod);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public boolean insert_new_News(Newsletters nl, String user) {
        try {
            String ins = "INSERT INTO newsletter VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement ps = this.c.prepareStatement(ins);
            ps.setString(1, nl.getCod());
            ps.setString(2, nl.getTitolo());
            ps.setString(3, nl.getDescr());
            ps.setString(4, nl.getFileout());
            ps.setString(5, nl.getDest());
            ps.setString(6, nl.getDt_updatestart());
            ps.setString(7, nl.getDt_upload());
            ps.executeUpdate(); // Esegui l'inserimento
    
            // Chiudi il PreparedStatement
            ps.close();
    
            // Chiamata al metodo insertValue_agg_NOCENTRAL
            insertValue_agg_NOCENTRAL(null, ins, null, null, user);
            
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    

    public ArrayList<Newsletters> getNewsDaElaborare() {
        ArrayList<Newsletters> out = new ArrayList<>();
        try (PreparedStatement ps = this.c.prepareStatement("SELECT * FROM newsletter_upl WHERE stato = ?")) {
            ps.setString(1, "0");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Newsletters nw = new Newsletters();
                    nw.setCod(rs.getString(1));
                    nw.setTitolo(rs.getString(2));
                    nw.setDescr(rs.getString(3));
                    nw.setFileout(rs.getString(4));
                    nw.setDest(rs.getString(5));
                    nw.setDt_updatestart(rs.getString(6));
                    nw.setDt_upload(rs.getString(7));
                    nw.setUser(rs.getString(8));
                    out.add(nw);
                }
                ps.close();
                rs.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }
    
    

    public ArrayList<Newsletters> getAllNews() {
        ArrayList<Newsletters> out = new ArrayList<>();
        try (Statement ps = this.c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet rs = ps.executeQuery("SELECT * FROM newsletter_upl")) {
            while (rs.next()) {
                Newsletters nw = new Newsletters();
                nw.setCod(rs.getString(1));
                nw.setTitolo(rs.getString(2));
                nw.setDescr(rs.getString(3));
                nw.setFileout(rs.getString(4));
                nw.setDest(rs.getString(5));
                nw.setDt_updatestart(rs.getString(6));
                nw.setDt_upload(rs.getString(7));
                nw.setUser(rs.getString(8));
                out.add(nw);
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }
    
    
    private ArrayList<String> list_branchcode_all() {
        ArrayList<String> li = new ArrayList<>();
        try (PreparedStatement ps = this.c.prepareStatement(
                 "SELECT distinct(cod) FROM branch WHERE fg_annullato='0' ORDER BY CAST(cod AS DECIMAL (10,0))");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                li.add(rs.getString(1));
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }
    
    

    private void insertValue_agg_NOCENTRAL(PreparedStatement ps, String statement, String filiali,
            String dt_val, String username) {
        String ty;
        String psstring;
        if (ps != null) {
            ty = "PS";
            psstring = ps.toString();
        } else if (statement != null) {
            ty = "ST";
            psstring = statement;
        } else {
            return;
        }
        if (filiali == null) {
            filiali = "";
            ArrayList<String> al = list_branchcode_all();
            for (int i = 0; i < al.size(); i++) {
                if (!al.get(i).equals("000")) {
                    filiali = filiali + al.get(i) + ";";
                }
            }
        }

        Iterable<String> parameters = Splitter.on(";").split(filiali);
        Iterator<String> it = parameters.iterator();
        String dtoper = new DateTime().toString(patternsqldate);
        if (dt_val == null) {
            dt_val = formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
        }

        while (it.hasNext()) {

            String value = it.next().trim();
            if (!value.equals("")) {
                insert_aggiornamenti_mod(new Aggiornamenti_mod(
                        generaId(50), value, dt_val, "0",
                        ty, psstring, username, dtoper));
            }
        }
    }

    public void insert_aggiornamenti_mod(Aggiornamenti_mod am) {
        try {
            String ins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            PreparedStatement ps = this.c.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, am.getCod());
            ps.setString(2, am.getFiliale());
            ps.setString(3, am.getDt_start());
            ps.setString(4, am.getFg_stato());
            ps.setString(5, am.getTipost());
            ps.setString(6, am.getAction());
            ps.setString(7, am.getUser());
            ps.setString(8, am.getTimestamp());
            ps.execute();
            ps.close(); // Chiudi il PreparedStatement
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public ArrayList<String> list_all_users() {
        ArrayList<String> out = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT * FROM users ORDER BY CAST(cod AS decimal (10,0))";
            ps = this.c.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString(2));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return out;
    }

    public boolean insert_new_News_recipients(Newsletters nl, String user) {
        PreparedStatement ps = null;
        try {
            String ins = "INSERT INTO newsletter_status VALUES (?,?,?,?)";
            ps = this.c.prepareStatement(ins);
            ps.setString(1, nl.getCod());
            ps.setString(2, nl.getUser());
            ps.setString(3, nl.getStatus());
            ps.setString(4, nl.getDt_read());
    
            insertValue_agg_NOCENTRAL(ps, null, null, null, user);
    
            return ps.executeUpdate() > 0;
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return false;
    }
    

    public boolean changeStatusNL(String cod, String stato) {
        try {
            String upd = "UPDATE newsletter_upl SET stato=? WHERE cod=?";
            PreparedStatement ps = this.c.prepareStatement(upd);
            ps.setString(1, stato);
            ps.setString(2, cod);
            
            boolean updated = ps.executeUpdate() > 0;
            
            ps.close(); // Chiudi lo statement
            
            return updated;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    

    private static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    private static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";

    private static String formatStringtoStringDate(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        return dat;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public boolean insert_user_Mancanti() {
        try {
            int indice = 1;
            ArrayList<Newsletters> allnews = getAllNews();
            for (int x = 0; x < allnews.size(); x++) {
                Newsletters nw = allnews.get(x);
                nw.setDt_read("-");
                nw.setStatus("U");
                String sql = "SELECT cod FROM users WHERE fg_stato = ? AND cod NOT IN (SELECT DISTINCT(user) FROM newsletter_status WHERE cod = ?)";
                PreparedStatement ps = this.c.prepareStatement(sql);
                ps.setString(1, "1");
                ps.setString(2, nw.getCod());
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    nw.setUser(rs.getString(1));
                    System.out.println(indice + ") INSERT NEWSLETTERS PER UTENTE " + rs.getString(1));
                    indice++;
                    insert_new_News_recipients(nw, rs.getString(1));
                }
                rs.close(); // Chiudi il ResultSet
                ps.close(); // Chiudi il PreparedStatement
            }
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    

}
