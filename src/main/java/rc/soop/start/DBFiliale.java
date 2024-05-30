/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.parseStringDate;
import static rc.soop.start.Utility.patternsqldate;
import java.sql.Statement;

/**
 *
 * @author rcosco
 */
public class DBFiliale {

    private Connection conn = null;
    private Logger log = null;

    public DBFiliale(String ip, String filiale, String user, String pwd, Logger log) {
        this.log = log;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            String host = "//" + ip + ":3306/maccorp";
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useSSL", "false");
            p.put("connectTimeout", "1500");
            p.put("useJDBCCompliantTimezoneShift", "true");
            p.put("useLegacyDatetimeCode", "false");
            p.put("serverTimezone", "UTC");
            this.conn = DriverManager.getConnection("jdbc:mysql:" + host, p);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            // log.severe(ex.getMessage());
            this.conn = null;
        }
    }

    public DBFiliale(Connection conn) {
        this.conn = conn;
    }

    public Connection getConnectionDB() {
        return conn;
    }

    public void closeDB() {
        try {
            if (conn != null) {
                this.conn.close();
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
    }

    public void setStatus_agg(String cod, String st) {
        try {
            String upd = "UPDATE aggiornamenti_mod SET fg_stato = ? WHERE cod = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(upd)) {
                ps.setString(1, st);
                ps.setString(2, cod);
                ps.executeUpdate();
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
    }

    public static String correct(String ing) {
        ing = StringUtils.replaceAll(ing, "']", "");
        return ing;
    }

    public boolean execute_agg(String type, Oper oper) {
        try {
            if (type.equalsIgnoreCase("PS")) {
                try (PreparedStatement ps = this.conn.prepareStatement(oper.getSql())) {
                    for (int i = 0; i < oper.getParam().size(); i++) {
                        ps.setString(i + 1, correct(oper.getParam().get(i)));
                    }
                    if (oper.getType().equals("UPD")) {
                        ps.executeUpdate();
                    } else if (oper.getType().equals("DEL") || oper.getType().equals("INS")) {
                        ps.execute();
                    }
                }
                return true;
            } else if (type.equalsIgnoreCase("ST")) {
                try (Statement st = this.conn.createStatement()) {
                    st.executeUpdate(oper.getSql());
                }
                return true;
            }
        } catch (Exception ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                return true;
            }
            log.severe(ex.getMessage());
        }
        return false;
    }

    public ArrayList<String[]> list_aggiornamenti_mod_div(String filiale, String stato) {
        ArrayList<String[]> li = new ArrayList<>();
        String sql = "SELECT cod,filiale,dt_start,fg_stato,tipost,action,user FROM aggiornamenti_mod "
                + "WHERE filiale <> '" + filiale + "' AND fg_stato='" + stato + "' ORDER BY timestamp,cod";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String[] output = { rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7) };
                li.add(output);
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return li;
    }

    public ArrayList<Aggiornamenti_mod> list_aggiornamenti_mod_div_limit(String filiale, String stato) {
        ArrayList<Aggiornamenti_mod> li = new ArrayList<>();
        String sql = "SELECT cod,filiale,dt_start,fg_stato,tipost,action,user FROM aggiornamenti_mod "
                + "WHERE filiale <> '" + filiale + "' AND fg_stato='" + stato + "' ORDER BY timestamp,cod LIMIT 10000";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                li.add(new Aggiornamenti_mod(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7)));
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return li;
    }

    public DateTime get_last_date_blocked() {
        DateTime dt1 = null;
        try {
            String sql = "SELECT timestamp FROM block_it_et WHERE data = curdate() AND status = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(sql)) {
                ps.setString(1, "1");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        dt1 = parseStringDate(rs.getString(1), patternsqldate);
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return dt1;
    }

    public boolean updateBlockedOperation(String user, String status) {
        boolean es;
        try {
            String upd = "UPDATE block_it_et SET user = ?, status = ? WHERE data = curdate()";
            try (PreparedStatement ps = this.conn.prepareStatement(upd)) {
                ps.setString(1, user);
                ps.setString(2, status);
                es = ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            es = false;
        }
        return es;
    }

    public ArrayList<Aggiornamenti_mod> list_aggiornamenti_mod_div_limit_local(String filiale, String stato) {
        ArrayList<Aggiornamenti_mod> li = new ArrayList<>();
        String sql = "SELECT cod, dt_start, tipost, action FROM aggiornamenti_mod "
                + "WHERE filiale = ? AND fg_stato = ? ORDER BY timestamp, cod LIMIT 10000";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filiale);
            ps.setString(2, stato);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    li.add(new Aggiornamenti_mod(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return li;
    }

}
