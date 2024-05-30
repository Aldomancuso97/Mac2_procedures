package rc.soop.movepdf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import static rc.soop.movepdf.Config.log;
import static rc.soop.movepdf.Config.startfile;
import static rc.soop.start.Utility.rb;

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
            String host = rb.getString("db.ip") + "/" + rb.getString("pdf.db.name");
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

    public List<Doc> get_list_tr_doc_MOVE() {
        List<Doc> li = new ArrayList<>();
        try {
            String sql = "SELECT codice_documento,data_load,content,nomefile FROM ch_transaction_doc "
                    + "WHERE content not like ? LIMIT " + rb.getString("limit");
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, startfile + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        li.add(new Doc(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }

        return li;
    }

    public ArrayList<Doc> get_list_trNC_doc_MOVE() {
        ArrayList<Doc> li = new ArrayList<>();
        try {
            String sql = "SELECT cod,data,docric FROM nc_transaction "
                    + "WHERE docric not like ? AND docric <> ? ORDER BY data ASC LIMIT " + rb.getString("limit");
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, startfile + "%");
                ps.setString(2, "-");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        li.add(new Doc(rs.getString(1), rs.getString(2), rs.getString(3)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }

        return li;
    }

    public ArrayList<Doc> get_list_nc_doc_RESTORE() {
        ArrayList<Doc> li = new ArrayList<>();
        try {
            String sql = "SELECT cod,data,docric FROM nc_transaction WHERE docric like ? LIMIT ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, startfile + "%");
                ps.setInt(2, Integer.parseInt(rb.getString("limit")));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        li.add(new Doc(rs.getString(1), rs.getString(2), rs.getString(3)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return li;
    }

    public ArrayList<String[]> get_list_tr_doc_RESTORE() {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            String sql = "SELECT * FROM ch_transaction_doc WHERE content like ? ORDER BY data_load ASC LIMIT 1";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, startfile + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String[] ctd = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getString(5),
                                rs.getString(6), rs.getString(7), rs.getString(8) };
                        li.add(ctd);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return li;
    }

    public boolean setContentDoc(String codice_documento, String content) {
        try {
            String update = "UPDATE ch_transaction_doc SET content = ? WHERE codice_documento = ?";
            try (PreparedStatement ps = this.c.prepareStatement(update)) {
                ps.setString(1, content);
                ps.setString(2, codice_documento);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public boolean setContentDocNC(String codice_documento, String content) {
        try {
            String update = "UPDATE nc_transaction SET docric = ? WHERE cod = ?";
            try (PreparedStatement ps = this.c.prepareStatement(update)) {
                ps.setString(1, content);
                ps.setString(2, codice_documento);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

}
