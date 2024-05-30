/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.qlik;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Db {

    private Connection c = null;
    private LoggerNew logger = new LoggerNew("SFTP_MAC", "/mnt/mac/log/");

    public Db(boolean mac) {
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
            String host = mac ? rb.getString("db.ip") + "/maccorpita" : rb.getString("db.ip") + "/macsftp";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            logger.log.log(Level.SEVERE, "Errore CONNESSIONE DB : {0}", ex.getMessage());
            this.c = null;
        }
    }

    public void closeDB() {
        try {
            if (c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore CONNESSIONE DB : {0}", ex.getMessage());
        }
    }

    public Connection getConnectionDB() {
        return c;
    }

    public Connection getConnection() {
        return c;
    }

    public String getPath(String path) {
        try {
            String sql = "SELECT url FROM path WHERE id=?";
            try (PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, path);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("url");
                    }
                }
            }
        } catch (SQLException e) {
            logger.log.log(Level.SEVERE, "Errore DB getPath: {0}", e.getMessage());
        }
        return "";
    }

}
