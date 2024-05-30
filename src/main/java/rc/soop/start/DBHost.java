/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import com.google.common.base.Splitter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;

import static rc.soop.start.Central_Branch.host_h;
import static rc.soop.start.Utility.comma;
import static rc.soop.start.Utility.formatStringtoStringDate;
import static rc.soop.start.Utility.parseStringDate;
import static rc.soop.start.Utility.pattern1;
import static rc.soop.start.Utility.patternsqldate;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class DBHost {

    private Connection conn = null;
    private Logger log = null;

    public DBHost(String host, String user, String pwd, Logger log) {
        this.log = log;
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
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
            this.conn = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
            this.conn = null;
        }
    }

    public DBHost(Connection conn) {
        try {
            this.conn = conn;
        } catch (Exception ex) {
            if (log != null) {
                log.log(Level.SEVERE, "{0}: ERROR {1}",
                        new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
            } else {
                System.err.println("Logger not initialized: " + ex.getMessage());
            }
        }
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
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

    public DateTime now() {
        try {
            String sql = "SELECT now()";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    DateTime now = parseStringDate(rs.getString(1), patternsqldate);
                    if (host_h.contains("uk")) {
                        return now.minusHours(1);
                    } else {
                        return now;
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        if (host_h.contains("uk")) {
            return new DateTime().minusHours(1);
        } else {
            return new DateTime();
        }
    }

    public String getIpFiliale(String filiale) {
        try {
            String sql = "SELECT ip FROM dbfiliali WHERE filiale = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, filiale);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
            return "";
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public ArrayList<Aggiornamenti_mod> list_aggiornamenti_mod(String filiale, String stato) {
        ArrayList<Aggiornamenti_mod> li = new ArrayList<>();
        String sql = "SELECT cod, dt_start, tipost, action FROM aggiornamenti_mod WHERE filiale = ? AND fg_stato = ? ORDER BY timestamp, cod LIMIT 10000";

        try (
                PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, filiale);
            ps.setString(2, stato);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    li.add(new Aggiornamenti_mod(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return li;
    }

    public void setStatus_agg(String cod, String st) {
        try {
            String upd = "UPDATE aggiornamenti_mod SET fg_stato = ? WHERE cod = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, st);
                ps.setString(2, cod);
                ps.executeUpdate();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

    public boolean execute_agg_copia(String cod, String filiale, String dt_start, String type, String action,
            String user) {
        try {
            String sqlins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            try (PreparedStatement ps = this.conn.prepareStatement(sqlins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, cod);
                ps.setString(2, filiale);
                ps.setString(3, dt_start);
                ps.setString(4, "0");
                ps.setString(5, type);
                ps.setString(6, action);
                ps.setString(7, user);
                ps.setString(8, Utility.getTime(log).toString(patternsqldate));
                int x = ps.executeUpdate();
                return x > 0;
            }
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                return true;
            }
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;

    }

    public boolean execute_agg(String type, Oper oper) {
        try {
            if (type.equalsIgnoreCase("PS")) {
                try (PreparedStatement ps = this.conn.prepareStatement(oper.getSql(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
                    for (int i = 0; i < oper.getParam().size(); i++) {
                        ps.setString(i + 1, oper.getParam().get(i));
                    }
                    if (oper.getType().equals("UPD")) {
                        ps.executeUpdate();
                    } else if (oper.getType().equals("DEL") || oper.getType().equals("INS")) {
                        ps.execute();
                    }
                }
                return true;
            } else if (type.equalsIgnoreCase("ST")) {
                try (Statement st = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
                    st.executeUpdate(oper.getSql());
                    return true;
                }
            }
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                return true;
            }
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public ArrayList<String> list_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT distinct(cod) FROM branch WHERE fg_annullato = ? ORDER BY cast(cod AS decimal (10,0))";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "0");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public List<WaitingExcel> getExceldaElaborare() {
        List<WaitingExcel> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,fileout,user,dt_start,data FROM excel_upload WHERE stato = ? order by data";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "0");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new WaitingExcel(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getString(5)));
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public boolean getPresenzaValuta(String filiale, String valuta, String bce, String dt_start, String user) {
        try {
            String selectQuery = "SELECT valuta FROM valute WHERE valuta = ? AND filiale = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(selectQuery, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, valuta);
                ps.setString(2, filiale);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        String ins = "INSERT INTO valute (filiale, valuta, codice_uic_divisa, de_valuta, cambio_acquisto, cambio_vendita, cambio_bce, de_messaggio) VALUES (?, ?, '', '-', '', '', ?, '-')";
                        if (filiale.equals("000")) {
                            insertValue_agg(null, ins, filiale, dt_start, user);
                        } else {
                            insertValue_agg(null, ins, "000", dt_start, user);
                            insertValue_agg(null, ins, filiale, dt_start, user);
                        }
                    }
                }
            }
            return true;
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    private void insertValue_agg(PreparedStatement ps, String statement, String filiali,
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
            ArrayList<String> al = list_branch_enabled();
            for (int i = 0; i < al.size(); i++) {
                filiali = filiali + al.get(i) + comma;
            }
        }
        Iterable<String> parameters = Splitter.on(comma).split(filiali);
        Iterator<String> it = parameters.iterator();
        String dtoper = new DateTime().toString(patternsqldate);
        if (dt_val == null) {
            dt_val = formatStringtoStringDate(dtoper, patternsqldate, pattern1, log);
        }
        while (it.hasNext()) {
            String value = it.next().trim();
            if (!value.equals("")) {
                insert_aggiornamenti_mod(new Aggiornamenti_mod(
                        Utility.generaId(50), value, dt_val, "0",
                        ty, psstring, username, dtoper));
            }
        }
    }

    public void insert_aggiornamenti_mod(Aggiornamenti_mod am) {
        try {
            String ins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            try (PreparedStatement ps = this.conn.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, am.getCod());
                ps.setString(2, am.getFiliale());
                ps.setString(3, am.getDt_start());
                ps.setString(4, am.getFg_stato());
                ps.setString(5, am.getTipost());
                ps.setString(6, am.getAction());
                ps.setString(7, am.getUser());
                ps.setString(8, am.getTimestamp());
                ps.execute();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

    public boolean update_change_BCE(String filiale, String valuta, String valore, String dtstart, String username) {
        String upd = "UPDATE valute SET cambio_bce = '" + valore
                + "', buy_std_type = '0', sell_std_type = '0'  WHERE valuta = '" + valuta + "' AND filiale = '"
                + filiale + "'";
        log.info(upd);
        insertValue_agg(null, upd, filiale, dtstart, username);
        insertValue_agg(null, upd, "000", dtstart, username);
        return true;
    }

    public boolean insert_ratehistory(Rate_history rh) {
        try {
            String ins = "INSERT INTO rate_history VALUES (?,?,?,?,?,?,?)";
            try (PreparedStatement ps = this.conn.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, rh.getCodic());
                ps.setString(2, rh.getFiliale());
                ps.setString(3, rh.getValuta());
                ps.setString(4, rh.getTipomod());
                ps.setString(5, rh.getModify());
                ps.setString(6, rh.getUser());
                ps.setString(7, rh.getDt_mod());
                ps.execute();
                String dtoper = Utility.getTime(log).toString(patternsqldate);
                String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, pattern1, log);
                insert_aggiornamenti_mod(new Aggiornamenti_mod(
                        Utility.generaId(50), rh.getFiliale(), dt_val, "0",
                        "PS", ps.toString(), "service", dtoper));
                return true;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public boolean update_stato_excel(String cod, String stato) {
        try {
            String upd = "UPDATE excel_upload SET stato = ? WHERE cod = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, stato);
                ps.setString(2, cod);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public ArrayList<String[]> listuserenabledsolocod() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,de_nome,de_cognome FROM users where length(username)<5 AND fg_stato='1'";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] in = { rs.getString(1), rs.getString(2), rs.getString(3) };
                    out.add(in);
                }
            }

        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public boolean updateusername(String cod, String username) {
        try {
            String upd = "UPDATE users SET username = ? WHERE cod = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, username);
                ps.setString(2, cod);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public int get_national_office_minutes() {
        try {
            String sql = "SELECT minutes FROM office";
            try (PreparedStatement ps = this.conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Integer.parseInt(rs.getString(1));
                }
            }
        } catch (NumberFormatException | SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return 0;
    }

    public DateTime get_last_date_blocked() {
        try {
            String sql = "SELECT timestamp FROM block_it_et WHERE data = curdate() AND status = ?";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "1");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return parseStringDate(rs.getString(1), patternsqldate);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public boolean updateBlockedOperation(String user, String status) {
        try {
            String upd = "UPDATE block_it_et SET user = ?, status = ? WHERE data = curdate()";
            try (PreparedStatement ps = this.conn.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, user);
                ps.setString(2, status);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public String[] get_currency_filiale(String filiale, String valuta) {
        try {
            String sql = "SELECT * FROM valute WHERE filiale = ? AND valuta = ? ORDER BY valuta";
            try (PreparedStatement ps = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, filiale);
                ps.setString(2, valuta);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String[] va = { rs.getString("valuta"), rs.getString("buy_std_type"), rs.getString("buy_std"),
                                rs.getString("sell_std_type"), rs.getString("sell_std") };
                        return va;
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public ResultSet getValoreTabelle(String nomeTabella, String filiale) {
        try {
            try (Statement st = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                return st.executeQuery("select * from " + nomeTabella + " where filiale='" + filiale + "'");
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public boolean insAgg(String tabella, String filiale, String filialenew) {
        try {
            try (Statement stmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                stmt.executeUpdate("insert into " + tabella + " values (select '" + filialenew
                        + "',gruppo_nc,causale_nc,de_causale_nc,fg_in_out,ip_prezzo_nc,fg_tipo_transazione_nc,annullato,nc_de,fg_batch,fg_gruppo_stampa,fg_scontrino,ticket_fee_type,ticket_fee,max_ticket,data,bonus,codice_integr,paymat,docric from "
                        + tabella + " where filiale='" + filiale + "')");
                return true;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return false;
    }

    public void updateSpreadSito() {
        String sql = "SELECT valuta, cambio_bce FROM valute WHERE filiale = '000'";
        String upd = "UPDATE sito_spread SET cambio_bce = ? WHERE valuta = ?";

        try (
                Statement stmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery(sql);
                PreparedStatement ps = this.conn.prepareStatement(upd)) {
            while (rs.next()) {
                ps.setString(1, rs.getString(2));
                ps.setString(2, rs.getString(1));
                ps.executeUpdate();
                insertValue_agg(ps, null, null, null, "site");
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

}
