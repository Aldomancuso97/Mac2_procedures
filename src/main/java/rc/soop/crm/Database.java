/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import com.google.common.base.Splitter;
import static rc.soop.crm.Action.formatStringtoStringDate;
import static rc.soop.crm.Action.generaId;
import static rc.soop.crm.Action.log;
import static rc.soop.crm.Action.pat1;
import static rc.soop.crm.Action.pat2;
import static rc.soop.crm.CRM_batch.formatter_N;
import static rc.soop.crm.CRM_batch.test;
import static rc.soop.crm.MailObject.sendMailHtml;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.comma;
import static rc.soop.start.Utility.rb;
import java.util.regex.Pattern;

/**
 *
 * @author rcosco
 */
public class Database {

    public Connection c = null;

    public Database() {
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
            String host = test ? rb.getString("db.ip") + "/maccorp" : rb.getString("db.ip") + "/maccorpita";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            this.c = null;
            log.log(Level.SEVERE, "Connection DB: {0}", ex.getMessage());
            sendMailHtml("mac2.0@smartoop.it", "ERROR PREAUTH",
                    "CONTROLLARE ERRORE CONNESSIONE DB PROCEDURA NOTTURNA CRM/CRV PREAUTH " + rc.soop.crm.CRM_batch.test
                            + " :" + ex.getMessage());
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
            log.log(Level.SEVERE, "closeDB: {0}", ex.getMessage());
        }
    }

    public String getUserPermission(String name) {// fatto
        try {
            String sql = "SELECT permessi FROM pagina WHERE nome = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString("permessi");
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "getUserPermission: {0}", ex.getMessage());
        }
        return "";
    }

    public Booking getBookingbyCod(String cod) {
        try {
            String sql = "SELECT * FROM sito_prenotazioni WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Booking(
                        rs.getString("cod").replaceAll(Pattern.quote(""), ""),
                        rs.getString("currency").replaceAll(Pattern.quote(""), ""),
                        rs.getString("quantity").replaceAll(Pattern.quote(""), ""),
                        rs.getString("rate").replaceAll(Pattern.quote(""), ""),
                        rs.getString("comm").replaceAll(Pattern.quote(""), ""),
                        rs.getString("euro").replaceAll(Pattern.quote(""), ""),
                        rs.getString("filiale").replaceAll(Pattern.quote(""), ""),
                        rs.getString("note").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cl_email").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cl_nome").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cl_cognome").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cl_telefono").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cl_tipologia").replaceAll(Pattern.quote(""), ""),
                        rs.getString("canale").replaceAll(Pattern.quote(""), ""),
                        rs.getString("agevolazioni").replaceAll(Pattern.quote(""), ""),
                        rs.getString("serviziagg").replaceAll(Pattern.quote(""), ""),
                        rs.getString("stato").replaceAll(Pattern.quote(""), ""),
                        rs.getString("cod_tr").replaceAll(Pattern.quote(""), ""),
                        rs.getString("dt_ritiro").replaceAll(Pattern.quote(""), ""),
                        rs.getString("dt_tr").replaceAll(Pattern.quote(""), ""),
                        rs.getString("timestamp").replaceAll(Pattern.quote(""), ""),
                        rs.getString("stato_crm").replaceAll(Pattern.quote(""), ""));
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "getBooking " + cod + ": {0}", ex.getMessage());
        }
        return null;
    }

    public String get_descr_Branch(String cod) {
        try {
            String sql = "SELECT de_branch,cod,add_cap,add_via,add_city FROM branch WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return (rs.getString(1) + " - " + rs.getString(2) + " - " + rs.getString(4) + ", " + rs.getString(3)
                        + " " + rs.getString(5)).toUpperCase();
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "getDescrBranch " + cod + ": {0}", ex.getMessage());
        }
        return cod;
    }

    public List<Items> get_branch_list() {
        List<Items> out = new ArrayList<>();
        try (PreparedStatement ps = this.c.prepareStatement(
                "SELECT cod, de_branch, brgr_05, brgr_03 FROM branch WHERE fg_annullato='0' AND cod <> '000' ORDER BY de_branch");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new Items(rs.getString(1), rs.getString(2),
                        (rs.getString(1) + " - " + rs.getString(2)).toUpperCase(), rs.getString(3), rs.getString(4)));
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "sendMailHtml: {0}", ExceptionUtils.getStackTrace(ex));
        }
        return out;
    }

    public List<String> get_branch_list_CRV(String crv) {
        List<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato='0' AND brgr_05 = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, crv);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                    ps.close();
                    rs.close();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public List<Items> get_branch_list(String crv) {
        List<Items> out = new ArrayList<>();
        try {
            String sql = "SELECT cod, de_branch, brgr_05, brgr_03 FROM branch WHERE fg_annullato='0' AND cod <> '000' ";

            if (!crv.equals("000")) {
                sql += "AND brgr_05 = ? ";
            }

            sql += "ORDER BY de_branch";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                if (!crv.equals("000")) {
                    ps.setString(1, crv);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Items(
                                rs.getString(1),
                                rs.getString(2),
                                (rs.getString(1) + " - " + rs.getString(2)).toUpperCase(),
                                rs.getString(3),
                                rs.getString(4)));
                    }
                    rs.close();
                }
                ps.close();

            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public List<Items> get_currency_list() {
        List<Items> out = new ArrayList<>();
        try {
            String sql = "SELECT valuta, de_valuta FROM valute WHERE filiale = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, "000");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Items(
                                rs.getString(1),
                                rs.getString(2),
                                (rs.getString(1) + " - " + rs.getString(2)).toUpperCase()));
                    }
                    rs.close();
                }
                ps.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Items> agevolazioni_e_servizi(Booking bo, String table) {
        ArrayList<Items> out = new ArrayList<>();
        try {
            String sql = "SELECT codice, descrizione, tipologia, perc_value, euro_value, sogliaminima, valuta, branch FROM "
                    + table + " WHERE attivo = 'Y' ";
            if (bo == null) {
                sql += "AND STR_TO_DATE(data_start,'%Y-%m-%d') <= curdate() AND STR_TO_DATE(data_end,'%Y-%m-%d') >= curdate() ORDER BY descrizione";
            } else {
                String dtrit = new SimpleDateFormat("yyyy-MM-dd").format(bo.getDt_ritiro());
                sql += "AND STR_TO_DATE(data_start,'%Y-%m-%d') <= STR_TO_DATE(?, '%Y-%m-%d') AND STR_TO_DATE(data_end,'%Y-%m-%d') >= STR_TO_DATE(?, '%Y-%m-%d') ORDER BY descrizione";
            }
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                if (bo != null) {
                    String dtrit = new SimpleDateFormat("yyyy-MM-dd").format(bo.getDt_ritiro());
                    ps.setString(1, dtrit);
                    ps.setString(2, dtrit);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Items it = new Items(
                                rs.getString(1),
                                rs.getString(2),
                                rs.getString(3),
                                rs.getString(4),
                                rs.getString(5),
                                rs.getString(6),
                                rs.getString(7),
                                rs.getString(8));
                        out.add(it);
                    }
                    rs.close();
                }
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return out;
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
            ArrayList<String> al = list_cod_branch_enabled();
            for (int i = 0; i < al.size(); i++) {
                if (!al.get(i).equals("000")) {
                    filiali = filiali + al.get(i) + comma;
                }
            }
        }

        Iterable<String> parameters = Splitter.on(",").split(filiali);
        Iterator<String> it = parameters.iterator();
        String dtoper = new DateTime().toString(pat2);
        if (dt_val == null) {
            dt_val = formatStringtoStringDate(dtoper, pat2, pat1);
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

    public ArrayList<String> list_cod_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato = ? AND filiale = ? ORDER BY cod";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString("cod"));
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
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
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Booking_Date> list_total_booking() {
        List<Booking_Date> total = new ArrayList<>();
        try (PreparedStatement ps = this.c.prepareStatement(
                "SELECT * FROM sito_prenotazioni WHERE stato='0' OR stato = '3' ORDER BY dt_ritiro");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Booking_Date bd1 = new Booking_Date(
                        rs.getString("cod").replaceFirst(Pattern.quote("cod"), ""),
                        rs.getString("dt_ritiro").replaceFirst(Pattern.quote("dt_ritiro"), ""),
                        rs.getString("stato").replaceFirst(Pattern.quote("stato"), ""),
                        rs.getString("stato_crm").replaceFirst(Pattern.quote("stato_crm"), ""),
                        formatter_N
                                .parseDateTime(rs.getString("dt_ritiro").replaceFirst(Pattern.quote("dt_ritiro"), "")),
                        rs.getString("filiale").replaceFirst(Pattern.quote("filiale"), ""));
                total.add(bd1);
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "EXPIRED_NOSHOW SQL: {0}", ex.getMessage());
        }
        return total;
    }

    public boolean update_status_sito(String cod, String stato, String statoCRM, String filiale) {
        try {
            String upd = "UPDATE sito_prenotazioni SET stato = ?, stato_crm = ? WHERE cod = ?";
            PreparedStatement pstmt = this.c.prepareStatement(upd);
            pstmt.setString(1, stato);
            pstmt.setString(2, statoCRM);
            pstmt.setString(3, cod);

            boolean es1 = pstmt.executeUpdate() > 0;
            pstmt.close(); // Chiudi il PreparedStatement dopo l'uso

            if (es1) {
                // Aggiorna la filiale
                // ---DIFFUSIONE---AGGIORNAMENTI
                insertValue_agg(null, upd, filiale, null, "service");
                return true;
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "UPDATE SQL: {0}", ex.getMessage());
        }
        return false;
    }

    public List<Items> list_Branch_Active() {
        List<Items> output = new ArrayList<>();
        try {
            String s1 = "SELECT cod,de_branch,fg_annullato FROM branch "
                    + "WHERE fg_annullato = ? AND cod <> ? ORDER BY de_branch";
            PreparedStatement ps = this.c.prepareStatement(s1, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, "000");
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                output.add(new Items(rs.getString(1), (rs.getString(2)), (rs.getString(3))));
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "UPDATE SQL: {0}", ex.getMessage());
        }
        return output;
    }

    public void updateSpreadSito() {
        String selectSql = "SELECT valuta, cambio_bce FROM valute WHERE filiale = '000'";
        String updateSql = "UPDATE sito_spread SET cambio_bce = ? WHERE valuta = ?";

        try (Statement stmt = this.c.createStatement();
                ResultSet rs = stmt.executeQuery(selectSql);
                PreparedStatement ps = this.c.prepareStatement(updateSql)) {

            while (rs.next()) {
                ps.setString(1, rs.getString(2));
                ps.setString(2, rs.getString(1));
                boolean success = ps.executeUpdate() > 0;
                insertValue_agg(ps, null, null, null, "site");
                log.log(Level.INFO, "{0} ---- {1}", new Object[] { ps.toString(), success });
            }
            stmt.close();
            rs.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: ERROR {1}",
                    new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }

    }

}
