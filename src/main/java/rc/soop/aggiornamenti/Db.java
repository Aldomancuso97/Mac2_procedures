/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.aggiornamenti;

import com.google.common.base.Splitter;
import static java.lang.Class.forName;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import static rc.soop.aggiornamenti.Utility.formatDoubleforMysql;
import static rc.soop.aggiornamenti.Utility.formatStringtoStringDate;
import static rc.soop.aggiornamenti.Utility.patternnormdate;
import static rc.soop.aggiornamenti.Utility.patternnormdate_filter;
import static rc.soop.aggiornamenti.Utility.patternsql;
import static rc.soop.aggiornamenti.Utility.patternsqldate;
import static rc.soop.aggiornamenti.Utility.visualizzaStringaMySQL;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.maintenance.Rate;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Db {

    public static final String comma = ",";
    public Connection c = null;
    public String h = null;

    public Connection getC() {
        return c;
    }

    public Connection getC_RAF(String hostfiliale) {
        if (c == null) {
            for (int x = 0; x < 5; x++) {
                Db a1 = new Db(hostfiliale);
                if (a1.getC() != null) {
                    return a1.getC();
                }
            }
        } else {
            return c;
        }
        return null;
    }

    public void setC(Connection c) {
        this.c = c;
    }

    public Db(String hostfiliale) {
        String drivername = "com.mysql.cj.jdbc.Driver";
        String typedb = "mysql";
        String user = "root";
        String pwd = "root";
        try {
            Class.forName(drivername).newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("connectTimeout", "1000");
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + hostfiliale, p);
        } catch (Exception ex) {
            // ex.printStackTrace();
        }
    }

    public Db(String host, boolean filiale) {
        String drivername = rb.getString("db.driver");
        String typedb = rb.getString("db.tipo");
        String user = rb.getString("db.User");
        String pwd = rb.getString("db.pwd");
        if (filiale) {
            user = "root";
            pwd = "root";
        }

        try {
            forName(drivername).newInstance();
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
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            // ex.printStackTrace();
            this.c = null;
        }
    }

    public Db() {
        String drivername = rb.getString("db.driver");
        String typedb = rb.getString("db.tipo");
        String user = rb.getString("db.User");
        String pwd = rb.getString("db.pwd");
        String host = rb.getString("db.ip") + "/maccorpita";

        this.h = host;
        try {
            forName(drivername).newInstance();
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
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            // ex.printStackTrace();
        }
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

    public boolean updateBranch(String[] val) {
        try {
            String upd = "UPDATE branch SET add_via = ?, add_city = ?, add_cap = ? WHERE cod = ?";
            try (PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, val[1]);
                ps.setString(2, val[2]);
                ps.setString(3, val[3]);
                ps.setString(4, val[0]);
                ps.close();
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    

    public ArrayList<String> listToUpdate() {
        ArrayList<String> li = new ArrayList<>();
        try {
            String sql = "SELECT distinct(filiale) FROM valute WHERE filiale <>'000' AND buy_std = '0.00' and valuta<>'EUR'";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                li.add(rs.getString(1));
            }
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public void insertValue_agg(PreparedStatement ps, String statement, String filiali,
            String dt_val, String username, boolean nocentr) {
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
                if (nocentr) {
                    if (!al.get(i).equals("000")) {
                        filiali = filiali + al.get(i) + comma;
                    }
                } else {
                    filiali = filiali + al.get(i) + comma;
                }

            }
        }

        Iterable<String> parameters = Splitter.on(comma).split(filiali);
        Iterator<String> it = parameters.iterator();
        String dtoper = new DateTime().minusHours(3).toString(patternsqldate);
        if (dt_val == null) {
            dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
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

    public boolean updateCurrency(String[] val) {

        try {
            String upd = "UPDATE valute SET buy_std_value = ?, sell_std_value = ?, buy_std = ?, buy_l1 = ?,  buy_l2 = ?,  buy_l3 = ?, buy_best = ?,"
                    + " sell_std = ?, sell_l1 = ?,  sell_l2 = ?,  sell_l3 = ?, sell_best = ?, cambio_vendita = ? WHERE filiale = ? AND valuta = ?";
            PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, val[2]);
            ps.setString(2, val[3]);
            ps.setString(3, val[4]);
            ps.setString(4, val[5]);
            ps.setString(5, val[6]);
            ps.setString(6, val[7]);
            ps.setString(7, val[8]);
            ps.setString(8, val[9]);
            ps.setString(9, val[10]);
            ps.setString(10, val[11]);
            ps.setString(11, val[12]);
            ps.setString(12, val[13]);
            ps.setString(13, val[14]);
            ps.setString(14, val[0]);
            ps.setString(15, val[1]);

            int x = ps.executeUpdate();
            String dtoper = getNow();
            String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);

            insert_aggiornamenti_mod(new Aggiornamenti_mod(
                    Utility.generaId(50), val[0], dt_val, "0",
                    "PS", ps.toString(), "service", dtoper));

            // aggiornamento per filiale
            ps.close();
            return x > 0;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public boolean updateCurrencyNOcolumn(String[] val) {

        try {
            String upd = "UPDATE valute SET buy_std = ?, buy_l1 = ?,  buy_l2 = ?,  buy_l3 = ?, buy_best = ?,"
                    + " sell_std = ?, sell_l1 = ?, sell_l2 = ?, sell_l3 = ?, sell_best = ?, cambio_vendita = ? WHERE filiale = ? AND valuta = ?";
            PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, val[4]);
            ps.setString(2, val[5]);
            ps.setString(3, val[6]);
            ps.setString(4, val[7]);
            ps.setString(5, val[8]);
            ps.setString(6, val[9]);
            ps.setString(7, val[10]);
            ps.setString(8, val[11]);
            ps.setString(9, val[12]);
            ps.setString(10, val[13]);
            ps.setString(11, val[14]);
            ps.setString(12, val[0]);
            ps.setString(13, val[1]);

            int x = ps.executeUpdate();
            String dtoper = getNow();
            String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);

            insert_aggiornamenti_mod(new Aggiornamenti_mod(
                    Utility.generaId(50), val[0], dt_val, "0",
                    "PS", ps.toString(), "service", dtoper));

            // aggiornamento per filiale
            ps.close();
            return x > 0;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String getNow() {
        try {
            String sql = "SELECT now()";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new DateTime().toString(patternsqldate);
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
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }

    public ArrayList<String> list_cod_branch_ALL() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch ORDER BY cod";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString("cod"));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
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
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> getIpFiliale() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,ip FROM dbfiliali";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ip = { rs.getString(1), rs.getString(2) };
                out.add(ip);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> getIpFiliale(String fil) {
        if (fil == null) {
            return getIpFiliale();
        }
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,ip FROM dbfiliali WHERE filiale = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, fil);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String[] ip = { rs.getString(1), rs.getString(2) };
                out.add(ip);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> aggfiliali() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            // String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE
            // fg_stato='0' group by filiale";
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' "
                    + "AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s') group by filiale";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ip = { rs.getString(1), rs.getString(2) };
                out.add(ip);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String getConf(String id) {
        try {
            String sql = "SELECT des FROM conf WHERE id = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1).trim();
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "-";
    }

    public String getH() {
        return h;
    }

    public void setH(String h) {
        this.h = h;
    }

    public ArrayList<String[]> listagg(String filiale) {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod Where fg_stato='0' group by filiale";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String[] ou = { rs.getString(1), rs.getString(2) };
                li.add(ou);
                System.out.println(
                        "FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : " + rs.getString(2));
            }
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public int countA() {
        int agg = 0;
        try {
            // String sql = "SELECT count(*) FROM aggiornamenti_mod Where fg_stato='0' ";
            String sql = "SELECT count(cod) FROM aggiornamenti_mod Where fg_stato='0' AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s'); ";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                agg = agg + rs.getInt(1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            agg = -1;
        }
        return agg;
    }

    public void listagg_in_locale(String filiale) {
        try {
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod Where fg_stato='0' AND filiale = '" + filiale
                    + "' group by filiale";

            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(
                        "FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : " + rs.getString(2));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public ArrayList<String[]> country() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT nazione,de_nazione,alpha_code,fg_area_geografica FROM nazioni order by de_nazione";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                        rs.getString(4) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> identificationCard() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT tipo_documento_identita,de_tipo_documento_identita,OAM_code,reader_robot FROM tipologiadocumento order by de_tipo_documento_identita";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                        rs.getString(4) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public boolean updatePSWOLTA(String[] valori) {
        try {
            String upd = "UPDATE branch SET olta_user = ?, olta_psw = ? WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, valori[1]);
            ps.setString(2, valori[2]);
            ps.setString(3, valori[0]);
            boolean es = ps.executeUpdate() > 0;
            if (es) {
                insertValue_agg(ps, null, valori[0], null, "service", true);
            }
            ps.close();
            return es;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String get_BCE_CZK(String giorno) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql = "SELECT modify FROM rate_history where filiale = '000' AND valuta = 'CZK' "
                    + "AND modify like '%bce value%' AND dt_mod < '" + giorno + " 23:59:59' order by dt_mod DESC";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (giorno.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(giorno).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        giorno = sqldate.parseDateTime(giorno).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "0";
    }

    public String get_BCE_GBP(String giorno) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql = "SELECT modify FROM rate_history where filiale = '000' AND valuta = 'GBP' "
                    + "AND modify like '%bce value%' AND dt_mod < '" + giorno + " 23:59:59' order by dt_mod DESC";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (giorno.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(giorno).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        giorno = sqldate.parseDateTime(giorno).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "0";
    }

    public String get_BCE_USD(String giorno) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql = "SELECT modify FROM rate_history where filiale = '000' AND valuta = 'USD' "
                    + "AND modify like '%bce value%' AND dt_mod < '" + giorno + " 23:59:59' order by dt_mod DESC";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (giorno.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(giorno).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        giorno = sqldate.parseDateTime(giorno).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "0";
    }

    public String get_BCE(String giorno, String valuta) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql = "SELECT modify FROM rate_history where filiale = '000' AND valuta = ? "
                    + "AND modify like '%bce value%' AND dt_mod < ? order by dt_mod DESC";
            PreparedStatement statement = this.c.prepareStatement(sql);
            statement.setString(1, valuta);
            statement.setString(2, giorno + " 23:59:59");

            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (giorno.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(giorno).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        giorno = sqldate.parseDateTime(giorno).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
                statement.close();
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "0";
    }

    public boolean insert_RATE_AGG(Rate r) {
        try {
            String ins = "INSERT INTO rate VALUES (?,?,?,?)";
            PreparedStatement ps = this.c.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, r.getData());
            ps.setString(2, r.getValuta());
            ps.setString(3, r.getFiliale());
            ps.setString(4, r.getRif_bce());

            insertValue_agg(ps, null, null, null, "service", true);
            ps.close();
            return true;
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                return true;
            }
            ex.printStackTrace();
        }
        return false;
    }

    public boolean insert_RATE(Rate r) {
        try {
            String selectQuery = "SELECT data FROM rate WHERE data = ? AND valuta = ? AND filiale = ?";
            try (PreparedStatement psSelect = this.c.prepareStatement(selectQuery)) {
                psSelect.setString(1, r.getData());
                psSelect.setString(2, r.getValuta());
                psSelect.setString(3, r.getFiliale());
                ResultSet rs = psSelect.executeQuery();
                if (rs.next()) {
                    return true;
                }
                psSelect.close();
            }
            String insertQuery = "INSERT INTO rate VALUES (?,?,?,?)";
            try (PreparedStatement psInsert = this.c.prepareStatement(insertQuery)) {
                psInsert.setString(1, r.getData());
                psInsert.setString(2, r.getValuta());
                psInsert.setString(3, r.getFiliale());
                psInsert.setString(4, r.getRif_bce());
                psInsert.executeUpdate();
            }
            return true;
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                return true;
            }
            ex.printStackTrace();
        }
        return false;
    }

    public ArrayList<String> listCurrency(String valuta) {
        ArrayList<String> li = new ArrayList<>();
        try {
            String sql = "SELECT distinct(valuta) FROM valute WHERE filiale = '000' AND valuta<>'" + valuta + "'";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery(sql);

            while (rs.next()) {
                li.add(rs.getString(1));
            }
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public String getPath(String cod) {
        try {
            String sql = "SELECT descr FROM path WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
