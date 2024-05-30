package rc.soop.spreadexcel;

import com.google.common.base.Splitter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import static rc.soop.spreadexcel.Utility.patternnormdate;
import static rc.soop.spreadexcel.Utility.patternsqldate;
import static rc.soop.spreadexcel.Utility.visualizzaStringaMySQL;
import org.joda.time.DateTime;
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

    public void setC(Connection c) {
        this.c = c;
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
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            ex.printStackTrace();
            this.c = null;
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
            try (PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, val[1]);
                ps.setString(2, val[2]);
                ps.setString(3, val[3]);
                ps.setString(4, val[0]);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public ArrayList<String> listToUpdate() {
        ArrayList<String> li = new ArrayList<>();
        String sql = "SELECT distinct(filiale) FROM valute WHERE filiale <> '000' AND buy_std = '0.00' AND valuta <> 'EUR'";
        try (PreparedStatement ps = this.c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                li.add(rs.getString(1));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
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
                filiali = filiali + al.get(i) + comma;
            }
        }
        Iterable<String> parameters = Splitter.on(comma).split(filiali);
        Iterator<String> it = parameters.iterator();
        String dtoper = new DateTime().toString(patternsqldate);
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
        String dtoper = new DateTime().toString(patternsqldate);
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

    public boolean updateCurrency(String[] val, String user, String dt_start) {
        try {
            String upd = "UPDATE valute SET buy_std_value = ?, sell_std_value = ?, buy_std = ?, buy_l1 = ?,  buy_l2 = ?,  buy_l3 = ?, buy_best = ?,"
                    + " sell_std = ?, sell_l1 = ?,  sell_l2 = ?,  sell_l3 = ?, sell_best = ?, cambio_vendita = ? WHERE filiale = ? AND valuta = ?";
            try (PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
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

                insertValue_agg(ps, null, "000", dt_start, user);
                insertValue_agg(ps, null, val[0], dt_start, user);

                return true;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public boolean updateCurrencyNOcolumn(String[] val) {
        try {
            String upd = "UPDATE valute SET buy_std = ?, buy_l1 = ?,  buy_l2 = ?,  buy_l3 = ?, buy_best = ?,"
                    + " sell_std = ?, sell_l1 = ?, sell_l2 = ?, sell_l3 = ?, sell_best = ?, cambio_vendita = ? WHERE filiale = ? AND valuta = ?";
            try (PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
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
                return x > 0;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String getNow() {
        try {
            String sql = "SELECT now()";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery();) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new DateTime().toString(patternsqldate);
    }

    public void insert_aggiornamenti_mod(Aggiornamenti_mod am) {
        try {
            String ins = "INSERT INTO aggiornamenti_mod VALUES (?,?,?,?,?,?,?,?)";
            try (PreparedStatement ps = this.c.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
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
            ex.printStackTrace();
        }

    }

    public ArrayList<String> list_cod_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato = ? AND filiale = ? ORDER BY cod";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "0");
                ps.setString(2, "000");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString("cod"));
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> getIpFiliale() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,ip FROM dbfiliali";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] ip = { rs.getString(1), rs.getString(2) };
                    out.add(ip);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> aggfiliali() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s') group by filiale";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] ip = { rs.getString(1), rs.getString(2) };
                    out.add(ip);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String getConf(String id) {
        try {
            String sql = "SELECT des FROM conf WHERE id = ? ";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1).trim();
                    }
                }
            }
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
        String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' GROUP BY filiale";
        try (PreparedStatement ps = this.c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String[] ou = { rs.getString(1), rs.getString(2) };
                li.add(ou);
                System.out.println(
                        "FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : " + rs.getString(2));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public int countA() {
        int agg = 0;
        String sql = "SELECT count(*) FROM aggiornamenti_mod WHERE fg_stato='0' AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s')";
        try (PreparedStatement ps = this.c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                agg += rs.getInt(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            agg = -1;
        }
        return agg;
    }

    public void listagg_in_locale(String filiale) {
        String sql = "SELECT filiale,count(*) FROM aggiornamenti_mod WHERE fg_stato='0' AND filiale = ?";
        try (PreparedStatement ps = this.c.prepareStatement(sql)) {
            ps.setString(1, filiale);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println("FILIALE ORIGINE: " + filiale + " - RISULTATI: " + rs.getString(1) + " : "
                            + rs.getString(2));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public ArrayList<String[]> country() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT nazione,de_nazione,alpha_code,fg_area_geografica FROM nazioni order by de_nazione";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                            rs.getString(4) };
                    out.add(o1);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> identificationCard() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT tipo_documento_identita,de_tipo_documento_identita,OAM_code,reader_robot FROM tipologiadocumento order by de_tipo_documento_identita";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                            rs.getString(4) };
                    out.add(o1);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> getExceldaElaborare() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,fileout,user,dt_start,data FROM excel_upload WHERE stato = ? order by data";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "3");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String[] ou = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getString(5) };
                        out.add(ou);
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public boolean update_stato_excel(String cod, String stato) {
        try {
            String upd = "UPDATE excel_upload SET stato = ? WHERE cod = ?";
            try (PreparedStatement ps = this.c.prepareStatement(upd, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, stato);
                ps.setString(2, cod);
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

}
