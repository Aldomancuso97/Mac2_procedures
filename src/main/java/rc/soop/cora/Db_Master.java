/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.cora;

import com.google.common.util.concurrent.AtomicDouble;
import static java.lang.Class.forName;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import static rc.soop.cora.CORA.estraicodicecognomenome;
import static rc.soop.cora.CORA.patternnormdate_filter;
import static rc.soop.cora.CORA.patternsql;
import static rc.soop.cora.CORA.patternsqldate;
import static rc.soop.cora.MacCORA.log;
import static rc.soop.cora.MacCORA.rb;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
            String host = rb.getString("db.ip") + "/maccorpita";
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
            this.c = null;
            log.severe(ex.getMessage());
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
            log.severe(ex.getMessage());
        }
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
            log.severe(ex.getMessage());
        }
        return "-";
    }

    public String getPath(String cod) {
        try {
            String sql = "SELECT descr FROM path WHERE cod = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, cod);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public ArrayList<String[]> getValuteForMonitor(String filiale) {
        ArrayList<String[]> al = new ArrayList<>();
        String query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                + "FROM valute where fg_valuta_corrente='0' AND filiale = ?";

        if (filiale.equals("---")) {
            query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                    + "FROM valute where fg_valuta_corrente='0' AND filiale <>'000' ";
        }

        query += " ORDER BY filiale, valuta";

        try (PreparedStatement ps = this.c.prepareStatement(query)) {
            if (!filiale.equals("---")) {
                ps.setString(1, filiale);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] var1 = {
                            rs.getString("valuta"),
                            rs.getString("de_valuta"),
                            rs.getString("cambio_bce"),
                            rs.getString("buy_std_value"),
                            rs.getString("buy_std_type"),
                            rs.getString("buy_std"),
                            rs.getString("sell_std_value"),
                            rs.getString("sell_std_type"),
                            rs.getString("sell_std"),
                            rs.getString("filiale"),
                            rs.getString("enable_buy"),
                            rs.getString("enable_sell")
                    };
                    al.add(var1);
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return al;
    }

    public ArrayList<Branch> list_branch_enabled() {
        ArrayList<Branch> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch WHERE fg_annullato = ? ORDER BY de_branch";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "0");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Branch ba = new Branch();
                        ba.setFiliale(rs.getString("filiale"));
                        ba.setCod(rs.getString("cod"));
                        ba.setDe_branch(visualizzaStringaMySQL(rs.getString("de_branch")));
                        ba.setAdd_city(visualizzaStringaMySQL(rs.getString("add_city")));
                        ba.setAdd_cap(visualizzaStringaMySQL(rs.getString("add_cap")));
                        ba.setAdd_via(visualizzaStringaMySQL(rs.getString("add_via")));
                        ba.setFg_persgiur(rs.getString("fg_persgiur"));
                        ba.setProv_raccval(rs.getString("prov_raccval"));
                        ba.setDa_annull(
                                formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
                        ba.setFg_annullato(rs.getString("fg_annullato"));
                        ba.setG01(rs.getString("g01"));
                        ba.setG02(rs.getString("g02"));
                        ba.setG03(rs.getString("g03"));
                        ba.setFg_modrate(rs.getString("fg_modrate"));
                        ba.setFg_crm(rs.getString("fg_crm"));
                        ba.setFg_agency(rs.getString("fg_agency"));
                        ba.setFg_pad(rs.getString("fg_pad"));
                        ba.setDt_start(rs.getString("dt_start"));
                        ba.setMax_ass(rs.getString("max_ass"));
                        ba.setTarget(rs.getString("target"));
                        ba.setBrgr_01(rs.getString("brgr_01"));
                        ba.setBrgr_02(rs.getString("brgr_02"));
                        ba.setBrgr_03(rs.getString("brgr_03"));
                        ba.setBrgr_04(rs.getString("brgr_04"));
                        ba.setBrgr_05(rs.getString("brgr_05"));
                        ba.setBrgr_06(rs.getString("brgr_06"));
                        ba.setBrgr_07(rs.getString("brgr_07"));
                        ba.setBrgr_08(rs.getString("brgr_08"));
                        ba.setBrgr_09(rs.getString("brgr_09"));
                        ba.setBrgr_10(rs.getString("brgr_10"));
                        ba.setListagruppi();
                        out.add(ba);
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    private static String visualizzaStringaMySQL(String ing) {
        if (ing == null) {
            return "";
        }
        ing = StringUtils.replace(ing, "\\'", "'");
        ing = StringUtils.replace(ing, "\'", "'");
        ing = StringUtils.replace(ing, "\"", "'");
        return ing.trim();
    }

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
            log.severe(ex.getMessage());
        }
        return dat;
    }

    public Client query_Client_transaction(String codtr, String codcl) {
        try {
            String sql = "SELECT * FROM ch_transaction_client WHERE codtr = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, codtr);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Client bl = new Client();
                        bl.setCode(rs.getString("codcl"));
                        bl.setCognome(rs.getString("cognome"));
                        bl.setNome(rs.getString("nome"));
                        bl.setSesso(rs.getString("sesso"));
                        bl.setCodfisc(rs.getString("codfisc"));
                        bl.setNazione(rs.getString("nazione"));
                        bl.setCitta(rs.getString("citta"));
                        bl.setIndirizzo(visualizzaStringaMySQL(rs.getString("indirizzo")));
                        bl.setCap(rs.getString("cap"));
                        bl.setProvincia(rs.getString("provincia"));
                        bl.setCitta_nascita(rs.getString("citta_nascita"));
                        bl.setProvincia_nascita(rs.getString("provincia_nascita"));
                        bl.setNazione_nascita(rs.getString("nazione_nascita"));
                        bl.setDt_nascita(rs.getString("dt_nascita"));
                        bl.setTipo_documento(rs.getString("tipo_documento"));
                        bl.setNumero_documento(rs.getString("numero_documento"));
                        bl.setDt_rilascio_documento(rs.getString("dt_rilascio_documento"));
                        bl.setDt_scadenza_documento(rs.getString("dt_scadenza_documento"));
                        bl.setRilasciato_da_documento(rs.getString("rilasciato_da_documento"));
                        bl.setLuogo_rilascio_documento(rs.getString("luogo_rilascio_documento"));
                        bl.setEmail(rs.getString("email"));
                        bl.setTelefono(rs.getString("telefono"));
                        bl.setPerc_buy(rs.getString("perc_buy"));
                        bl.setPerc_sell(rs.getString("perc_sell"));
                        bl.setTimestamp(rs.getString("timestamp"));
                        bl.setPep(rs.getString("pep"));
                        bl.setDatatr(rs.getString("codtr"));
                        return bl;
                    } else {
                        sql = "SELECT * FROM ch_transaction_client WHERE codcl = ?";
                        try (PreparedStatement ps1 = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE)) {
                            ps1.setString(1, codcl);
                            try (ResultSet rs1 = ps.executeQuery()) {
                                if (rs1.next()) {
                                    Client bl = new Client();
                                    bl.setCode(rs1.getString("codcl"));
                                    bl.setCognome(rs1.getString("cognome"));
                                    bl.setNome(rs1.getString("nome"));
                                    bl.setSesso(rs1.getString("sesso"));
                                    bl.setCodfisc(rs1.getString("codfisc"));
                                    bl.setNazione(rs1.getString("nazione"));
                                    bl.setCitta(rs1.getString("citta"));
                                    bl.setIndirizzo(visualizzaStringaMySQL(rs1.getString("indirizzo")));
                                    bl.setCap(rs1.getString("cap"));
                                    bl.setProvincia(rs1.getString("provincia"));
                                    bl.setCitta_nascita(rs1.getString("citta_nascita"));
                                    bl.setProvincia_nascita(rs1.getString("provincia_nascita"));
                                    bl.setNazione_nascita(rs1.getString("nazione_nascita"));
                                    bl.setDt_nascita(rs1.getString("dt_nascita"));
                                    bl.setTipo_documento(rs1.getString("tipo_documento"));
                                    bl.setNumero_documento(rs1.getString("numero_documento"));
                                    bl.setDt_rilascio_documento(rs1.getString("dt_rilascio_documento"));
                                    bl.setDt_scadenza_documento(rs1.getString("dt_scadenza_documento"));
                                    bl.setRilasciato_da_documento(rs1.getString("rilasciato_da_documento"));
                                    bl.setLuogo_rilascio_documento(rs1.getString("luogo_rilascio_documento"));
                                    bl.setEmail(rs1.getString("email"));
                                    bl.setTelefono(rs1.getString("telefono"));
                                    bl.setPerc_buy(rs1.getString("perc_buy"));
                                    bl.setPerc_sell(rs1.getString("perc_sell"));
                                    bl.setTimestamp(rs1.getString("timestamp"));
                                    bl.setPep(rs1.getString("pep"));
                                    bl.setDatatr(rs1.getString("codtr"));
                                    return bl;
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return query_Client(codcl);
    }

    public Client query_Client(String cod) {
        try {
            String sql = "SELECT * FROM anagrafica_ru where ndg = ? limit 1";
            try (PreparedStatement ps1 = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps1.setString(1, cod);
                try (ResultSet rs1 = ps1.executeQuery()) {
                    if (rs1.next()) {
                        Client bl = new Client();
                        bl.setCode(rs1.getString("ndg"));
                        bl.setCognome(rs1.getString("cognome"));
                        bl.setNome(rs1.getString("nome"));
                        bl.setSesso(rs1.getString("sesso"));
                        bl.setCodfisc(rs1.getString("codice_fiscale"));
                        bl.setNazione(rs1.getString("paese_estero_residenza"));
                        if (rs1.getString("citta").trim().equals("")) {
                            bl.setCitta(rs1.getString("cab_comune"));
                        } else {
                            bl.setCitta(rs1.getString("citta"));
                        }
                        bl.setIndirizzo(visualizzaStringaMySQL(rs1.getString("indirizzo")));
                        bl.setCap(rs1.getString("cap"));
                        bl.setProvincia(rs1.getString("provincia"));
                        bl.setCitta_nascita(rs1.getString("comune_nascita"));
                        bl.setProvincia_nascita(rs1.getString("cod_provincia_nascita"));
                        bl.setNazione_nascita(rs1.getString("paese_estero_residenza"));
                        bl.setDt_nascita(formatStringtoStringDate(rs1.getString("dt_nascita"), patternsql,
                                patternnormdate_filter));
                        bl.setTipo_documento(rs1.getString("tipo_documento"));
                        bl.setNumero_documento(rs1.getString("numero_documento"));
                        bl.setDt_rilascio_documento(formatStringtoStringDate(rs1.getString("dt_rilascio"), patternsql,
                                patternnormdate_filter));
                        bl.setDt_scadenza_documento(formatStringtoStringDate(rs1.getString("dt_scadenza"), patternsql,
                                patternnormdate_filter));
                        bl.setRilasciato_da_documento(rs1.getString("autorita_rilascio"));
                        bl.setLuogo_rilascio_documento(rs1.getString("luogo_rilascio_documento"));
                        bl.setEmail("");
                        bl.setTelefono("");
                        bl.setPerc_buy("-");
                        bl.setPerc_sell("-");
                        bl.setTimestamp("");
                        bl.setPep("NO");
                        return bl;
                    }
                }
            }

        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public ArrayList<Branch> list_branch_enabledB() {
        ArrayList<Branch> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch WHERE fg_annullato = ? ORDER BY de_branch";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "0");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Branch ba = new Branch();
                        ba.setFiliale(rs.getString("filiale"));
                        ba.setCod(rs.getString("cod"));
                        ba.setDe_branch(visualizzaStringaMySQL(rs.getString("de_branch")));
                        ba.setAdd_city(visualizzaStringaMySQL(rs.getString("add_city")));
                        ba.setAdd_cap(visualizzaStringaMySQL(rs.getString("add_cap")));
                        ba.setAdd_via(visualizzaStringaMySQL(rs.getString("add_via")));
                        ba.setFg_persgiur(rs.getString("fg_persgiur"));
                        ba.setProv_raccval(rs.getString("prov_raccval"));
                        ba.setDa_annull(
                                formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
                        ba.setFg_annullato(rs.getString("fg_annullato"));
                        ba.setG01(rs.getString("g01"));
                        ba.setG02(rs.getString("g02"));
                        ba.setG03(rs.getString("g03"));
                        ba.setFg_modrate(rs.getString("fg_modrate"));
                        ba.setFg_crm(rs.getString("fg_crm"));
                        ba.setFg_agency(rs.getString("fg_agency"));
                        ba.setFg_pad(rs.getString("fg_pad"));
                        ba.setDt_start(rs.getString("dt_start"));
                        ba.setMax_ass(rs.getString("max_ass"));
                        ba.setTarget(rs.getString("target"));
                        ba.setBrgr_01(rs.getString("brgr_01"));
                        ba.setBrgr_02(rs.getString("brgr_02"));
                        ba.setBrgr_03(rs.getString("brgr_03"));
                        ba.setBrgr_04(rs.getString("brgr_04"));
                        ba.setBrgr_05(rs.getString("brgr_05"));
                        ba.setBrgr_06(rs.getString("brgr_06"));
                        ba.setBrgr_07(rs.getString("brgr_07"));
                        ba.setBrgr_08(rs.getString("brgr_08"));
                        ba.setBrgr_09(rs.getString("brgr_09"));
                        ba.setBrgr_10(rs.getString("brgr_10"));
                        ba.setListagruppi();
                        out.add(ba);
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public Branch get_branch(String cod) {
        try {
            String sql = "SELECT * FROM branch WHERE cod = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, cod);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Branch ba = new Branch();
                        ba.setFiliale(rs.getString("filiale"));
                        ba.setCod(rs.getString("cod"));
                        ba.setDe_branch(visualizzaStringaMySQL(rs.getString("de_branch")));
                        ba.setAdd_city(visualizzaStringaMySQL(rs.getString("add_city")));
                        ba.setAdd_cap(rs.getString("add_cap"));
                        ba.setAdd_via(visualizzaStringaMySQL(rs.getString("add_via")));
                        ba.setFg_persgiur(rs.getString("fg_persgiur"));
                        ba.setProv_raccval(rs.getString("prov_raccval"));
                        ba.setDa_annull(
                                formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
                        ba.setFg_annullato(rs.getString("fg_annullato"));
                        ba.setG01(rs.getString("g01"));
                        ba.setG02(rs.getString("g02"));
                        ba.setG03(rs.getString("g03"));
                        ba.setFg_modrate(rs.getString("fg_modrate"));
                        ba.setFg_crm(rs.getString("fg_crm"));
                        ba.setOlta_user(rs.getString("olta_user"));
                        ba.setOlta_pass(rs.getString("olta_psw"));
                        ba.setFg_pad(rs.getString("fg_pad"));
                        ba.setPay_nomeazienda(rs.getString("pay_nomeazienda"));
                        ba.setPay_idazienda(rs.getString("pay_idazienda"));
                        ba.setPay_skin(rs.getString("pay_skin"));
                        ba.setPay_user(rs.getString("pay_user"));
                        ba.setPay_password(rs.getString("pay_password"));
                        ba.setPay_token(rs.getString("pay_token"));
                        ba.setPay_terminale(rs.getString("pay_terminale"));
                        ba.setFg_agency(rs.getString("fg_agency"));
                        ba.setDt_start(rs.getString("dt_start"));
                        ba.setMax_ass(rs.getString("max_ass"));
                        ba.setTarget(rs.getString("target"));
                        ba.setBrgr_01(rs.getString("brgr_01"));
                        ba.setBrgr_02(rs.getString("brgr_02"));
                        ba.setBrgr_03(rs.getString("brgr_03"));
                        ba.setBrgr_04(rs.getString("brgr_04"));
                        ba.setBrgr_05(rs.getString("brgr_05"));
                        ba.setBrgr_06(rs.getString("brgr_06"));
                        ba.setBrgr_07(rs.getString("brgr_07"));
                        ba.setBrgr_08(rs.getString("brgr_08"));
                        ba.setBrgr_09(rs.getString("brgr_09"));
                        ba.setBrgr_10(rs.getString("brgr_10"));
                        ba.setListagruppi();
                        return ba;
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public List<Object_DB> district() {
        List<Object_DB> out = new ArrayList<>();
        try {
            String sql = "SELECT provincia,de_provincia FROM province group by provincia order by de_provincia";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Object_DB(rs.getString(1), rs.getString(2).toUpperCase()));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
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
            log.severe(ex.getMessage());
        }
        return out;
    }

    public ArrayList<Currency> list_figures() {
        ArrayList<Currency> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM valute WHERE filiale = ? ORDER BY valuta";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, "000");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Currency cu = new Currency();
                        cu.setCode(rs.getString("valuta"));
                        cu.setDescrizione(visualizzaStringaMySQL(rs.getString("de_valuta")));
                        cu.setChange_buy(rs.getString("cambio_acquisto"));
                        cu.setChange_sell(rs.getString("cambio_vendita"));

                        cu.setUic(rs.getString("codice_uic_divisa"));
                        cu.setMessage(rs.getString("de_messaggio"));
                        cu.setInternal_cur(rs.getString("fg_valuta_corrente"));
                        cu.setId(rs.getString("id"));
                        cu.setFilial(rs.getString("filiale"));

                        cu.setBuy_std(rs.getString("buy_std"));
                        cu.setBuy_l1(rs.getString("buy_l1"));
                        cu.setBuy_l2(rs.getString("buy_l2"));
                        cu.setBuy_l3(rs.getString("buy_l3"));
                        cu.setBuy_best(rs.getString("buy_best"));

                        cu.setSell_std(rs.getString("sell_std"));
                        cu.setSell_l1(rs.getString("sell_l1"));
                        cu.setSell_l2(rs.getString("sell_l2"));
                        cu.setSell_l3(rs.getString("sell_l3"));
                        cu.setSell_best(rs.getString("sell_best"));

                        cu.setEnable_buy(rs.getString("enable_buy"));
                        cu.setEnable_sell(rs.getString("enable_sell"));

                        cu.setCambio_bce(rs.getString("cambio_bce"));
                        cu.setBuy_std_type(rs.getString("buy_std_type"));
                        cu.setBuy_std_value(rs.getString("buy_std_value"));
                        cu.setSell_std_type(rs.getString("sell_std_type"));
                        cu.setSell_std_value(rs.getString("sell_std_value"));

                        out.add(cu);
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
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
            log.severe(ex.getMessage());
        }
        return out;
    }

    public DateTime getNowDT() {
        try {
            String sql = "SELECT now()";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsqldate);
                    return formatter.parseDateTime(rs.getString(1).substring(0, 19));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return new DateTime();
    }

    public ArrayList<Client> getTransazioni(String data, String f1, String f2) {
        ArrayList<Client> out = new ArrayList<>();
        ArrayList<Client> cl = new ArrayList<>();
        try {
            String query = "SELECT cod, cl_cod, data FROM ch_transaction WHERE del_fg='0' AND data LIKE ?";
            if (f1 != null && f2 != null) {
                query += " AND cl_cod NOT IN (SELECT cl_cod FROM ch_transaction WHERE del_fg='0' AND data BETWEEN ? AND ?)";
            }
            try (PreparedStatement ps = this.c.prepareStatement(query)) {
                ps.setString(1, data + "%");
                if (f1 != null && f2 != null) {
                    ps.setString(2, f1 + " 00:00:00");
                    ps.setString(3, f2 + " 23:59:59");
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Client cL = query_Client_transaction(rs.getString("cod"), rs.getString("cl_cod"));
                        System.out.println(" - " + rs.getString("cod"));
                        boolean add = true;
                        for (int i = 0; i < cl.size(); i++) {
                            Client dacontrollare = cl.get(i);
                            if (dacontrollare.getCode().equalsIgnoreCase(cL.getCode())) {
                                add = false;
                                break;
                            } else if (dacontrollare.getCodfisc().equals("---")) {
                                if (dacontrollare.getNome().trim().equalsIgnoreCase(cL.getNome().trim())
                                        && dacontrollare.getCognome().trim().equalsIgnoreCase(cL.getCognome().trim())
                                        && dacontrollare.getNazione_nascita().trim()
                                                .equalsIgnoreCase(cL.getNazione_nascita().trim())
                                        && dacontrollare.getDt_nascita().trim()
                                                .equalsIgnoreCase(cL.getDt_nascita().trim())) {
                                    add = false;
                                    break;
                                }
                            } else {
                                if (dacontrollare.getCodfisc().equalsIgnoreCase(cL.getCodfisc().trim())) {
                                    add = false;
                                    break;
                                }
                            }
                        }
                        if (add) {
                            cl.add(cL);
                            cL.setDatatr(formatStringtoStringDate(rs.getString("data"), patternsqldate,
                                    patternnormdate_filter));
                            out.add(cL);
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public ArrayList<String[]> city_Italy_APM() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM comuni_apm ORDER BY denominazione";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)) };
                    out.add(o1);
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public String generacodiceCORA(String codcll, String anno, String mese, String datafile, String cognome,
            String nome) {
        try {
            String sql = "SELECT codice FROM cora_codici WHERE codicecliente = ? AND anno = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, codcll);
                ps.setString(2, anno);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    } else {
                        String id = "";
                        String count = "SELECT count(codice) FROM cora_codici WHERE anno = ?";
                        try (PreparedStatement psCount = this.c.prepareStatement(count,
                                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                            psCount.setString(1, anno);
                            try (ResultSet rscount = psCount.executeQuery()) {
                                if (rscount.next()) {
                                    int iii = rscount.getInt(1) + 1;
                                    id = StringUtils.leftPad(String.valueOf(iii), 9, "0");
                                }
                                String newcod = "12951210157_" + anno + mese + "_" + datafile + "_"
                                        + estraicodicecognomenome(cognome, nome) + "_" + id;
                                String insert = "INSERT INTO cora_codici VALUES (?,?,?)";
                                try (PreparedStatement psInsert = this.c.prepareStatement(insert,
                                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                                    psInsert.setString(1, newcod);
                                    psInsert.setString(2, anno);
                                    psInsert.setString(3, codcll);
                                    psInsert.executeUpdate();
                                    return newcod;
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public boolean insertCORA(String datarif, String content, String tipo) {
        try {

            String ins = "INSERT INTO cora VALUES ('" + datarif + "','" + content + "','" + tipo + "')";
            try (Statement st = this.c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                st.execute(ins);
                return true;
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return false;
    }

    public Transaction getTransaction(String cl_cod) {
        try {
            String sql = "SELECT tr.data, va.valuta, va.quantita, va.total FROM ch_transaction tr, ch_transaction_valori va WHERE tr.cl_cod=? AND tr.cod=va.cod_tr AND tr.del_fg='0' ORDER BY tr.data ASC";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, cl_cod);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Transaction> TOTAL = new ArrayList<>();
                    AtomicDouble controval_cora = new AtomicDouble(0.00);
                    while (rs.next()) {
                        String valuta = rs.getString(2);
                        String quantita = rs.getString(3);
                        String new_rate = rb.getString(valuta);
                        
                        double quantityValue = fd(quantita);
                        double newRateValue = fd(new_rate);
                        
                        if (newRateValue != 0.0) {
                            double newtotal = quantityValue / newRateValue;
                            controval_cora.addAndGet(newtotal);
                            TOTAL.add(new Transaction(rs.getString(1), "", ""));
                        } else {
                            // Gestione dell'errore: il nuovo tasso è zero
                            System.err.println("Errore: Impossibile eseguire la divisione perché il nuovo tasso è zero.");
                        }
                    }
                    if (!TOTAL.isEmpty()) {
                        return new Transaction(TOTAL.get(0).getData_inizio(),
                                roundDoubleandFormat(controval_cora.get(), 2), String.valueOf(TOTAL.size()));
                    }
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }
    

    public List<Codici> getCORA(String anno) {
        List<Codici> out = new ArrayList<>();
        String sql = "SELECT * FROM maccorpita.cora_codici WHERE anno = ?";
        try (PreparedStatement ps = this.c.prepareStatement(sql)) {
            ps.setString(1, anno);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Codici(rs.getString(1), rs.getString(2), rs.getString(3)));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public static double fd(String si_t_old) {
        double d1;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (NumberFormatException e) {
            d1 = 0.0D;
        }
        return d1;
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static double roundDouble(double d, int scale) {
        d = new BigDecimal(d).setScale(scale, RoundingMode.HALF_DOWN).doubleValue();
        return d;
    }

    public List<Codici> getCORA_REC() {
        List<Codici> out = new ArrayList<>();
        String sql = "SELECT * FROM cora_codici WHERE codicecliente IN (?, ?, ?, ?)";
        try (PreparedStatement ps = this.c.prepareStatement(sql)) {
            ps.setString(1, "180129180212880Sy5ObUlTfS");
            ps.setString(2, "180117125231435EPhuOb1H1f");
            ps.setString(3, "180109104122550LLODOPZA2K");
            ps.setString(4, "180123162523760MhxnRHqcCj");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Codici(rs.getString(1), rs.getString(2), rs.getString(3)));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

}
