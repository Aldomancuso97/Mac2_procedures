/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.oam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import static rc.soop.oam.MacOAM.log;
import static rc.soop.oam.OAM.patternnormdate_filter;
import static rc.soop.oam.OAM.patternsql;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
            String host = rb.getString("db.ip") + "/" + rb.getString("pdf.db.name");
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
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1).trim();
            }
            ps.close();
            rs.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return "-";
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
            rs.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public ArrayList<Branch> list_branch_enabled() {
        ArrayList<Branch> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch WHERE fg_annullato = ? ORDER BY de_branch";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ResultSet rs = ps.executeQuery();
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
                ba.setDa_annull(formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
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
            rs.close();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
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
            log.severe(dat + "!" + ex.getMessage());
        }
        return null;
    }

    public Client query_Client_transaction(String codtr, String codcl) {
        try {
            String sql = "SELECT * FROM ch_transaction_client WHERE codtr = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, codtr);
                ResultSet rs = ps.executeQuery();

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
                    return bl;
                }
                ps.close();
            }

            sql = "SELECT * FROM ch_transaction_client WHERE codcl = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, codcl);
                ResultSet rs = ps.executeQuery();
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
                    return bl;
                }
                ps.close();
            }
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return query_Client(codcl);
    }

    public Client query_Client(String cod) {
        try {
            String sql = "SELECT * FROM anagrafica_ru where ndg = ? limit 1";
            PreparedStatement ps1 = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps1.setString(1, cod);
            ResultSet rs1 = ps1.executeQuery();
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
                bl.setDt_nascita(
                        formatStringtoStringDate(rs1.getString("dt_nascita"), patternsql, patternnormdate_filter));
                bl.setTipo_documento(rs1.getString("tipo_documento"));
                bl.setNumero_documento(rs1.getString("numero_documento"));
                bl.setDt_rilascio_documento(
                        formatStringtoStringDate(rs1.getString("dt_rilascio"), patternsql, patternnormdate_filter));
                bl.setDt_scadenza_documento(
                        formatStringtoStringDate(rs1.getString("dt_scadenza"), patternsql, patternnormdate_filter));
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
            ps1.close();

        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
    }

    public ArrayList<Ch_transaction_value> query_transaction_value(String cod_tr) {
        ArrayList<Ch_transaction_value> li = new ArrayList<>();
        try {
            String sql = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod_tr);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Ch_transaction_value chv = new Ch_transaction_value();
                chv.setId(rs.getString(1));
                chv.setCod_tr(rs.getString(2));
                chv.setNumeroriga(rs.getString(3));
                chv.setSupporto(rs.getString(4));
                chv.setPos(rs.getString(5));
                chv.setValuta(rs.getString(6));
                chv.setQuantita(rs.getString(7));
                chv.setRate(rs.getString(8));
                chv.setCom_perc(rs.getString(9));
                chv.setCom_perc_tot(rs.getString(10));
                chv.setFx_com(rs.getString(11));
                chv.setTot_com(rs.getString(12));
                chv.setNet(rs.getString(13));
                chv.setSpread(rs.getString(14));
                chv.setTotal(rs.getString(15));
                chv.setKind_fix_comm(rs.getString(16));
                chv.setLow_com_ju(rs.getString(17));
                chv.setBb(rs.getString(18));
                chv.setBb_fidcode(rs.getString(19));
                chv.setDt_tr(rs.getString(20));
                chv.setContr_valuta(rs.getString(21));
                chv.setContr_supporto(rs.getString(22));
                chv.setContr_quantita(rs.getString(23));
                chv.setDel_fg(rs.getString(24));
                chv.setDel_dt(rs.getString(25));
                chv.setPosnum(rs.getString(26));
                chv.setRoundvalue(rs.getString(27));
                li.add(chv);
            }
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return li;
    }

    public ArrayList<Branch> list_branch_enabledB() {
        ArrayList<Branch> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch ORDER BY de_branch";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
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
                ba.setDa_annull(formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
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
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public Branch get_branch(String cod) {

        try {
            String sql = "SELECT * FROM branch WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
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
                ba.setDa_annull(formatStringtoStringDate(rs.getString("da_annull"), "yyyy-MM-dd", "dd/MM/yyyy"));
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
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return null;
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
            log.severe(ex.getMessage());
        }
        return out;
    }

    public ArrayList<Currency> list_figures() {
        ArrayList<Currency> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM valute WHERE filiale = ? ORDER BY valuta";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "000");
            ResultSet rs = ps.executeQuery();
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
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public ArrayList<Ch_transaction> list_transaction_oam(String anno, String mese) {
        ArrayList<Ch_transaction> out = new ArrayList<>();
        String sel = "SELECT * FROM ch_transaction ch WHERE data LIKE ? AND del_fg='0' ORDER BY cl_cod";

        try (PreparedStatement pstmt = this.c.prepareStatement(sel)) {
            pstmt.setString(1, anno + "-" + mese + "%");
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                Ch_transaction ch = new Ch_transaction();
                ch.setCod(rs.getString("cod"));
                ch.setId(StringUtils.leftPad(rs.getString("id"), 15, "0"));
                ch.setFiliale(rs.getString("filiale"));
                ch.setTipotr(rs.getString("tipotr"));
                ch.setUser(rs.getString("user"));
                ch.setTill(rs.getString("till"));
                ch.setData(rs.getString("data"));
                ch.setTipocliente(rs.getString("tipocliente"));
                ch.setId_open_till(rs.getString("id_open_till"));
                ch.setPay(rs.getString("pay"));
                ch.setTotal(rs.getString("total"));
                ch.setFix(rs.getString("fix"));
                ch.setCom(rs.getString("com"));
                ch.setRound(rs.getString("round"));
                ch.setCommission(rs.getString("commission"));
                ch.setSpread_total(rs.getString("spread_total"));
                ch.setNote(rs.getString("note"));
                ch.setAgency(rs.getString("agency"));
                ch.setAgency_cod(rs.getString("agency_cod"));
                ch.setLocalfigures(rs.getString("localfigures"));
                ch.setPos(rs.getString("pos"));
                ch.setIntbook(rs.getString("intbook"));
                ch.setIntbook_type(rs.getString("intbook_type"));
                ch.setIntbook_1_tf(rs.getString("intbook_1_tf"));
                ch.setIntbook_1_mod(rs.getString("intbook_1_mod"));
                ch.setIntbook_1_val(rs.getString("intbook_1_val"));
                ch.setIntbook_2_tf(rs.getString("intbook_2_tf"));
                ch.setIntbook_2_mod(rs.getString("intbook_2_mod"));
                ch.setIntbook_2_val(rs.getString("intbook_2_val"));
                ch.setIntbook_3_tf(rs.getString("intbook_3_tf"));
                ch.setIntbook_3_mod(rs.getString("intbook_3_mod"));
                ch.setIntbook_3_val(rs.getString("intbook_3_val"));
                ch.setIntbook_mac(rs.getString("intbook_mac"));
                ch.setIntbook_cli(rs.getString("intbook_cli"));
                ch.setCl_cf(rs.getString("cl_cf"));
                ch.setCl_cod(rs.getString("cl_cod"));
                ch.setDel_fg(rs.getString("del_fg"));
                ch.setDel_dt(rs.getString("del_dt"));
                ch.setDel_user(rs.getString("del_user"));
                ch.setDel_motiv(rs.getString("del_motiv"));
                ch.setRefund(rs.getString("refund"));
                ch.setFa_number(rs.getString("fa_number"));
                ch.setCn_number(rs.getString("cn_number"));
                out.add(ch);
            }
            pstmt.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return out;
    }

    public boolean insertOAM(String datarif, String content, String tipo) {
        String ins = "INSERT INTO oam (datarif, content, tipo) VALUES (?, ?, ?)";
        try (PreparedStatement ps = this.c.prepareStatement(ins)) {
            ps.setString(1, datarif);
            ps.setString(2, content);
            ps.setString(3, tipo);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
        return false;
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
            log.severe(ex.getMessage());
        }
        return out;
    }

}
