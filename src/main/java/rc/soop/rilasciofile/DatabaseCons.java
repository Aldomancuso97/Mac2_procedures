/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.AtomicDouble;
import rc.soop.esolver.Branch;
import rc.soop.esolver.NC_category;
import rc.soop.esolver.Users;

import static rc.soop.esolver.ESolver.sanitizeInput;
import static rc.soop.esolver.ESolver.sanitizePath;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.formatAL;
import static rc.soop.esolver.Util.formatBankBranchReport;
import static rc.soop.esolver.Util.formatStringtoStringDate;
import static rc.soop.esolver.Util.getNC_category;
import static rc.soop.esolver.Util.get_user;
import static rc.soop.esolver.Util.parseIntR;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.patternsqldate;
import static rc.soop.esolver.Util.removeDuplicatesAL;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import static rc.soop.esolver.Util.visualizzaStringaMySQL;
import static rc.soop.maintenance.Db_Master.getControvalore;
import static rc.soop.maintenance.Db_Master.getControvaloreOFFSET;
import static rc.soop.maintenance.Db_Master.roundDouble;
import rc.soop.qlik.Qlik_ref;
import static rc.soop.rilasciofile.Ch_transaction.formatType;
import static rc.soop.rilasciofile.Utility.formatALCurrency;
import static rc.soop.rilasciofile.Utility.formatBankBranch;
import static rc.soop.rilasciofile.Utility.formatDoubleforMysql;
import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
import static rc.soop.rilasciofile.Utility.formatType_new;
import static rc.soop.rilasciofile.Utility.format_tofrom_brba_new;
import static rc.soop.rilasciofile.Utility.getDT;
import static rc.soop.rilasciofile.Utility.getNC_causal;
import static rc.soop.rilasciofile.Utility.getValueDiff_R;
import static rc.soop.rilasciofile.Utility.get_Value_history_BB;
import static rc.soop.rilasciofile.Utility.get_customerKind;
import static rc.soop.rilasciofile.Utility.get_figures;
import static rc.soop.rilasciofile.Utility.parseDoubleR;
import static rc.soop.rilasciofile.Utility.parseDoubleR_CZ;
import static rc.soop.rilasciofile.Utility.parseStringDate;
import static rc.soop.rilasciofile.Utility.patternhours_d;
import static rc.soop.rilasciofile.Utility.patternnormdate;
import static rc.soop.rilasciofile.Utility.subDays;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.leftPad;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.crm.Booking;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class DatabaseCons {

    public Connection c = null;
    public GeneraFile gf = null;

    public DatabaseCons(GeneraFile gf) {
        this.gf = gf;
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
            String pwd = rb.getString("db.pwd");
            String host;

            if (gf.isIs_UK()) {
                host = rb.getString("db.ip") + "/maccorpukprod";
            } else if (gf.isIs_CZ()) {
                host = rb.getString("db.ip") + "/maccorpczprod";
            } else {
                host = rb.getString("db.ip") + "/maccorpita";
            }

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
        } catch (Exception ex0) {
            if (this.c != null) {
                try {
                    this.c.close();
                } catch (SQLException ex1) {
                }
            }
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

    public Connection getC() {
        return c;
    }

    public void setC(Connection c) {
        this.c = c;
    }

    public GeneraFile getGf() {
        return gf;
    }

    public void setGf(GeneraFile gf) {
        this.gf = gf;
    }

    public String getPath(String cod) {
        try {
            String sql = sanitizePath("SELECT descr FROM path WHERE cod = ?");
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
        }
        return "";
    }

    public ArrayList<String> list_branchcode_completeAFTER311217() {
        ArrayList<String> li = new ArrayList<>();
        String sqlQuery;
        if (this.gf.isIs_IT()) {
            sqlQuery = sanitizeInput("SELECT distinct(cod) FROM branch WHERE cod<>'000' AND fg_annullato='0' OR (fg_annullato='1' AND STR_TO_DATE(da_annull, ?)>'2017-12-31') order by cast(cod AS decimal (10,0))");
        } else {
            sqlQuery = sanitizeInput("SELECT distinct(cod) FROM branch WHERE cod<>'000' AND fg_annullato='0' OR (fg_annullato='1' AND STR_TO_DATE(da_annull, ?)>'2018-12-31') order by cast(cod AS decimal (10,0))");
        }

        try (PreparedStatement ps = this.c.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE)) {
            // Utilizza setDate per passare il parametro della data al PreparedStatement
            if (this.gf.isIs_IT()) {
                ps.setDate(1, java.sql.Date.valueOf("2017-12-31"));
            } else {
                ps.setDate(1, java.sql.Date.valueOf("2018-12-31"));
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    li.add(rs.getString(1).replaceAll("'", "''"));
                }
                rs.close();
                ps.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public ArrayList<Ch_transaction> query_transaction_ch_new(
            String d1, String d2, ArrayList<String> branch) {
        ArrayList<Ch_transaction> out = new ArrayList<>();
        try {

            if (d1 == null || d1.equals("...")) {
                d1 = "";
            }
            d1 = d1.trim() + " 00:00:00";

            if (d2 == null || d2.equals("...")) {
                d2 = "";
            }
            d2 = d2.trim() + " 23:59:59";

            String sql = "SELECT * FROM ch_transaction ch WHERE data >= ? AND data <= ? ";

            String filwhere = "";
            for (int i = 0; i < branch.size(); i++) {
                filwhere = filwhere + "filiale = ? OR ";
            }

            if (filwhere.length() > 3) {
                sql = sql + " AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
            }

            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, d1);
            ps.setString(2, d2);
            for (int i = 0; i < branch.size(); i++) {
                ps.setString(i + 3, branch.get(i));
            }

            ResultSet rs = ps.executeQuery();

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
                ch.setBb(rs.getString("bb"));
                ch.setRefund(rs.getString("refund"));
                ch.setFa_number(rs.getString("fa_number"));
                ch.setCn_number(rs.getString("cn_number"));
                ch.setCredccard_number(rs.getString("credccard_number"));
                out.add(ch);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Figures> list_all_figures() {
        ArrayList<Figures> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM supporti where filiale = ? ORDER BY fg_sys_trans,supporto";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Figures fi = new Figures();
                fi.setBuy(rs.getString("buy"));
                fi.setBuy_back_commission(rs.getString("buy_back_commission"));
                fi.setBuy_comm_soglia_causale(rs.getString("buy_comm_soglia_causale"));
                fi.setCommissione_acquisto(rs.getString("commissione_acquisto"));
                fi.setCommissione_vendita(rs.getString("commissione_vendita"));
                fi.setCommissione_fissa(rs.getString("commissione_fissa"));
                fi.setDe_supporto(visualizzaStringaMySQL(rs.getString("de_supporto")));
                fi.setFg_annullato(rs.getString("fg_annullato"));
                fi.setFg_sys_trans(rs.getString("fg_sys_trans"));
                fi.setFg_tipo_incasso(rs.getString("fg_tipo_incasso"));
                fi.setFiliale(rs.getString("filiale"));
                fi.setFix_buy_commission(rs.getString("fix_buy_commission"));
                fi.setFix_sell_commission(rs.getString("fix_sell_commission"));
                fi.setSell_back_commission(rs.getString("sell_back_commission"));
                fi.setBuy_back_commission(rs.getString("buy_back_commission"));
                fi.setSell_comm_soglia_causale(rs.getString("sell_comm_soglia_causale"));
                fi.setSupporto(StringUtils.leftPad(rs.getString("supporto"), 2, "0"));
                fi.setResidenti(rs.getString("residenti"));
                fi.setFg_uploadobbl(rs.getString("fg_uploadobbl"));
                fi.setUpload_thr(rs.getString("upl_thr"));
                out.add(fi);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Currency> list_currency(String filiale) {
        ArrayList<Currency> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM valute WHERE filiale = ?";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Currency cu = new Currency();
                cu.setCode(rs.getString("valuta"));
                cu.setDescrizione(visualizzaStringaMySQL(rs.getString("de_valuta")));
                cu.setEnable_sellback(rs.getString("cambio_acquisto"));
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
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> city_Italy_APM() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM comuni_apm";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> country() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT nazione,de_nazione,alpha_code,fg_area_geografica FROM nazioni";
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

    public Client query_Client_transaction(String codtr, String codcl) {
        try {
            String sql = "SELECT * FROM ch_transaction_client WHERE codtr = ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
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
                ps.close();
                return bl;
            } else {
                sql = "SELECT * FROM ch_transaction_client WHERE codcl = ? ORDER BY timestamp DESC LIMIT 1";
                ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, codcl);
                rs = ps.executeQuery();
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
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return query_Client(codcl);
    }

    public Client query_Client(String cod) {
        try {
            String sql = "SELECT * FROM ch_transaction_client WHERE codcl = ? ORDER BY timestamp DESC LIMIT 1";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
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
            } else {
                sql = "SELECT * FROM anagrafica_ru where ndg = ? limit 1";
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
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new Client();
    }

    public String query_LOY_transaction(String codtr) {
        try {
            String sql = "SELECT RIGHT(loy,8) FROM loyalty_ch WHERE codtr = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, codtr);
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
                chv.setTrorig(null);
                chv.setRoundvalue(rs.getString(27));
                li.add(chv);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public ArrayList<NC_transaction> query_NC_ondemand(String anno) {
        ArrayList<NC_transaction> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,id,filiale,gruppo_nc,causale_nc,valuta,supporto,pos,"
                    + "user,till,data,total,commissione,netto,prezzo,quantita,fg_inout,ricevuta,mtcn,"
                    + "del_fg,del_dt,del_user,del_motiv,fg_tipo_transazione_nc,fg_dogana,ass_idcode,ass_startdate,ass_enddate,cl_cognome,"
                    + "cl_nome,cl_indirizzo,cl_citta,cl_nazione,cl_cap,cl_provincia,cl_email,cl_telefono,note,ti_diritti,ti_ticket_fee,id_open_till,"
                    + "posnum,percentiva,bonus,ch_transaction FROM nc_transaction WHERE data > '2019-06-01 00:00:00' AND data < '2019-10-01 00:00:00'";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_transaction nc = new NC_transaction(
                        rs.getString(1), StringUtils.leftPad(rs.getString(2), 15, "0"),
                        rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7),
                        rs.getString(8),
                        rs.getString(9), rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13),
                        rs.getString(14),
                        rs.getString(15), rs.getString(16), rs.getString(17), rs.getString(18), rs.getString(19),
                        rs.getString(20),
                        rs.getString(21), rs.getString(22), rs.getString(23), rs.getString(24), rs.getString(25),
                        rs.getString(26),
                        rs.getString(27), rs.getString(28), rs.getString(29), rs.getString(30), rs.getString(31),
                        rs.getString(32),
                        rs.getString(33), rs.getString(34), rs.getString(35), rs.getString(36), rs.getString(37),
                        rs.getString(38),
                        rs.getString(39), rs.getString(40), rs.getString(41), rs.getString(42), rs.getString(43),
                        rs.getString(44),
                        rs.getString(45), "");
                out.add(nc);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_transaction> query_NC_transaction_NEW(String d1, String d2, ArrayList<String> branch,
            String vatref) {
        ArrayList<NC_transaction> out = new ArrayList<>();
        try {
            if (d1 == null || d1.equals("...")) {
                d1 = "";
            }
            d1 = d1.trim() + " 00:00:00";

            if (d2 == null || d2.equals("...")) {
                d2 = "";
            }
            d2 = d2.trim() + " 23:59:59";

            StringBuilder sql = new StringBuilder(
                    "SELECT cod,id,filiale,gruppo_nc,causale_nc,valuta,supporto,pos,user,till,data,total,commissione,netto,prezzo,quantita,fg_inout,ricevuta,mtcn,del_fg,del_dt,del_user,del_motiv,fg_tipo_transazione_nc,fg_dogana,ass_idcode,ass_startdate,ass_enddate,cl_cognome,cl_nome,cl_indirizzo,cl_citta,cl_nazione,cl_cap,cl_provincia,cl_email,cl_telefono,note,ti_diritti,ti_ticket_fee,id_open_till,posnum,percentiva,bonus,ch_transaction FROM nc_transaction WHERE data >= ? AND data <= ?");

            if (vatref.equalsIgnoreCase("SI")) {
                sql.append(" AND fg_tipo_transazione_nc='3'");
            }

            if (!branch.isEmpty()) {
                sql.append(" AND (");
                for (int i = 0; i < branch.size(); i++) {
                    sql.append("filiale = ?");
                    if (i < branch.size() - 1) {
                        sql.append(" OR ");
                    }
                }
                sql.append(")");
            }

            try (PreparedStatement ps = this.c.prepareStatement(sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, d1);
                ps.setString(2, d2);

                for (int i = 0; i < branch.size(); i++) {
                    ps.setString(3 + i, branch.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        NC_transaction nc = new NC_transaction(
                                rs.getString(1), StringUtils.leftPad(rs.getString(2), 15, "0"),
                                rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7),
                                rs.getString(8),
                                rs.getString(9), rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13),
                                rs.getString(14),
                                rs.getString(15), rs.getString(16), rs.getString(17), rs.getString(18),
                                rs.getString(19), rs.getString(20),
                                rs.getString(21), rs.getString(22), rs.getString(23), rs.getString(24),
                                rs.getString(25), rs.getString(26),
                                rs.getString(27), rs.getString(28), rs.getString(29), rs.getString(30),
                                rs.getString(31), rs.getString(32),
                                rs.getString(33), rs.getString(34), rs.getString(35), rs.getString(36),
                                rs.getString(37), rs.getString(38),
                                rs.getString(39), rs.getString(40), rs.getString(41), rs.getString(42),
                                rs.getString(43), rs.getString(44),
                                rs.getString(45), "");
                        out.add(nc);
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_category> list_ALL_nc_category(String filiale) {
        ArrayList<NC_category> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_tipologia WHERE filiale = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_category nc1 = new NC_category();
                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setDe_gruppo_nc(visualizzaStringaMySQL(rs.getString("de_gruppo_nc")));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setConto_coge_01(rs.getString("conto_coge_01"));
                nc1.setConto_coge_02(rs.getString("conto_coge_02"));
                nc1.setDe_scontrino(visualizzaStringaMySQL(rs.getString("de_scontrino")));
                nc1.setDe_riga(visualizzaStringaMySQL(rs.getString("de_riga")));
                nc1.setPc_iva_corrispettivo(rs.getString("pc_iva_corrispettivo"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_enabled(rs.getString("ticket_enabled"));
                nc1.setTimestamp(rs.getString("data"));
                nc1.setInt_code(rs.getString("int_code"));
                nc1.setInt_corrisp(rs.getString("int_corrisp"));
                nc1.setInt_iva(rs.getString("int_iva"));
                nc1.setFg_registratore(rs.getString("fg_registratore"));
                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_causal> list_nc_causal_all(String filiale) {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_causali WHERE filiale = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_causal nc1 = new NC_causal();
                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setCausale_nc(rs.getString("causale_nc"));
                nc1.setDe_causale_nc(visualizzaStringaMySQL(rs.getString("de_causale_nc")));
                nc1.setFg_in_out(rs.getString("fg_in_out"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setNc_de(rs.getString("nc_de"));
                nc1.setFg_batch(rs.getString("fg_batch"));
                nc1.setFg_gruppo_stampa(rs.getString("fg_gruppo_stampa"));
                nc1.setFg_scontrino(rs.getString("fg_scontrino"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setData(rs.getString("data"));
                nc1.setBonus(rs.getString("bonus"));
                nc1.setCodice_integr(rs.getString("codice_integr"));
                nc1.setPaymat(rs.getString("paymat"));
                nc1.setDocric(rs.getString("docric"));
                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_nc_descr() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,descr,kind,fg_inout FROM selectncde";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Branch> list_branch() {
        ArrayList<Branch> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
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
            ex.printStackTrace();
        }
        return out;
    }

    public Ch_transaction query_transaction_ch_reportNC(String cod) {
        try {
            String sql = "SELECT pay,total,fix,com,round,commission,spread_total FROM ch_transaction WHERE cod = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                Ch_transaction ch = new Ch_transaction();
                ch.setPay(rs.getString("pay"));
                ch.setTotal(rs.getString("total"));
                ch.setFix(rs.getString("fix"));
                ch.setCom(rs.getString("com"));
                ch.setRound(rs.getString("round"));
                ch.setCommission(rs.getString("commission"));
                ch.setSpread_total(rs.getString("spread_total"));
                return ch;
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public TillTransactionListBB_value list_BBTransactionList_mod(ArrayList<String> branch, String datad1,
            String datad2, ArrayList<Branch> allb) {
        String sqlALL = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2 WHERE tr1.del_fg='0' AND tr1.cod=tr2.cod_tr"
                + " AND tr1.tipotr = 'B' AND LENGTH(tr2.bb_fidcode) = '18' ";
        try {
            String sql = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2 "
                    + " WHERE tr1.del_fg='0' AND tr1.cod=tr2.cod_tr AND tr1.tipotr"
                    + " AND (tr1.bb = '1' OR tr1.bb = '2') ";
            String filwhere = "";
            for (int i = 0; i < branch.size(); i++) {
                filwhere = filwhere + "tr1.filiale = ? OR ";
            }

            if (filwhere.length() > 3) {
                sql = sql + " AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
            }

            if (datad1 != null) {
                sql = sql + "AND tr1.data >= ? ";
            }
            if (datad2 != null) {
                sql = sql + "AND tr1.data <= ? ";
            }
            sql = sql + " ORDER BY tr1.tipotr DESC,tr1.data ASC";

            ArrayList<String> giàinseriti = new ArrayList<>();

            ArrayList<TillTransactionListBB_value> dati = new ArrayList<>();
            ArrayList<CustomerKind> list_customerKind = list_customerKind();
            ArrayList<Figures> fig = list_all_figures();
            PreparedStatement pstmt = this.c.prepareStatement(sql);
            PreparedStatement pstmtALL = this.c.prepareStatement(sqlALL);

            int paramIndex = 1;
            for (String branchCode : branch) {
                pstmt.setString(paramIndex++, branchCode);
            }
            if (datad1 != null) {
                pstmt.setString(paramIndex++, datad1 + " 00:00:00");
            }
            if (datad2 != null) {
                pstmt.setString(paramIndex++, datad2 + " 23:59:59");
            }

            ResultSet rs = pstmt.executeQuery();
            ResultSet rsALL = pstmtALL.executeQuery();

            LinkedList<String> fidcode = new LinkedList<>();
            LinkedList<Ch_transaction> codb = new LinkedList<>();

            while (rsALL.next()) {
                fidcode.add(rsALL.getString("tr2.bb_fidcode"));
                Ch_transaction ch = new Ch_transaction();
                ch.setCod(rsALL.getString("tr1.cod"));
                ch.setId(StringUtils.leftPad(rsALL.getString("tr1.id"), 15, "0"));
                ch.setFiliale(rsALL.getString("tr1.filiale"));
                ch.setTipotr(rsALL.getString("tr1.tipotr"));
                ch.setUser(rsALL.getString("tr1.user"));
                ch.setTill(rsALL.getString("tr1.till"));
                ch.setData(rsALL.getString("tr1.data"));
                ch.setTipocliente(rsALL.getString("tr1.tipocliente"));
                ch.setId_open_till(rsALL.getString("tr1.id_open_till"));
                ch.setPay(rsALL.getString("tr1.pay"));
                ch.setTotal(rsALL.getString("tr1.total"));
                ch.setFix(rsALL.getString("tr1.fix"));
                ch.setCom(rsALL.getString("tr1.com"));
                ch.setRound(rsALL.getString("tr1.round"));
                ch.setCommission(rsALL.getString("tr1.commission"));
                ch.setSpread_total(rsALL.getString("tr1.spread_total"));
                ch.setNote(rsALL.getString("tr1.note"));
                ch.setAgency(rsALL.getString("tr1.agency"));
                ch.setAgency_cod(rsALL.getString("tr1.agency_cod"));
                ch.setLocalfigures(rsALL.getString("tr1.localfigures"));
                ch.setPos(rsALL.getString("tr1.pos"));
                ch.setIntbook(rsALL.getString("tr1.intbook"));
                ch.setIntbook_type(rsALL.getString("tr1.intbook_type"));
                ch.setIntbook_1_tf(rsALL.getString("tr1.intbook_1_tf"));
                ch.setIntbook_1_mod(rsALL.getString("tr1.intbook_1_mod"));
                ch.setIntbook_1_val(rsALL.getString("tr1.intbook_1_val"));
                ch.setIntbook_2_tf(rsALL.getString("tr1.intbook_2_tf"));
                ch.setIntbook_2_mod(rsALL.getString("tr1.intbook_2_mod"));
                ch.setIntbook_2_val(rsALL.getString("tr1.intbook_2_val"));
                ch.setIntbook_3_tf(rsALL.getString("tr1.intbook_3_tf"));
                ch.setIntbook_3_mod(rsALL.getString("tr1.intbook_3_mod"));
                ch.setIntbook_3_val(rsALL.getString("tr1.intbook_3_val"));
                ch.setIntbook_mac(rsALL.getString("tr1.intbook_mac"));
                ch.setIntbook_cli(rsALL.getString("tr1.intbook_cli"));
                ch.setCl_cf(rsALL.getString("tr1.cl_cf"));
                ch.setCl_cod(rsALL.getString("tr1.cl_cod"));
                ch.setDel_fg(rsALL.getString("tr1.del_fg"));
                ch.setDel_dt(rsALL.getString("tr1.del_dt"));
                ch.setDel_user(rsALL.getString("tr1.del_user"));
                ch.setDel_motiv(rsALL.getString("tr1.del_motiv"));
                ch.setRefund(rsALL.getString("tr1.refund"));
                ch.setFa_number(rsALL.getString("tr1.fa_number"));
                ch.setCn_number(rsALL.getString("tr1.cn_number"));
                codb.add(ch);
            }
            pstmtALL.close();

            while (rs.next()) {
                boolean isbuy = rs.getString("tr1.tipotr").equals("B");
                String cod = rs.getString("tr1.cod");

                // if (isbuy) {
                if (giàinseriti.contains(cod)) {
                    continue;
                }
                // }

                giàinseriti.add(cod);
                Ch_transaction codbuy = null;
                boolean havebuy = false;
                if (fidcode.contains(rs.getString("tr1.filiale") + rs.getString("tr1.id"))) {
                    codbuy = codb.get(fidcode.indexOf(rs.getString("tr1.filiale") + rs.getString("tr1.id")));
                    havebuy = true;
                }

                // LISTA SELL
                ArrayList<Ch_transaction_value> list_value = query_transaction_value(rs.getString("tr1.cod"));
                for (int x = 0; x < list_value.size(); x++) {
                    Ch_transaction_value v1 = list_value.get(x);
                    if (v1.getBb().equals("Y") || v1.getBb().equals("F")) {
                        TillTransactionListBB_value rm = new TillTransactionListBB_value();
                        rm.setId_filiale(rs.getString("tr1.filiale"));
                        rm.setDe_filiale(formatBankBranchReport(rs.getString("tr1.filiale"), "BR", null, allb));
                        if (havebuy) {
                            rm.setType("OK");
                        } else {
                            rm.setType("  ");
                        }
                        rm.setTill(rs.getString("tr1.till"));
                        rm.setUser(rs.getString("tr1.user"));
                        rm.setNotr(rs.getString("tr1.id"));
                        rm.setTime(rs.getString("tr1.data"));
                        rm.setCur(v1.getValuta());
                        rm.setKind(formatType(rs.getString("tr1.tipotr")) + " "
                                + get_figures(fig, v1.getSupporto()).getDe_supporto());
                        rm.setAmount(v1.getQuantita());
                        rm.setRate(v1.getRate());
                        rm.setTotal(v1.getTotal());
                        rm.setPerc(v1.getCom_perc());
                        rm.setComfree(v1.getFx_com());
                        rm.setPayinpayout(v1.getNet());
                        rm.setCustomer(query_Client_transactionCN(rs.getString("tr1.cod"), rs.getString("tr1.cl_cod")));
                        rm.setSpread(v1.getSpread());
                        rm.setRound(rs.getString("tr2.roundvalue").replaceAll("-", ""));
                        if (rs.getString("tr2.supporto").equals("04")) {
                            rm.setPos(rs.getString("tr2.pos"));
                        } else {
                            rm.setPos(rs.getString("tr1.pos"));
                        }
                        rm.setInternetbooking(rs.getString("tr1.intbook"));
                        if (get_customerKind(list_customerKind, rs.getString("tr1.tipocliente")).getFg_nazionalita()
                                .equals("1")) {
                            rm.setResidentnonresident("Resident");
                        } else {
                            rm.setResidentnonresident("Non Resident");
                        }
                        String lfig = rs.getString("tr1.localfigures");
                        if (rs.getString("tr1.localfigures").equals("-")) {
                            lfig = "01";
                        }
                        rm.setFig(lfig);
                        dati.add(rm);
                    }
                }

                if (havebuy && codbuy != null) {
                    ArrayList<Ch_transaction_value> buy_value = query_transaction_value(codbuy.getCod());
                    for (int y = 0; y < buy_value.size(); y++) {
                        giàinseriti.add(codbuy.getCod());
                        TillTransactionListBB_value rm = new TillTransactionListBB_value();
                        rm.setId_filiale(codbuy.getFiliale());
                        rm.setDe_filiale(formatBankBranchReport(codbuy.getFiliale(), "BR", null, allb));
                        rm.setType(" ");
                        rm.setTill(codbuy.getTill());
                        rm.setUser(codbuy.getUser());
                        rm.setNotr(codbuy.getId());
                        rm.setTime(codbuy.getData());
                        rm.setCur(buy_value.get(y).getValuta());
                        rm.setKind(formatType(codbuy.getTipotr()) + " "
                                + get_figures(fig, buy_value.get(y).getSupporto()).getDe_supporto());
                        rm.setAmount(buy_value.get(y).getQuantita());
                        rm.setRate(buy_value.get(y).getRate());
                        rm.setTotal(buy_value.get(y).getTotal());
                        rm.setPerc(buy_value.get(y).getCom_perc());

                        rm.setComfree(buy_value.get(y).getTot_com());
                        rm.setPayinpayout(buy_value.get(y).getNet());
                        rm.setCustomer(" ");
                        rm.setSpread(buy_value.get(y).getSpread());
                        rm.setRound(buy_value.get(y).getRoundvalue().replaceAll("-", ""));

                        if (buy_value.get(y).getSupporto().equals("04")) {
                            rm.setPos(buy_value.get(y).getPos());
                        } else {
                            rm.setPos(codbuy.getPos());
                        }
                        rm.setInternetbooking(codbuy.getIntbook());
                        if (get_customerKind(list_customerKind, codbuy.getTipocliente()).getFg_nazionalita()
                                .equals("1")) {
                            rm.setResidentnonresident("Resident");
                        } else {
                            rm.setResidentnonresident("Non Resident");
                        }
                        String lfig = rs.getString("tr1.localfigures");
                        if (rs.getString("tr1.localfigures").equals("-")) {
                            lfig = "01";
                        }
                        rm.setFig(lfig);
                        dati.add(rm);
                    }

                }

                TillTransactionListBB_value rm = new TillTransactionListBB_value();
                rm.setId_filiale("");
                rm.setDe_filiale("");
                rm.setType("");
                rm.setTill("");
                rm.setUser("");
                rm.setNotr("");
                rm.setTime("");
                rm.setCur("");
                rm.setKind("");
                rm.setAmount("");
                rm.setRate("");
                rm.setTotal("");
                rm.setPerc("");
                rm.setComfree("");
                rm.setPayinpayout("");
                rm.setCustomer("");
                rm.setSpread("");
                rm.setRound("");
                rm.setPos("");
                rm.setInternetbooking("");
                rm.setResidentnonresident("");
                rm.setFig("");
                dati.add(rm);

            }
            pstmt.close();

            if (dati.size() > 0) {
                TillTransactionListBB_value pdf = new TillTransactionListBB_value();
                pdf.setId_filiale(branch.get(0));
                pdf.setDe_filiale(formatBankBranchReport(branch.get(0), "BR", null, allb));
                pdf.setDati(dati);

                double setTransvalueresidentbuy = 0.0;
                double setTransvaluenonresidentbuy = 0.0;
                double setTransvalueresidentsell = 0.0;
                double setTransvaluenonresidentsell = 0.0;
                double setCommisionvaluetresidentbuy = 0.0;
                double setCommisionvaluenonresidentbuy = 0.0;
                double setCommisionvaluetresidentsell = 0.0;
                double setCommisionvaluenonresidentsell = 0.0;

                int setTransactionnumberresidentbuy = 0;
                int setTransactionnumbernonresidentbuy = 0;
                int setTransactionnumberresidentsell = 0;
                int setTransactionnumbernonresidentsell = 0;

                double setInternetbookingamountyes = 0.0;
                int setInternetbookingnumberyes = 0;
                double setInternetbookingamountno = 0.0;
                int setInternetbookingnumberno = 0;

                double setPosbuyamount = 0.0;
                int setPosbuynumber = 0;
                double setPossellamount = 0.0;
                int setPossellnumber = 0;
                double setBanksellamount = 0.0;
                int setBanksellnumber = 0;

                for (int i = 0; i < dati.size(); i++) {

                    TillTransactionListBB_value rm = dati.get(i);
                    if (rm.getResidentnonresident().equals("Resident")
                            && rm.getKind().toUpperCase().startsWith("BUY")) {
                        setTransvalueresidentbuy = setTransvalueresidentbuy + fd(rm.getTotal());
                        setCommisionvaluetresidentbuy = setCommisionvaluetresidentbuy + fd(rm.getComfree());
                        setTransactionnumberresidentbuy++;
                    }
                    if (rm.getResidentnonresident().equals("Non Resident")
                            && rm.getKind().toUpperCase().startsWith("BUY")) {
                        setTransvaluenonresidentbuy = setTransvaluenonresidentbuy + fd(rm.getTotal());
                        setCommisionvaluenonresidentbuy = setCommisionvaluenonresidentbuy + fd(rm.getComfree());
                        setTransactionnumbernonresidentbuy++;
                    }

                    if (rm.getResidentnonresident().equals("Resident")
                            && rm.getKind().toUpperCase().startsWith("SELL")) {
                        setTransvalueresidentsell = setTransvalueresidentsell + fd(rm.getAmount());
                        setCommisionvaluetresidentsell = setCommisionvaluetresidentsell + fd(rm.getComfree());
                        setTransactionnumberresidentsell++;
                    }
                    if (rm.getResidentnonresident().equals("Non Resident")
                            && rm.getKind().toUpperCase().startsWith("SELL")) {
                        setTransvaluenonresidentsell = setTransvaluenonresidentsell + fd(rm.getAmount());
                        setCommisionvaluenonresidentsell = setCommisionvaluenonresidentsell + fd(rm.getComfree());
                        setTransactionnumbernonresidentsell++;
                    }

                    boolean ca = false;
                    if (rm.getKind().toUpperCase().startsWith("SELL") && !rm.getType().contains("D")) {
                        if (!rm.getInternetbooking().equals("0")) {
                            setInternetbookingamountyes = setInternetbookingamountyes + fd(rm.getPayinpayout());
                            setInternetbookingnumberyes++;
                        } else {
                            setInternetbookingamountno = setInternetbookingamountno + fd(rm.getPayinpayout());
                            setInternetbookingnumberno++;
                        }
                    } else if (rm.getKind().toUpperCase().contains("CASH ADVANCE") && !rm.getType().contains("D")) {
                        setPosbuyamount = setPosbuyamount + fd(rm.getAmount());
                        setPosbuynumber++;
                        ca = true;
                    }

                    if (!rm.getPos().equals("-") && !rm.getType().contains("D")) {
                        if (rm.getKind().toUpperCase().startsWith("SELL")) {
                            if (rm.getFig().equals("06") || rm.getFig().equals("07")) {
                                setPossellamount = setPossellamount + fd(rm.getPayinpayout());
                                setPossellnumber++;
                            } else {
                                setBanksellamount = setBanksellamount + fd(rm.getPayinpayout());
                                setBanksellnumber++;
                            }
                        } else {
                            if (!ca) {
                                setPosbuyamount = setPosbuyamount + fd(rm.getTotal());
                                setPosbuynumber++;
                            }
                        }
                    }

                }
                pdf.setTransvalueresidentbuy(roundDoubleandFormat(setTransvalueresidentbuy, 2));
                pdf.setTransvaluenonresidentbuy(roundDoubleandFormat(setTransvaluenonresidentbuy, 2));
                pdf.setTransvalueresidentsell(roundDoubleandFormat(setTransvalueresidentsell, 2));
                pdf.setTransvaluenonresidentsell(roundDoubleandFormat(setTransvaluenonresidentsell, 2));
                pdf.setCommisionvaluetresidentbuy(roundDoubleandFormat(setCommisionvaluetresidentbuy, 2));
                pdf.setCommisionvaluenonresidentbuy(roundDoubleandFormat(setCommisionvaluenonresidentbuy, 2));
                pdf.setCommisionvaluetresidentsell(roundDoubleandFormat(setCommisionvaluetresidentsell, 2));
                pdf.setCommisionvaluenonresidentsell(roundDoubleandFormat(setCommisionvaluenonresidentsell, 2));
                pdf.setTransactionnumberresidentbuy(roundDoubleandFormat(setTransactionnumberresidentbuy, 0));
                pdf.setTransactionnumbernonresidentbuy(
                        roundDoubleandFormat(setTransactionnumbernonresidentbuy, 0) + "");
                pdf.setTransactionnumberresidentsell(roundDoubleandFormat(setTransactionnumberresidentsell, 0) + "");
                pdf.setTransactionnumbernonresidentsell(
                        roundDoubleandFormat(setTransactionnumbernonresidentsell, 0) + "");
                pdf.setInternetbookingamountyes(roundDoubleandFormat(setInternetbookingamountyes, 2));
                pdf.setInternetbookingnumberyes(roundDoubleandFormat(setInternetbookingnumberyes, 0));
                pdf.setInternetbookingamountno(roundDoubleandFormat(setInternetbookingamountno, 2) + "");
                pdf.setInternetbookingnumberno(roundDoubleandFormat(setInternetbookingnumberno, 0) + "");
                pdf.setPosbuyamount(roundDoubleandFormat(setPosbuyamount, 2));
                pdf.setPosbuynumber(roundDoubleandFormat(setPosbuynumber, 0) + "");
                pdf.setPossellamount(roundDoubleandFormat(setPossellamount, 2));
                pdf.setPossellnumber(roundDoubleandFormat(setPossellnumber, 0) + "");
                pdf.setBankbuyamount("0.00");
                pdf.setBankbuynumber("0");
                pdf.setBanksellamount(roundDoubleandFormat(setBanksellamount, 2));
                pdf.setBanksellnumber(roundDoubleandFormat(setBanksellnumber, 0) + "");

                return pdf;
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;

    }

    public ArrayList<CustomerKind> list_customerKind() {
        ArrayList<CustomerKind> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM tipologiaclienti";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                CustomerKind ck = new CustomerKind();
                ck.setDe_tipologia_clienti(visualizzaStringaMySQL(rs.getString("de_tipologia_clienti")));
                ck.setTipologia_clienti(StringUtils.leftPad(rs.getString("tipologia_clienti"), 3, "0"));
                ck.setFg_nazionalita(rs.getString("fg_nazionalita"));
                ck.setFg_tipo_cliente(rs.getString("fg_tipo_cliente"));
                ck.setIp_max_singola_transazione(rs.getString("ip_max_singola_transazione"));
                ck.setIp_max_settimanale(rs.getString("ip_max_settimanale"));
                ck.setFg_area_geografica(rs.getString("fg_area_geografica"));
                ck.setStampa_autocertificazione(rs.getString("stampa_autocertificazione"));
                ck.setIp_soglia_antiriciclaggio(rs.getString("ip_soglia_antiriciclaggio"));
                ck.setIp_soglia_extraCEE_certification(rs.getString("ip_soglia_extraCEE_certification"));
                ck.setFg_annullato(rs.getString("fg_annullato"));
                ck.setFg_uploadobbl(rs.getString("fg_uploadobbl"));
                ck.setTipofat(rs.getString("tipofat"));
                ck.setVatcode(rs.getString("vatcode"));
                ck.setIp_soglia_bollo(rs.getString("ip_soglia_bollo"));
                ck.setIp_value_bollo(rs.getString("ip_value_bollo"));
                ck.setDescr_bollo(visualizzaStringaMySQL(rs.getString("descr_bollo")));
                ck.setResident(rs.getString("resid"));
                ck.setTaxfree(rs.getString("taxfree"));
                out.add(ck);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String query_Client_transactionCN(String codtr, String codcl) {
        try {
            String sql = "SELECT cognome, nome FROM ch_transaction_client WHERE codtr = ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, codtr);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("cognome") + " " + rs.getString("nome");
                    }
                    ps.close();
                }
            }

            // Se non ci sono risultati per il codtr, cercare per codcl
            sql = "SELECT cognome, nome FROM ch_transaction_client WHERE codcl = ? ORDER BY timestamp DESC LIMIT 1";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, codcl);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("cognome") + " " + rs.getString("nome");
                    }
                    ps.close();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        // Se non ci sono risultati ne per codtr ne per codcl, restituire un valore di
        // default
        return query_ClientCN(codcl);
    }

    public String query_ClientCN(String cod) {
        try {
            String sql = "SELECT cognome, nome FROM ch_transaction_client WHERE codcl = ? ORDER BY timestamp DESC LIMIT 1";
            try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps.setString(1, cod);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("cognome") + " " + rs.getString("nome");
                    }
                    ps.close();
                }
            }

            // Se non ci sono risultati per codcl, cercare nell'anagrafica_ru
            sql = "SELECT cognome, nome FROM anagrafica_ru WHERE ndg = ? LIMIT 1";
            try (PreparedStatement ps1 = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {
                ps1.setString(1, cod);
                try (ResultSet rs1 = ps1.executeQuery()) {
                    if (rs1.next()) {
                        return rs1.getString("cognome") + " " + rs1.getString("nome");
                    }
                    ps1.close();

                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return "";
    }

    public TillTransactionListBB_value list_SBTransactionList(ArrayList<String> branch, String datad1, String datad2,
            ArrayList<Branch> allb) {
        String sqlSell_incassati = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2 WHERE tr1.del_fg='0' AND tr1.cod=tr2.cod_tr"
                + " AND tr1.tipotr = 'S' AND LENGTH(tr2.bb_fidcode) = '18' ";

        try {
            String sql = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2 "
                    + " WHERE tr1.del_fg='0' AND tr1.cod=tr2.cod_tr AND tr1.tipotr"
                    + " AND (tr1.bb = '3' OR tr1.bb = '4') ";

            String filwhere = "";
            for (int i = 0; i < branch.size(); i++) {
                filwhere = filwhere + "tr1.filiale = ? OR ";
            }

            if (filwhere.length() > 3) {
                sql = sql + " AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
            }

            if (datad1 != null) {
                sql = sql + "AND tr1.data >= ? ";
            }
            if (datad2 != null) {
                sql = sql + "AND tr1.data <= ? ";
            }
            sql = sql + " ORDER BY tr1.tipotr ASC,tr1.data ASC";

            ArrayList<String> giàinseriti = new ArrayList<>();

            ArrayList<TillTransactionListBB_value> dati = new ArrayList<>();
            ArrayList<CustomerKind> list_customerKind = list_customerKind();
            ArrayList<Figures> fig = list_all_figures();

            PreparedStatement pstmt = this.c.prepareStatement(sql);
            PreparedStatement pstmtALL = this.c.prepareStatement(sqlSell_incassati);

            int paramIndex = 1;
            for (String branchCode : branch) {
                pstmt.setString(paramIndex++, branchCode);
            }
            if (datad1 != null) {
                pstmt.setString(paramIndex++, datad1 + " 00:00:00");
            }
            if (datad2 != null) {
                pstmt.setString(paramIndex++, datad2 + " 23:59:59");
            }

            ResultSet rs = pstmt.executeQuery();
            ResultSet rsALL = pstmtALL.executeQuery();

            LinkedList<String> fidcode = new LinkedList<>();
            LinkedList<Ch_transaction> codSe = new LinkedList<>();
            while (rsALL.next()) {
                fidcode.add(rsALL.getString("tr2.bb_fidcode"));
                Ch_transaction ch = new Ch_transaction();
                ch.setCod(rsALL.getString("tr1.cod"));
                ch.setId(StringUtils.leftPad(rsALL.getString("tr1.id"), 15, "0"));
                ch.setFiliale(rsALL.getString("tr1.filiale"));
                ch.setTipotr(rsALL.getString("tr1.tipotr"));
                ch.setUser(rsALL.getString("tr1.user"));
                ch.setTill(rsALL.getString("tr1.till"));
                ch.setData(rsALL.getString("tr1.data"));
                ch.setTipocliente(rsALL.getString("tr1.tipocliente"));
                ch.setId_open_till(rsALL.getString("tr1.id_open_till"));
                ch.setPay(rsALL.getString("tr1.pay"));
                ch.setTotal(rsALL.getString("tr1.total"));
                ch.setFix(rsALL.getString("tr1.fix"));
                ch.setCom(rsALL.getString("tr1.com"));
                ch.setRound(rsALL.getString("tr1.round"));
                ch.setCommission(rsALL.getString("tr1.commission"));
                ch.setSpread_total(rsALL.getString("tr1.spread_total"));
                ch.setNote(rsALL.getString("tr1.note"));
                ch.setAgency(rsALL.getString("tr1.agency"));
                ch.setAgency_cod(rsALL.getString("tr1.agency_cod"));
                ch.setLocalfigures(rsALL.getString("tr1.localfigures"));
                ch.setPos(rsALL.getString("tr1.pos"));
                ch.setIntbook(rsALL.getString("tr1.intbook"));
                ch.setIntbook_type(rsALL.getString("tr1.intbook_type"));
                ch.setIntbook_1_tf(rsALL.getString("tr1.intbook_1_tf"));
                ch.setIntbook_1_mod(rsALL.getString("tr1.intbook_1_mod"));
                ch.setIntbook_1_val(rsALL.getString("tr1.intbook_1_val"));
                ch.setIntbook_2_tf(rsALL.getString("tr1.intbook_2_tf"));
                ch.setIntbook_2_mod(rsALL.getString("tr1.intbook_2_mod"));
                ch.setIntbook_2_val(rsALL.getString("tr1.intbook_2_val"));
                ch.setIntbook_3_tf(rsALL.getString("tr1.intbook_3_tf"));
                ch.setIntbook_3_mod(rsALL.getString("tr1.intbook_3_mod"));
                ch.setIntbook_3_val(rsALL.getString("tr1.intbook_3_val"));
                ch.setIntbook_mac(rsALL.getString("tr1.intbook_mac"));
                ch.setIntbook_cli(rsALL.getString("tr1.intbook_cli"));
                ch.setCl_cf(rsALL.getString("tr1.cl_cf"));
                ch.setCl_cod(rsALL.getString("tr1.cl_cod"));
                ch.setDel_fg(rsALL.getString("tr1.del_fg"));
                ch.setDel_dt(rsALL.getString("tr1.del_dt"));
                ch.setDel_user(rsALL.getString("tr1.del_user"));
                ch.setDel_motiv(rsALL.getString("tr1.del_motiv"));
                ch.setRefund(rsALL.getString("tr1.refund"));
                ch.setFa_number(rsALL.getString("tr1.fa_number"));
                ch.setCn_number(rsALL.getString("tr1.cn_number"));
                codSe.add(ch);
            }
            pstmtALL.close();

            while (rs.next()) {
                String cod = rs.getString("tr1.cod");
                if (giàinseriti.contains(cod)) {
                    continue;
                }
                giàinseriti.add(cod);
                Ch_transaction codsell = null;
                boolean havesell = false;
                if (fidcode.contains(rs.getString("tr1.filiale") + rs.getString("tr1.id"))) {
                    codsell = codSe.get(fidcode.indexOf(rs.getString("tr1.filiale") + rs.getString("tr1.id")));
                    havesell = true;
                }
                // LISTA SELL
                ArrayList<Ch_transaction_value> list_value = query_transaction_value(rs.getString("tr1.cod"));
                for (int x = 0; x < list_value.size(); x++) {
                    Ch_transaction_value v1 = list_value.get(x);
                    if (v1.getBb().equals("Y") || v1.getBb().equals("F")) {
                        TillTransactionListBB_value rm = new TillTransactionListBB_value();
                        rm.setId_filiale(rs.getString("tr1.filiale"));
                        rm.setDe_filiale(formatBankBranchReport(rs.getString("tr1.filiale"), "BR", null, allb));
                        if (havesell) {
                            rm.setType("OK");
                        } else {
                            rm.setType("  ");
                        }
                        rm.setTill(rs.getString("tr1.till"));
                        rm.setUser(rs.getString("tr1.user"));
                        rm.setNotr(rs.getString("tr1.id"));
                        rm.setTime(rs.getString("tr1.data"));
                        rm.setCur(v1.getValuta());
                        rm.setKind(formatType(rs.getString("tr1.tipotr")) + " "
                                + get_figures(fig, v1.getSupporto()).getDe_supporto());
                        rm.setAmount(v1.getQuantita());
                        rm.setRate(v1.getRate());
                        rm.setTotal(v1.getTotal());
                        rm.setPerc(v1.getCom_perc());
                        rm.setComfree(v1.getFx_com());
                        rm.setPayinpayout(v1.getNet());
                        rm.setCustomer(query_Client_transactionCN(rs.getString("tr1.cod"), rs.getString("tr1.cl_cod")));
                        rm.setSpread(v1.getSpread());
                        rm.setRound(rs.getString("tr2.roundvalue").replaceAll("-", ""));
                        if (rs.getString("tr2.supporto").equals("04")) {
                            rm.setPos(rs.getString("tr2.pos"));
                        } else {
                            rm.setPos(rs.getString("tr1.pos"));
                        }
                        rm.setInternetbooking(rs.getString("tr1.intbook"));
                        if (get_customerKind(list_customerKind, rs.getString("tr1.tipocliente")).getFg_nazionalita()
                                .equals("1")) {
                            rm.setResidentnonresident("Resident");
                        } else {
                            rm.setResidentnonresident("Non Resident");
                        }
                        String lfig = rs.getString("tr1.localfigures");
                        if (rs.getString("tr1.localfigures").equals("-")) {
                            lfig = "01";
                        }
                        rm.setFig(lfig);
                        dati.add(rm);
                    }
                }

                if (havesell) {

                    if (codsell != null) {
                        ArrayList<Ch_transaction_value> sell_value = query_transaction_value(codsell.getCod());
                        for (int y = 0; y < sell_value.size(); y++) {
                            giàinseriti.add(codsell.getCod());
                            TillTransactionListBB_value rm = new TillTransactionListBB_value();
                            rm.setId_filiale(codsell.getFiliale());
                            rm.setDe_filiale(formatBankBranchReport(codsell.getFiliale(), "BR", null, allb));
                            rm.setType(" ");
                            rm.setTill(codsell.getTill());
                            rm.setUser(codsell.getUser());
                            rm.setNotr(codsell.getId());
                            rm.setTime(codsell.getData());
                            rm.setCur(sell_value.get(y).getValuta());
                            rm.setKind(formatType(codsell.getTipotr()) + " "
                                    + get_figures(fig, sell_value.get(y).getSupporto()).getDe_supporto());
                            rm.setAmount(sell_value.get(y).getQuantita());
                            rm.setRate(sell_value.get(y).getRate());
                            rm.setTotal(sell_value.get(y).getTotal());
                            rm.setPerc(sell_value.get(y).getCom_perc());
                            rm.setComfree(sell_value.get(y).getTot_com());
                            rm.setPayinpayout(sell_value.get(y).getNet());
                            rm.setCustomer(" ");
                            rm.setSpread(sell_value.get(y).getSpread());
                            rm.setRound(sell_value.get(y).getRoundvalue().replaceAll("-", ""));
                            if (sell_value.get(y).getSupporto().equals("04")) {
                                rm.setPos(sell_value.get(y).getPos());
                            } else {
                                rm.setPos(codsell.getPos());
                            }
                            rm.setInternetbooking(codsell.getIntbook());
                            if (get_customerKind(list_customerKind, codsell.getTipocliente()).getFg_nazionalita()
                                    .equals("1")) {
                                rm.setResidentnonresident("Resident");
                            } else {
                                rm.setResidentnonresident("Non Resident");
                            }
                            String lfig = rs.getString("tr1.localfigures");
                            if (rs.getString("tr1.localfigures").equals("-")) {
                                lfig = "01";
                            }
                            rm.setFig(lfig);
                            dati.add(rm);
                        }
                    }
                }

                TillTransactionListBB_value rm = new TillTransactionListBB_value();
                rm.setId_filiale("");
                rm.setDe_filiale("");
                rm.setType("");
                rm.setTill("");
                rm.setUser("");
                rm.setNotr("");
                rm.setTime("");
                rm.setCur("");
                rm.setKind("");
                rm.setAmount("");
                rm.setRate("");
                rm.setTotal("");
                rm.setPerc("");
                rm.setComfree("");
                rm.setPayinpayout("");
                rm.setCustomer("");
                rm.setSpread("");
                rm.setRound("");
                rm.setPos("");
                rm.setInternetbooking("");
                rm.setResidentnonresident("");
                rm.setFig("");
                dati.add(rm);

            }
            pstmt.close();

            if (dati.size() > 0) {
                TillTransactionListBB_value pdf = new TillTransactionListBB_value();
                pdf.setId_filiale(branch.get(0));
                pdf.setDe_filiale(formatBankBranchReport(branch.get(0), "BR", null, allb));
                pdf.setDati(dati);
                double setTransvalueresidentbuy = 0.0;
                double setTransvaluenonresidentbuy = 0.0;
                double setTransvalueresidentsell = 0.0;
                double setTransvaluenonresidentsell = 0.0;
                double setCommisionvaluetresidentbuy = 0.0;
                double setCommisionvaluenonresidentbuy = 0.0;
                double setCommisionvaluetresidentsell = 0.0;
                double setCommisionvaluenonresidentsell = 0.0;
                int setTransactionnumberresidentbuy = 0;
                int setTransactionnumbernonresidentbuy = 0;
                int setTransactionnumberresidentsell = 0;
                int setTransactionnumbernonresidentsell = 0;
                double setInternetbookingamountyes = 0.0;
                int setInternetbookingnumberyes = 0;
                double setInternetbookingamountno = 0.0;
                int setInternetbookingnumberno = 0;
                double setPosbuyamount = 0.0;
                int setPosbuynumber = 0;
                double setPossellamount = 0.0;
                int setPossellnumber = 0;
                double setBanksellamount = 0.0;
                int setBanksellnumber = 0;
                for (int i = 0; i < dati.size(); i++) {
                    TillTransactionListBB_value rm = dati.get(i);
                    if (rm.getResidentnonresident().equals("Resident")
                            && rm.getKind().toUpperCase().startsWith("BUY")) {
                        setTransvalueresidentbuy = setTransvalueresidentbuy + fd(rm.getTotal());
                        setCommisionvaluetresidentbuy = setCommisionvaluetresidentbuy + fd(rm.getComfree());
                        setTransactionnumberresidentbuy++;
                    }
                    if (rm.getResidentnonresident().equals("Non Resident")
                            && rm.getKind().toUpperCase().startsWith("BUY")) {
                        setTransvaluenonresidentbuy = setTransvaluenonresidentbuy + fd(rm.getTotal());
                        setCommisionvaluenonresidentbuy = setCommisionvaluenonresidentbuy + fd(rm.getComfree());
                        setTransactionnumbernonresidentbuy++;
                    }

                    if (rm.getResidentnonresident().equals("Resident")
                            && rm.getKind().toUpperCase().startsWith("SELL")) {
                        setTransvalueresidentsell = setTransvalueresidentsell + fd(rm.getAmount());
                        setCommisionvaluetresidentsell = setCommisionvaluetresidentsell + fd(rm.getComfree());
                        setTransactionnumberresidentsell++;
                    }
                    if (rm.getResidentnonresident().equals("Non Resident")
                            && rm.getKind().toUpperCase().startsWith("SELL")) {
                        setTransvaluenonresidentsell = setTransvaluenonresidentsell + fd(rm.getAmount());
                        setCommisionvaluenonresidentsell = setCommisionvaluenonresidentsell + fd(rm.getComfree());
                        setTransactionnumbernonresidentsell++;
                    }
                    boolean ca = false;
                    if (rm.getKind().toUpperCase().startsWith("SELL") && !rm.getType().contains("D")) {
                        if (!rm.getInternetbooking().equals("0")) {
                            setInternetbookingamountyes = setInternetbookingamountyes + fd(rm.getPayinpayout());
                            setInternetbookingnumberyes++;
                        } else {
                            setInternetbookingamountno = setInternetbookingamountno + fd(rm.getPayinpayout());
                            setInternetbookingnumberno++;
                        }
                    } else if (rm.getKind().toUpperCase().contains("CASH ADVANCE") && !rm.getType().contains("D")) {
                        setPosbuyamount = setPosbuyamount + fd(rm.getAmount());
                        setPosbuynumber++;
                        ca = true;
                    }
                    if (!rm.getPos().equals("-") && !rm.getType().contains("D")) {
                        if (rm.getKind().toUpperCase().startsWith("SELL")) {
                            if (rm.getFig().equals("06") || rm.getFig().equals("07")) {
                                setPossellamount = setPossellamount + fd(rm.getPayinpayout());
                                setPossellnumber++;
                            } else {
                                setBanksellamount = setBanksellamount + fd(rm.getPayinpayout());
                                setBanksellnumber++;
                            }
                        } else {
                            if (!ca) {
                                setPosbuyamount = setPosbuyamount + fd(rm.getTotal());
                                setPosbuynumber++;
                            }
                        }
                    }
                }
                pdf.setTransvalueresidentbuy(roundDoubleandFormat(setTransvalueresidentbuy, 2));
                pdf.setTransvaluenonresidentbuy(roundDoubleandFormat(setTransvaluenonresidentbuy, 2));
                pdf.setTransvalueresidentsell(roundDoubleandFormat(setTransvalueresidentsell, 2));
                pdf.setTransvaluenonresidentsell(roundDoubleandFormat(setTransvaluenonresidentsell, 2));
                pdf.setCommisionvaluetresidentbuy(roundDoubleandFormat(setCommisionvaluetresidentbuy, 2));
                pdf.setCommisionvaluenonresidentbuy(roundDoubleandFormat(setCommisionvaluenonresidentbuy, 2));
                pdf.setCommisionvaluetresidentsell(roundDoubleandFormat(setCommisionvaluetresidentsell, 2));
                pdf.setCommisionvaluenonresidentsell(roundDoubleandFormat(setCommisionvaluenonresidentsell, 2));
                pdf.setTransactionnumberresidentbuy(roundDoubleandFormat(setTransactionnumberresidentbuy, 0));
                pdf.setTransactionnumbernonresidentbuy(
                        roundDoubleandFormat(setTransactionnumbernonresidentbuy, 0) + "");
                pdf.setTransactionnumberresidentsell(roundDoubleandFormat(setTransactionnumberresidentsell, 0) + "");
                pdf.setTransactionnumbernonresidentsell(
                        roundDoubleandFormat(setTransactionnumbernonresidentsell, 0) + "");
                pdf.setInternetbookingamountyes(roundDoubleandFormat(setInternetbookingamountyes, 2));
                pdf.setInternetbookingnumberyes(roundDoubleandFormat(setInternetbookingnumberyes, 0));
                pdf.setInternetbookingamountno(roundDoubleandFormat(setInternetbookingamountno, 2) + "");
                pdf.setInternetbookingnumberno(roundDoubleandFormat(setInternetbookingnumberno, 0) + "");
                pdf.setPosbuyamount(roundDoubleandFormat(setPosbuyamount, 2));
                pdf.setPosbuynumber(roundDoubleandFormat(setPosbuynumber, 0) + "");
                pdf.setPossellamount(roundDoubleandFormat(setPossellamount, 2));
                pdf.setPossellnumber(roundDoubleandFormat(setPossellnumber, 0) + "");
                pdf.setBankbuyamount("0.00");
                pdf.setBankbuynumber("0");
                pdf.setBanksellamount(roundDoubleandFormat(setBanksellamount, 2));
                pdf.setBanksellnumber(roundDoubleandFormat(setBanksellnumber, 0) + "");
                return pdf;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<String[]> list_fasce_cashier_perf(String tipo, String status) {
        ArrayList<String[]> li = new ArrayList<>();
        try {
            if (tipo == null) {
                tipo = "BS";
            }
            String sql = "SELECT * FROM cash_perm WHERE id <> '' ";
            if (!tipo.equals("BS")) {
                sql = sql + "AND tipo='" + tipo + "'";
            }
            if (status != null) {
                sql = sql + "AND fg_stato='" + status + "'";
            }
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ou = { rs.getString("id"),
                        formatMysqltoDisplay(rs.getString("da")),
                        formatMysqltoDisplay(rs.getString("a")),
                        rs.getString("fg_stato"), rs.getString("tipo") };
                li.add(ou);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return li;
    }

    public ArrayList<C_CashierPerformance_value> list_C_CashierPerformance_value(String data1, String data2,
            String bss, ArrayList<String> branch,
            ArrayList<String[]> fasce) {
        ArrayList<C_CashierPerformance_value> out = new ArrayList<>();
        try {
            String oper = "/";
            String sql = "SELECT * FROM ch_transaction WHERE del_fg <> '2' ";

            StringBuilder filwhereBuilder = new StringBuilder();

            for (String branchCode : branch) {
                filwhereBuilder.append("filiale=? OR ");
            }

            if (filwhereBuilder.length() > 3) {
                String filwhere = filwhereBuilder.substring(0, filwhereBuilder.length() - 3);
                sql += " AND (" + filwhere + ")";
            }

            if (data1 != null) {
                sql += " AND data >= ? ";
            }

            if (data2 != null) {
                sql += " AND data <= ? ";
            }

            if (!bss.equals("BS")) {
                sql += " AND tipotr = ? ";
            }

            sql += " ORDER BY user,data";

            PreparedStatement ps = this.c.prepareStatement(sql);
            int parameterIndex = 1;

            for (String branchCode : branch) {
                ps.setString(parameterIndex++, branchCode);
            }

            if (data1 != null) {
                ps.setString(parameterIndex++, data1 + " 00:00:00");
            }

            if (data2 != null) {
                ps.setString(parameterIndex++, data2 + " 23:59:59");
            }

            if (!bss.equals("BS")) {
                ps.setString(parameterIndex, bss);
            }

            ResultSet rs = ps.executeQuery();
            List<String> usl = new ArrayList<>();
            while (rs.next()) {
                usl.add(rs.getString("user"));
            }
            ps.close();

            String sql1 = "SELECT * FROM oc_lista WHERE errors ='Y' ";

            if (filwhereBuilder.length() > 3) {
                String filwhere = filwhereBuilder.substring(0, filwhereBuilder.length() - 3);
                sql1 += " AND (" + filwhere + ")";
            }

            if (data1 != null) {
                sql1 += " AND data >= ? ";
            }

            if (data2 != null) {
                sql1 += " AND data <= ? ";
            }

            sql1 += " ORDER BY data";

            PreparedStatement ps1 = this.c.prepareStatement(sql1);
            parameterIndex = 1;

            for (String branchCode : branch) {
                ps1.setString(parameterIndex++, branchCode);
            }

            if (data1 != null) {
                ps1.setString(parameterIndex++, data1 + " 00:00:00");
            }

            if (data2 != null) {
                ps1.setString(parameterIndex, data2 + " 23:59:59");
            }

            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()) {
                usl.add(rs1.getString("user"));
            }
            ps1.close();

            rs.beforeFirst();
            rs1.beforeFirst();

            usl = usl.stream().distinct().collect(Collectors.toList());

            ArrayList<Users> lius = list_all_users();
            ArrayList<Figures> lifi = list_all_figures();

            for (String cl : usl) {
                Users cls = get_user(cl, lius);
                C_CashierPerformance_value cp = new C_CashierPerformance_value();
                cp.setUser(cls.getCod() + " " + cls.getDe_cognome() + " " + cls.getDe_nome());
                int setFull = 0;
                int set0 = 0;
                int setnTrans = 0;
                int setNff = 0;
                int setDel = 0;
                ArrayList<String> data = new ArrayList<>();
                ArrayList<Integer> val = new ArrayList<>();
                if (bss.equals("B") || bss.equals("S")) {
                    for (int x = 0; x < fasce.size(); x++) {
                        val.add(0);
                    }
                } else {
                    val.add(0);
                }

                double setVolume = 0.00;
                double setComFix = 0.00;

                while (rs.next()) {
                    if (rs.getString("user").equals(cl)) {
                        if (rs.getString("del_fg").equals("1")) {
                            setDel++;
                        } else {
                            if (rs.getString("fix").equals("0.00")) {
                                setNff++;
                            }
                            setnTrans++;
                            setVolume = setVolume + fd(rs.getString("total"));
                            setComFix = setComFix + fd(rs.getString("commission"));
                            String sql2 = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                            PreparedStatement pstmt2 = this.c.prepareStatement(sql2);
                            pstmt2.setString(1, rs.getString("cod"));
                            ResultSet rs2 = pstmt2.executeQuery();

                            double fullvalue = 0.00;
                            double comvalue = 0.00;
                            int rowvalue = 0;
                            while (rs2.next()) {
                                rowvalue++;
                                String ki = rs2.getString("supporto");
                                Figures f1 = get_figures(lifi, ki);
                                if (rs.getString("tipotr").equals("B")) {
                                    fullvalue = fullvalue + fd(f1.getCommissione_acquisto());
                                } else {
                                    fullvalue = fullvalue + fd(f1.getCommissione_vendita());
                                }
                                comvalue = comvalue + fd(rs2.getString("com_perc"));
                            }
                            pstmt2.close();
                            if (rowvalue > 0) {
                                fullvalue = fullvalue / rowvalue;
                                comvalue = comvalue / rowvalue;
                            }

                            if (comvalue == 0.0) {
                                set0++;
                            } else if (comvalue == fullvalue) {
                                setFull++;
                            } else if (bss.equals("BS")) {
                                int neg = val.get(0);
                                neg++;
                                val.set(0, neg);
                            } else if (bss.equals("B") || bss.equals("S")) {
                                boolean found = false;
                                for (int x = 0; x < fasce.size(); x++) {
                                    String[] vx = fasce.get(x);
                                    if (comvalue >= fd(vx[1]) && comvalue <= fd(vx[2])) {
                                        found = true;
                                        int vv = val.get(x);
                                        vv++;
                                        val.set(x, vv);
                                        break;
                                    }
                                }
                                if (!found) {
                                    set0++;
                                }
                            }
                        }
                    }
                }

                data.add(set0 + "");
                for (int x = 0; x < val.size(); x++) {
                    data.add(String.valueOf(val.get(x)));
                }

                rs.beforeFirst();

                int setErr = 0;
                double setTotErr = 0.00;
                while (rs1.next()) {
                    if (rs1.getString("user").equals(cl)) {
                        setErr++;
                        ArrayList<String[]> err_value = list_oc_errors(rs1.getString("cod"));
                        for (int y = 0; y < err_value.size(); y++) {
                            String[] errv = err_value.get(y);
                            if (errv[1].equals("CH")) {
                                if (oper.equals("*")) {
                                    setTotErr = setTotErr + roundDouble(fd(errv[7]) * fd(errv[8]), 2);
                                } else {
                                    setTotErr = setTotErr + roundDouble(fd(errv[7]) / fd(errv[8]), 2);
                                }
                            }
                        }
                    }
                }

                rs1.beforeFirst();

                cp.setFull(setFull + "");
                cp.setnTrans(setnTrans + "");
                cp.setNff(setNff + "");
                cp.setDel(setDel + "");
                cp.setVolume(roundDoubleandFormat(setVolume, 2) + "");
                cp.setComFix(roundDoubleandFormat(setComFix, 2) + "");
                cp.setErr(setErr + "");
                cp.setTotErr(roundDoubleandFormat(setTotErr, 2) + "");
                cp.setDati(data);

                out.add(cp);

            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Users> list_all_users() {
        ArrayList<Users> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM users ORDER BY CAST(cod AS decimal (10,0))";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Users us = new Users();
                us.setFiliale(rs.getString(1));
                us.setCod(rs.getString(2));
                us.setUsername(rs.getString(3));
                us.setPwd(rs.getString(4));
                us.setDe_nome(rs.getString(5));
                us.setDe_cognome(rs.getString(6));
                us.setDt_mod_pwd(rs.getString(7));
                us.setValidita(rs.getString(8));
                us.setConto(rs.getString(9));
                us.setEmail(rs.getString(10));
                us.setFg_tipo(rs.getString(11));
                us.setFg_stato(rs.getString(12));
                us.setDt_insert(rs.getString(13));
                out.add(us);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_oc_errors(String cod_oc) {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,tipo,valuta,kind,gruppo_nc,carta_credito,note,total_diff,rate,data,"
                    + "quantity_user,total_user,quantity_system,total_system"
                    + " FROM oc_errors WHERE cod = ? ORDER BY tipo,valuta,kind,gruppo_nc,carta_credito";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod_oc);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String[] o1 = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10),
                        rs.getString(11),
                        rs.getString(12), rs.getString(13), rs.getString(14) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String getOAM(String annomese, String tipo) {
        try {
            String sql = "SELECT content FROM oam WHERE data like '" + annomese + "%' AND tipo='" + tipo
                    + "' ORDER by data desc LIMIT 1";
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
        return null;
    }

    public String getCORA(String annomese, String tipo) {
        try {
            String sql = "SELECT content FROM cora WHERE data like '" + annomese + "%' AND tipo='" + tipo
                    + "' ORDER by data desc LIMIT 1";

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
        return null;
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

    public ResultSet getSogliaTipologiaCL() {
        String query = "SELECT tipologia_clienti,ip_soglia_antiriciclaggio FROM tipologiaclienti "
                + "where fg_annullato='0' and tipologia_clienti<>'003' and filiale='000'";
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.executeQuery();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ResultSet getTransazioni(double soglia, String cod, String data_da, String data_a) {
        ResultSet rs = null;
        String query = "Select * from ch_transaction where tipocliente = ? and CAST(pay AS DECIMAL(10,2)) >= ? and data > '"
                + data_da + "' and data < '" + data_a + "'"; // MANCA WHERE
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ps.setDouble(2, soglia);
            rs = ps.executeQuery();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return rs;
    }

    public ResultSet getSogliaTipologiaCLRegistrazione() {
        String query = "SELECT tipologia_clienti,ip_soglia_antiriciclaggio FROM tipologiaclienti where fg_annullato='0' and filiale='000'";
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.executeQuery();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ResultSet getCommissione(String idCodTransazione) {
        String query = "SELECT * FROM ch_transaction_valori where cod_tr = ?";
        ResultSet rs = null;
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, idCodTransazione);
            rs = ps.executeQuery();
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return rs;
    }

    public String getCodiceValuta(String valuta) {
        String cod_valuta = "";
        String query = "select codice_uic_divisa FROM valute where valuta = ?";
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, valuta);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                cod_valuta = rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return cod_valuta;
    }

    public String getNDGSocieta(String cl_cod) {
        String query = "select ndg_rappresentante FROM anagrafica_ru where ndg = ?";
        String ndg = "";
        try {
            PreparedStatement ps = this.c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cl_cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                ndg = rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return ndg;
    }

    public Branchbudget get_branch_budget(String filiale, String meseanno) {
        try {
            String sql = "SELECT * FROM branchbudget WHERE branch = ? AND meseanno = ? ORDER BY timestamp DESC LIMIT 1";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
            ps.setString(2, meseanno);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Branchbudget(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10), rs.getString(11));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<Branchbudget> get_branch_budget_YTD(String filiale, String anno) {
        ArrayList<Branchbudget> lb = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branchbudget WHERE branch = ? AND meseanno like ? ORDER BY timestamp DESC";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
            ps.setString(2, anno + "%");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Branchbudget bb = new Branchbudget(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10), rs.getString(11));
                lb.add(bb);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return lb;
    }

    public String[] get_local_currency() {
        try {
            String sql = "SELECT valuta,codice_uic_divisa FROM valute WHERE fg_valuta_corrente = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "1");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new String[] { rs.getString(1), rs.getString(2) };
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        if (this.gf.isIs_UK()) {
            return new String[] { "GBP", "002" };
        } else if (this.gf.isIs_CZ()) {
            return new String[] { "CZK", "223" };
        } else {
            return new String[] { "EUR", "242" };
        }
    }

    public Daily_value list_Daily_value_2023_report(String[] fil, String datad1, String datad2, String valutalocale,
            ArrayList<String[]> cc, ArrayList<String[]> bc, ArrayList<String[]> listkind, ArrayList<NC_causal> nc_caus)
            throws SQLException {

        if (datad1 != null && datad2 != null) {

            try {

                Daily_value d = new Daily_value();

                double setPurchTotal = 0.0;
                double setPurchComm = 0.0;
                double setPurchGrossTot;
                double setPurchSpread = 0.0;
                double setPurchProfit;
                double setSalesTotal = 0.0;
                double setSalesComm = 0.0;
                double setSalesGrossTot = 0.0;
                double setSalesSpread = 0.0;
                double setSalesProfit;
                double setCashAdNetTot = 0.0;
                double setCashAdComm = 0.0;
                double setCashAdGrossTot;
                double setCashAdSpread = 0.0;
                double setCashAdProfit;

                double refund = 0.0;
                double refundshow = 0.0;

                // ArrayList<NC_causal> nc_caus = query_nc_causal_filial(fil[0], null);
                // ArrayList<String[]> listkind = nc_kind_order();
                // refund
                String sql0 = "SELECT value FROM ch_transaction_refund where status = '1' and method = 'BR' and branch_cod = '"
                        + fil[0] + "'";
                sql0 = sql0 + "AND dt_refund >= '" + datad1 + ":00' ";
                sql0 = sql0 + "AND dt_refund <= '" + datad2 + ":59' ";

                PreparedStatement ps0 = this.c.prepareStatement(sql0);

                ResultSet rs0 = ps0.executeQuery();
                while (rs0.next()) {
                    refund = refund + fd(rs0.getString("value"));
                    refundshow = refundshow + parseDoubleR(this.gf, rs0.getString("value"));
                }
                ps0.close();

                // TRANSACTION
                String sql = "SELECT tr1.cod,tr1.tipotr,tr1.pay,tr1.total,tr1.commission,tr1.round,tr1.spread_total,"
                        + "tr1.pos,tr1.localfigures FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = '"
                        + fil[0] + "' ";

                sql = sql + "AND tr1.data >= '" + datad1 + ":00' ";

                sql = sql + "AND tr1.data <= '" + datad2 + ":59' ";

                PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = ps.executeQuery();

                int setNoTransPurch = 0;
                int setNoTransCC = 0;
                int setNoTransSales = 0;
                @SuppressWarnings("unused")
                int setTotal = 0;
                int setTotPos = 0;
                int setTotAcc = 0;

                double poamount = 0.00;

                ArrayList<DailyCOP> dclist = new ArrayList<>();

                for (int x = 0; x < cc.size(); x++) {
                    DailyCOP dc = new DailyCOP(cc.get(x)[1], cc.get(x)[0]);
                    dclist.add(dc);
                }

                ArrayList<DailyBank> listdb = new ArrayList<>();

                for (int x = 0; x < bc.size(); x++) {
                    DailyBank dc = new DailyBank(bc.get(x)[1], bc.get(x)[0]);
                    listdb.add(dc);
                }
                int ij = 1;
                while (rs.next()) {
                    setTotal++;
                    if (rs.getString("tr1.tipotr").equals("B")) {
                        setNoTransPurch++;
                        String chTransactionValoriQuery = "SELECT supporto,net,total,tot_com,roundvalue,pos,spread FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psChTransactionValori = this.c.prepareStatement(chTransactionValoriQuery);
                        psChTransactionValori.setString(1, rs.getString("tr1.cod"));
                        ResultSet rsval = psChTransactionValori.executeQuery();

                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) {// CASH ADVANCE
                                setNoTransCC++;
                                setCashAdNetTot += fd(rsval.getString("net"));

                                if (this.gf.isIs_CZ()) {
                                    setCashAdComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setCashAdComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }

                                setNoTransPurch--;
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCashAdNtrans());
                                    start++;
                                    dc.setCashAdNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCashAdAmount());
                                    d1 += fd(rsval.getString("total"));
                                    dc.setCashAdAmount(roundDoubleandFormat(d1, 2));
                                }
                            } else if (rsval.getString("supporto").equals("06")) {// CREDIT CARD
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCcNtrans());
                                    start++;
                                    dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCcAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("07")) {// bancomat
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getBankNtrans());
                                    start++;
                                    dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getBankAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("08")) {
                                DailyBank dc = DailyBank.get_obj(listdb, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getNtrans());
                                    start++;
                                    dc.setNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else {
                                setPurchSpread += fd(rsval.getString("spread"));
                                setPurchTotal += fd(rsval.getString("net"));
                                if (this.gf.isIs_CZ()) {
                                    setPurchComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setPurchComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }
                            }
                        }
                        psChTransactionValori.close();

                    } else {
                        setNoTransSales++;
                        setSalesTotal += fd(rs.getString("tr1.pay"));
                        setSalesGrossTot += fd(rs.getString("tr1.total"));

                        setSalesComm += fd(rs.getString("tr1.commission")) + fd(rs.getString("tr1.round"));
                        ij++;
                        setSalesSpread += fd(rs.getString("tr1.spread_total"));

                        if (rs.getString("tr1.localfigures").equals("06")) {// CREDIT CARD
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getCcNtrans());
                                start++;
                                dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getCcAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("tr1.localfigures").equals("07")) {// bancomat
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getBankNtrans());
                                start++;
                                dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getBankAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("localfigures").equals("08")) {
                            DailyBank dc = DailyBank.get_obj(listdb, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getNtrans());
                                start++;
                                dc.setNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                            setTotAcc++;
                        }

                        String chTransactionValoriQuery = "SELECT supporto,total,tot_com,roundvalue,pos FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psChTransactionValori = this.c.prepareStatement(chTransactionValoriQuery,
                                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        psChTransactionValori.setString(1, rs.getString("tr1.cod"));
                        ResultSet rsval = (ResultSet) psChTransactionValori;
                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) {// CASH ADVANCE
                                setCashAdNetTot += fd(rsval.getString("total"));

                                if (this.gf.isIs_CZ()) {
                                    setCashAdComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setCashAdComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }

                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCashAdNtrans());
                                    start++;
                                    dc.setCashAdNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCashAdAmount());
                                    d1 += fd(rsval.getString("total"));
                                    dc.setCashAdAmount(roundDoubleandFormat(d1, 2));
                                }
                            }
                        }
                        psChTransactionValori.close();
                    }

                    setPurchGrossTot = setPurchTotal + setPurchComm;
                    // setSalesGrossTot = setSalesTotal - setSalesComm;
                    setCashAdGrossTot = setCashAdNetTot + setCashAdComm;

                    setPurchProfit = setPurchComm + setPurchSpread;
                    setSalesProfit = setSalesComm + setSalesSpread;
                    setCashAdProfit = setCashAdComm + setCashAdSpread;

                    // NO CHANGE
                    String sql1 = "SELECT causale_nc,supporto,total,pos,fg_inout,quantita FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                            + fil[0] + "' ";
                    sql1 = sql1 + "AND data >= '" + datad1 + ":00' ";
                    sql1 = sql1 + "AND data <= '" + datad2 + ":59' ";

                    PreparedStatement ps1 = this.c.prepareStatement(sql);

                    ResultSet rs1 = ps1.executeQuery();
                    double totalnotesnochange = 0.00;

                    // ArrayList<String[]> list_nc_descr = list_nc_descr();
                    ArrayList<DailyKind> dklist = new ArrayList<>();
                    ArrayList<DailyKind> dkATL = new ArrayList<>();

                    for (int i = 0; i < listkind.size(); i++) {
                        rs1.beforeFirst();

                        String kind = listkind.get(i)[0];
                        int setToNrTran = 0;
                        double setToTotal = 0.00;
                        double setToLocalCurr = 0.00;
                        double setToCC = 0.00;
                        double setToB = 0.00;
                        int setFromNrTran = 0;
                        double setFromTotal = 0.00;
                        double setFromLocalCurr = 0.00;
                        double setFromCC = 0.00;
                        double setFromB = 0.00;
                        while (rs1.next()) {
                            NC_causal nc2 = getNC_causal(nc_caus, rs1.getString("causale_nc"));
                            if (nc2 == null) {
                                continue;
                            }
                            String kindrs = nc2.getFg_tipo_transazione_nc();
                            if (kind.equals(kindrs)) {
                                String supporto = rs1.getString("supporto");
                                if (kindrs.equals("8")) {
                                    if (rs1.getString("fg_inout").equals("1")
                                            || rs1.getString("fg_inout").equals("3")) {

                                        setFromNrTran = setFromNrTran + parseIntR(rs1.getString("quantita"));
                                        setFromTotal = setFromTotal + fd(rs1.getString("total"));
                                        switch (supporto) {
                                            case "01":
                                            case "...":
                                                setFromLocalCurr = setFromLocalCurr + fd(rs1.getString("total"));
                                                break;
                                            case "04":
                                            case "06":
                                            case "07":
                                                if (supporto.equals("06")) {
                                                    setFromCC = setFromCC + fd(rs1.getString("total"));

                                                    DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                                    if (dc != null) {
                                                        double start = fd(dc.getNC_ccNtrans());
                                                        start++;
                                                        dc.setNC_ccNtrans(roundDoubleandFormat(start, 0));
                                                        double d1 = fd(dc.getNC_ccAmount());
                                                        d1 = d1 + fd(rs1.getString("total"));
                                                        dc.setNC_ccAmount(roundDoubleandFormat(d1, 2));
                                                        // poamount = poamount + fd(rs1.getString("total"));
                                                    }

                                                } else if (supporto.equals("07")) {
                                                    setFromB = setFromB + +fd(rs1.getString("total"));
                                                    DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                                    if (dc != null) {
                                                        double start = fd(dc.getNC_bankNtrans());
                                                        start++;
                                                        dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                        double d1 = fd(dc.getNC_bankAmount());
                                                        d1 = d1 + fd(rs1.getString("total"));
                                                        dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                        // poamount = poamount + fd(rs1.getString("total"));
                                                    }
                                                }
                                                break;
                                            case "08":
                                                DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                                if (dc != null) {
                                                    double start = fd(dc.getNC_bankNtrans());
                                                    start++;
                                                    dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                    double d1 = fd(dc.getNC_bankAmount());
                                                    d1 = d1 + fd(rs1.getString("total"));
                                                    dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                    // poamount = poamount + fd(rs1.getString("total"));
                                                }
                                                break;
                                            default:
                                                break;
                                        }
                                    } else {
                                        if (!nc2.getNc_de().equals("14")) {
                                            setToNrTran = setToNrTran + parseIntR(rs1.getString("quantita"));
                                            setToTotal = setToTotal + fd(rs1.getString("total"));
                                            if (supporto.equals("01") || supporto.equals("...")) {
                                                setToLocalCurr = setToLocalCurr + fd(rs1.getString("total"));
                                            } else if (supporto.equals("04") || supporto.equals("06")
                                                    || supporto.equals("07")) {
                                                if (supporto.equals("06")) {
                                                    setToCC = setToCC + fd(rs1.getString("total"));
                                                } else if (supporto.equals("07")) {
                                                    setToB = setToB + fd(rs1.getString("total"));
                                                }
                                            }
                                        }
                                    }
                                } else {

                                    if (rs1.getString("fg_inout").equals("1")
                                            || rs1.getString("fg_inout").equals("3")) {

                                        setFromNrTran++;
                                        setFromTotal = setFromTotal + fd(rs1.getString("total"));
                                        if (supporto.equals("01") || supporto.equals("...")) {
                                            setFromLocalCurr = setFromLocalCurr + fd(rs1.getString("total"));
                                        } else if (supporto.equals("04") || supporto.equals("06")
                                                || supporto.equals("07")) {
                                            if (supporto.equals("06")) {
                                                setFromCC = setFromCC + fd(rs1.getString("total"));
                                                DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                                if (dc != null) {
                                                    double start = fd(dc.getNC_ccNtrans());
                                                    start++;
                                                    dc.setNC_ccNtrans(roundDoubleandFormat(start, 0));
                                                    double d1 = fd(dc.getNC_ccAmount());
                                                    d1 = d1 + fd(rs1.getString("total"));
                                                    dc.setNC_ccAmount(roundDoubleandFormat(d1, 2));
                                                    // poamount = poamount + fd(rs1.getString("total"));
                                                }
                                            } else if (supporto.equals("07")) {
                                                setFromB = setFromB + +fd(rs1.getString("total"));
                                                DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                                if (dc != null) {
                                                    double start = fd(dc.getNC_bankNtrans());
                                                    start++;
                                                    dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                    double d1 = fd(dc.getNC_bankAmount());
                                                    d1 = d1 + fd(rs1.getString("total"));
                                                    dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                    // poamount = poamount + fd(rs1.getString("total"));
                                                }
                                            }
                                        } else if (supporto.equals("08")) {
                                            // poamount = poamount + fd(rs1.getString("total"));
                                            DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                            if (dc != null) {
                                                double start = fd(dc.getNC_bankNtrans());
                                                start++;
                                                dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                double d1 = fd(dc.getNC_bankAmount());
                                                d1 = d1 + fd(rs1.getString("total"));
                                                dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                // poamount = poamount + fd(rs1.getString("total"));
                                            }
                                        }
                                    } else {

                                        if (!nc2.getNc_de().equals("14")) { // solo gli acquisti non vengono considerati
                                            setToNrTran++;
                                            setToTotal = setToTotal + fd(rs1.getString("total"));
                                            if (supporto.equals("01") || supporto.equals("...")) {
                                                setToLocalCurr = setToLocalCurr + fd(rs1.getString("total"));
                                            } else if (supporto.equals("04") || supporto.equals("06")
                                                    || supporto.equals("07")) {
                                                if (supporto.equals("06")) {
                                                    setToCC = setToCC + fd(rs1.getString("total"));
                                                } else if (supporto.equals("07")) {
                                                    setToB = setToB + fd(rs1.getString("total"));
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                        }
                        ps1.close();

                        DailyKind dk = new DailyKind();
                        dk.setKind(listkind.get(i)[1]);

                        dk.setTo(true);

                        if (kind.equals("3") || kind.equals("8")) {
                            dk.setFrom(false);
                        } else {
                            dk.setFrom(true);
                        }
                        dk.setToNrTran(setToNrTran + "");
                        dk.setToTotal(roundDoubleandFormat(setToTotal, 2));
                        dk.setToLocalCurr(roundDoubleandFormat(setToLocalCurr, 2));
                        dk.setToCC(roundDoubleandFormat(setToCC, 2));
                        dk.setToB(roundDoubleandFormat(setToB, 2));
                        dk.setFromNrTran(setFromNrTran + "");
                        dk.setFromTotal(roundDoubleandFormat(setFromTotal, 2));
                        dk.setFromLocalCurr(roundDoubleandFormat(setFromLocalCurr, 2));
                        dk.setFromCC(roundDoubleandFormat(setFromCC, 2));
                        dk.setFromB(roundDoubleandFormat(setFromB, 2));
                        dk.setEtichetta1(listkind.get(i)[2]);
                        dk.setEtichetta2(listkind.get(i)[3]);

                        if (kind.equals("8")) {
                            dkATL.add(dk);
                        } else {
                            dklist.add(dk);
                            totalnotesnochange = totalnotesnochange + setToLocalCurr + setFromLocalCurr;
                        }
                    }

                    d.setDati(dklist);
                    d.setDatiatl(dkATL);

                    double setBaPurchTotal = 0.00;
                    double setBaPurchSpread = 0.00;
                    double setBaPurchCreditCard = 0.00;
                    double setBaPurchTransfNotes = 0.00;
                    double setBaPurchTransfOther = 0.00;
                    double setBaSalesTotal = 0.00;
                    double setBaSalesSpread = 0.00;
                    double setBaSalesCreditCard = 0.00;
                    double setBaSalesTransfNotes = 0.00;
                    double setBaSalesTransfOther = 0.00;
                    double setBraPurchTotal = 0.00;
                    double setBraPurchSpread = 0.00;
                    double setBraPurchLocalCurr = 0.00;
                    double setBraSalesTotal = 0.00;
                    double setBraSalesSpread = 0.00;
                    double setBraSalesLocalCurr = 0.00;

                    // EXTERNAL TRANSFER
                    String sql2 = "SELECT * FROM et_change WHERE fg_annullato = '0' AND filiale = '" + fil[0] + "' ";
                    sql2 = sql2 + "AND dt_it >= '" + datad1 + ":00' ";
                    sql2 = sql2 + "AND dt_it <= '" + datad2 + ":59' ";
                    PreparedStatement ps2 = this.c.prepareStatement(sql2);
                    ResultSet rs2 = ps2.executeQuery();

                    while (rs2.next()) {
                        String etChangeValoriQuery = "SELECT * FROM et_change_valori WHERE cod = ?";
                        PreparedStatement psEtChangeValori = this.c.prepareStatement(etChangeValoriQuery);
                        psEtChangeValori.setString(1, rs2.getString("cod"));
                        ResultSet rs2val = psEtChangeValori.executeQuery();

                        if (rs2.getString("fg_tofrom").equals("T")) { // sales
                            if (rs2.getString("fg_brba").equals("BA")) { // BANK
                                while (rs2val.next()) {
                                    setBaSalesSpread += fd(rs2val.getString("ip_spread"));
                                    if (rs2val.getString("kind").equals("01")) {
                                        if (rs2val.getString("currency").equals(valutalocale)) {
                                            setBaSalesTransfNotes += fd(rs2val.getString("ip_total"));
                                        } else {
                                            setBaSalesTotal += fd(rs2val.getString("ip_total"));
                                        }
                                    } else if (rs2val.getString("kind").equals("02")
                                            || rs2val.getString("kind").equals("03")) {
                                        setBaSalesTotal += fd(rs2val.getString("ip_total"));
                                    } else if (rs2val.getString("kind").equals("04")) {
                                        setBaSalesCreditCard += fd(rs2val.getString("ip_total"));
                                    } else {
                                        setBaSalesTransfOther += fd(rs2val.getString("ip_total"));
                                    }
                                }
                                psEtChangeValori.close();
                            } else {// BRANCH
                                while (rs2val.next()) {
                                    setBraSalesSpread += fd(rs2val.getString("ip_spread"));
                                    if (rs2val.getString("currency").equals(valutalocale)
                                            && rs2val.getString("kind").equals("01")) {
                                        setBraSalesLocalCurr += fd(rs2val.getString("ip_total"));
                                    } else {
                                        setBraSalesTotal += fd(rs2val.getString("ip_total"));
                                    }
                                }
                            }
                        } else if (rs2.getString("fg_brba").equals("BA")) { // BANK
                            while (rs2val.next()) {
                                setBaPurchSpread += fd(rs2val.getString("ip_spread"));
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        setBaPurchTransfNotes += fd(rs2val.getString("ip_total"));
                                    } else {
                                        setBaPurchTotal += fd(rs2val.getString("ip_total"));
                                    }
                                } else if (rs2val.getString("kind").equals("02")
                                        || rs2val.getString("kind").equals("03")) {
                                    setBaPurchTotal += fd(rs2val.getString("ip_total"));
                                } else if (rs2val.getString("kind").equals("04")) {
                                    setBaPurchCreditCard += fd(rs2val.getString("ip_total"));
                                } else {
                                    setBaPurchTransfOther += fd(rs2val.getString("ip_total"));
                                }
                            }
                        } else {// BRANCH
                            while (rs2val.next()) {
                                setBraPurchSpread += fd(rs2val.getString("ip_spread"));
                                if (rs2val.getString("currency").equals(valutalocale)
                                        && rs2val.getString("kind").equals("01")) {
                                    setBraPurchLocalCurr += fd(rs2val.getString("ip_total"));
                                } else {
                                    setBraPurchTotal += fd(rs2val.getString("ip_total"));
                                }
                            }
                        }
                        ps2.close();
                    }

                    d.setPurchTotal(roundDoubleandFormat(setPurchTotal, 2));
                    d.setPurchComm(roundDoubleandFormat(setPurchComm, 2));
                    d.setPurchGrossTot(roundDoubleandFormat(setPurchGrossTot, 2));
                    d.setPurchSpread(roundDoubleandFormat(setPurchSpread, 2));
                    d.setPurchProfit(roundDoubleandFormat(setPurchProfit, 2));

                    d.setSalesTotal(roundDoubleandFormat(setSalesTotal, 2));
                    d.setSalesComm(roundDoubleandFormat(setSalesComm, 2));
                    d.setSalesGrossTot(roundDoubleandFormat(setSalesGrossTot, 2));
                    d.setSalesSpread(roundDoubleandFormat(setSalesSpread, 2));
                    d.setSalesProfit(roundDoubleandFormat(setSalesProfit, 2));

                    d.setCashAdNetTot(roundDoubleandFormat(setCashAdNetTot, 2));
                    d.setCashAdComm(roundDoubleandFormat(setCashAdComm, 2));
                    d.setCashAdGrossTot(roundDoubleandFormat(setCashAdGrossTot, 2));
                    d.setCashAdSpread(roundDoubleandFormat(setCashAdSpread, 2));
                    d.setCashAdProfit(roundDoubleandFormat(setCashAdProfit, 2));

                    d.setDatiCOP(dclist);

                    d.setBaPurchTotal(roundDoubleandFormat(setBaPurchTotal, 2));
                    d.setBaPurchSpread(roundDoubleandFormat(setBaPurchSpread, 2));
                    d.setBaPurchCreditCard(roundDoubleandFormat(setBaPurchCreditCard, 2));
                    d.setBaPurchTransfNotes(roundDoubleandFormat(setBaPurchTransfNotes, 2));
                    d.setBaPurchTransfOther(roundDoubleandFormat(setBaPurchTransfOther, 2));
                    d.setBaSalesTotal(roundDoubleandFormat(setBaSalesTotal, 2));
                    d.setBaSalesSpread(roundDoubleandFormat(setBaSalesSpread, 2));
                    d.setBaSalesCreditCard(roundDoubleandFormat(setBaSalesCreditCard, 2));
                    d.setBaSalesTransfNotes(roundDoubleandFormat(setBaSalesTransfNotes, 2));
                    d.setBaSalesTransfOther(roundDoubleandFormat(setBaSalesTransfOther, 2));

                    d.setBraPurchTotal(roundDoubleandFormat(setBraPurchTotal, 2));
                    d.setBraPurchSpread(roundDoubleandFormat(setBraPurchSpread, 2));
                    d.setBraPurchLocalCurr(roundDoubleandFormat(setBraPurchLocalCurr, 2));
                    d.setBraSalesTotal(roundDoubleandFormat(setBraSalesTotal, 2));
                    d.setBraSalesSpread(roundDoubleandFormat(setBraSalesSpread, 2));
                    d.setBraSalesLocalCurr(roundDoubleandFormat(setBraSalesLocalCurr, 2));

                    d.setRefund(roundDoubleandFormat(refundshow, 2));

                    double setGroffTurnover = setPurchTotal + setSalesTotal + setCashAdNetTot;
                    double setGrossProfit = setPurchComm + setSalesComm + setCashAdComm + setSalesSpread
                            + setBaSalesSpread + setBraSalesSpread;
                    double setLastCashOnPrem = 0.0;
                    double setFx = 0.0;

                    ArrayList<Office_sp> li = list_query_officesp2(fil[0],
                            subDays(datad1.substring(0, 10), patternsql, 1));
                    if (!li.isEmpty()) {
                        Office_sp o = li.get(0);
                        d.setOfficesp(o.getCodice());
                        setLastCashOnPrem = fd(o.getTotal_cod());
                    } else {
                        d.setOfficesp(null);
                    }

                    Office_sp o = list_query_last_officesp(fil[0], datad2);
                    if (o != null) {
                        // double[] d1 = list_dettagliotransazioni(fil, o.getData(), datad2,
                        // valutalocale);
                        setFx = fd(o.getTotal_fx());
                    }

                    String oper = get_national_office().getChangetype();
                    boolean dividi = oper.equals("/");

                    double setCashOnPremFromTrans = setSalesTotal
                            - setPurchTotal
                            + setBaPurchTransfNotes
                            - setBaSalesTransfNotes
                            + setBraPurchLocalCurr
                            - setBraSalesLocalCurr
                            + totalnotesnochange
                            + setLastCashOnPrem
                            - refund
                            - setCashAdNetTot
                            - poamount;

                    double setFxClosureErrorDeclared = 0.0;
                    double setCashOnPremError = 0.0;
                    String sql10 = "SELECT valuta,kind,total_user,total_system,rate FROM oc_errors where filiale = '"
                            + fil[0]
                            + "' AND cod IN (SELECT cod FROM oc_lista WHERE data >= '" + datad1
                            + ":00' AND data <= '" + datad2 + ":59' AND errors='Y')"
                            + " AND tipo='CH' AND (kind='01' OR kind='02' OR kind='03')";
                    PreparedStatement ps10 = this.c.prepareStatement(sql0);
                    ResultSet rs10 = ps10.executeQuery();
                    while (rs10.next()) {
                        if (rs10.getString("valuta").equals(valutalocale) && rs10.getString("kind").equals("01")) {
                            double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                            setCashOnPremError = setCashOnPremError + eurerr;
                        } else {
                            double fxerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                            setFxClosureErrorDeclared = setFxClosureErrorDeclared
                                    + getControvalore(fxerr, fd(rs10.getString("rate")), dividi);
                        }
                        ps10.close();
                    }

                    double setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;
                    d.setGroffTurnover(roundDoubleandFormat(setGroffTurnover, 2));
                    d.setGrossProfit(roundDoubleandFormat(setGrossProfit, 2));
                    d.setLastCashOnPrem(roundDoubleandFormat(setLastCashOnPrem, 2));
                    d.setCashOnPrem(roundDoubleandFormat(setCashOnPrem, 2));
                    d.setFx(roundDoubleandFormat(setFx, 2));
                    d.setCashOnPremFromTrans(roundDoubleandFormat(setCashOnPremFromTrans, 2));

                    if (setCashOnPremError == 0) {
                        d.setCashOnPremError(roundDoubleandFormat(setCashOnPremError, 2).replaceAll("-", ""));
                    } else {
                        d.setCashOnPremError(roundDoubleandFormat(setCashOnPremError, 2));
                    }

                    d.setFxClosureErrorDeclared(roundDoubleandFormat(setFxClosureErrorDeclared, 2));
                    d.setNoTransPurch(String.valueOf(setNoTransPurch));
                    d.setNoTransCC(String.valueOf(setNoTransCC));
                    d.setNoTransSales(String.valueOf(setNoTransSales));

                    d.setTotal(String.valueOf(setNoTransPurch + setNoTransCC + setNoTransSales));

                    d.setTotPos(String.valueOf(setTotPos));
                    d.setTotAcc(String.valueOf(setTotAcc));
                    d.setDatiBank(listdb);
                    d.setId_filiale(fil[0]);
                    d.setDe_filiale(fil[1]);
                    d.setData(formatStringtoStringDate(datad1.substring(0, 10), patternsql, patternnormdate_filter));
                    return d;
                }
                ps.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public Daily_value list_Daily_value(String[] fil, String datad1, String datad2, boolean now, boolean contuk) {

        if (datad1 != null && datad2 != null) {

            try {

                String valutalocale = get_local_currency()[0];

                Daily_value d = new Daily_value();

                double setPurchTotal = 0.0;
                double setPurchComm = 0.0;
                double setPurchGrossTot;
                double setPurchSpread = 0.0;
                double setPurchProfit;
                double setSalesTotal = 0.0;
                double setSalesComm = 0.0;
                double setSalesGrossTot = 0.0;
                double setSalesSpread = 0.0;
                double setSalesProfit;
                double setCashAdNetTot = 0.0;
                double setCashAdComm = 0.0;
                double setCashAdGrossTot;
                double setCashAdSpread = 0.0;
                double setCashAdProfit;

                double refund = 0.0;
                double refundshow = 0.0;

                ArrayList<NC_causal> nc_caus = query_nc_causal_filial(fil[0], null);
                ArrayList<String[]> listkind = nc_kind_order();

                // refund
                String sql0 = "SELECT value FROM ch_transaction_refund where status = '1' and method = 'BR' and branch_cod = '"
                        + fil[0] + "'";

                sql0 = sql0 + "AND dt_refund >= '" + datad1 + ":00' ";

                sql0 = sql0 + "AND dt_refund <= '" + datad2 + ":59' ";

                sql0 = sql0 + " ORDER BY dt_refund";

                PreparedStatement ps0 = this.c.prepareStatement(sql0);
                ResultSet rs0 = ps0.executeQuery();
                while (rs0.next()) {
                    refund = refund + fd(rs0.getString("value"));
                    refundshow = refundshow + parseDoubleR(this.gf, rs0.getString("value"));
                }
                ps0.close();

                // int cartasi_setCashAdNtrans = 0;
                // double cartasi_setCashAdAmount = 0.0;
                // int cartasi_setCcNtrans = 0;
                // double cartasi_setCcAmount = 0.0;
                // int cartasi_setBankNtrans = 0;
                // double cartasi_setBankAmount = 0.0;
                //
                // int setefi_setCashAdNtrans = 0;
                // double setefi_setCashAdAmount = 0.0;
                // int setefi_setCcNtrans = 0;
                // double setefi_setCcAmount = 0.0;
                // int setefi_setBankNtrans = 0;
                // double setefi_setBankAmount = 0.0;
                // TRANSACTION
                String sql = "SELECT tr1.cod,tr1.tipotr,tr1.pay,tr1.total,tr1.commission,tr1.round,tr1.spread_total,"
                        + "tr1.pos,tr1.localfigures FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = '"
                        + fil[0] + "' ";

                sql = sql + "AND tr1.data >= '" + datad1 + ":00' ";

                sql = sql + "AND tr1.data <= '" + datad2 + ":59' ";

                // sql = sql + " ORDER BY tr1.data";
                PreparedStatement ps = this.c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery();

                int setNoTransPurch = 0;
                int setNoTransCC = 0;
                int setNoTransSales = 0;
                int setTotal = 0;
                int setTotPos = 0;
                int setTotAcc = 0;

                double poamount = 0.00;

                ArrayList<String[]> cc = credit_card_enabled();

                ArrayList<String[]> bc = list_bankAccount();

                ArrayList<DailyCOP> dclist = new ArrayList<>();

                for (int x = 0; x < cc.size(); x++) {
                    DailyCOP dc = new DailyCOP(cc.get(x)[1], cc.get(x)[0]);
                    dclist.add(dc);
                }

                ArrayList<DailyBank> listdb = new ArrayList<>();

                for (int x = 0; x < bc.size(); x++) {
                    DailyBank dc = new DailyBank(bc.get(x)[1], bc.get(x)[0]);
                    listdb.add(dc);
                }
                int ij = 1;
                while (rs.next()) {
                    setTotal++;
                    if (rs.getString("tr1.tipotr").equals("B")) {
                        setNoTransPurch++;

                        String chTransactionValoriQuery = "SELECT supporto, net, total, tot_com, roundvalue, pos, spread FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psChTransactionValori = this.c.prepareStatement(chTransactionValoriQuery);
                        psChTransactionValori.setString(1, rs.getString("tr1.cod"));
                        ResultSet rsval = psChTransactionValori.executeQuery();

                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) { // CASH ADVANCE
                                setNoTransCC++;
                                setCashAdNetTot += fd(rsval.getString("net"));

                                if (this.gf.isIs_CZ()) {
                                    setCashAdComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setCashAdComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }

                                setNoTransPurch--;
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCashAdNtrans());
                                    start++;
                                    dc.setCashAdNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCashAdAmount());
                                    d1 += fd(rsval.getString("total"));
                                    dc.setCashAdAmount(roundDoubleandFormat(d1, 2));
                                }
                            } else if (rsval.getString("supporto").equals("06")) { // CREDIT CARD
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCcNtrans());
                                    start++;
                                    dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCcAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("07")) { // bancomat
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getBankNtrans());
                                    start++;
                                    dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getBankAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("08")) {
                                DailyBank dc = DailyBank.get_obj(listdb, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getNtrans());
                                    start++;
                                    dc.setNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getAmount());
                                    d1 += fd(rsval.getString("net"));
                                    dc.setAmount(roundDoubleandFormat(d1, 2));
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else {
                                setPurchSpread += fd(rsval.getString("spread"));
                                setPurchTotal += fd(rsval.getString("net"));
                                if (this.gf.isIs_CZ()) {
                                    setPurchComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setPurchComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }
                            }
                        }
                        psChTransactionValori.close();

                    } else {
                        setNoTransSales++;
                        setSalesTotal += fd(rs.getString("tr1.pay"));
                        setSalesGrossTot += fd(rs.getString("tr1.total"));
                        setSalesComm += fd(rs.getString("tr1.commission")) + fd(rs.getString("tr1.round"));
                        ij++;
                        setSalesSpread += fd(rs.getString("tr1.spread_total"));

                        if (rs.getString("tr1.localfigures").equals("06")) { // CREDIT CARD
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getCcNtrans());
                                start++;
                                dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getCcAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("tr1.localfigures").equals("07")) { // bancomat
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getBankNtrans());
                                start++;
                                dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getBankAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("localfigures").equals("08")) {
                            DailyBank dc = DailyBank.get_obj(listdb, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getNtrans());
                                start++;
                                dc.setNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getAmount());
                                d1 += fd(rs.getString("tr1.pay"));
                                dc.setAmount(roundDoubleandFormat(d1, 2));
                                poamount += fd(rs.getString("tr1.pay"));
                            }

                            setTotAcc++;
                        }

                        String chTransactionValoriQuery = "SELECT supporto, total, tot_com, roundvalue, pos FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psChTransactionValori = this.c.prepareStatement(chTransactionValoriQuery);
                        psChTransactionValori.setString(1, rs.getString("tr1.cod"));
                        ResultSet rsval = psChTransactionValori.executeQuery();

                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) { // CASH ADVANCE
                                setCashAdNetTot += fd(rsval.getString("total"));

                                if (this.gf.isIs_CZ()) {
                                    setCashAdComm += fd(rsval.getString("tot_com"))
                                            + parseDoubleR_CZ(this.gf, rsval.getString("roundvalue"), true);
                                } else {
                                    setCashAdComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                }

                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCashAdNtrans());
                                    start++;
                                    dc.setCashAdNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCashAdAmount());
                                    d1 += fd(rsval.getString("total"));
                                    dc.setCashAdAmount(roundDoubleandFormat(d1, 2));
                                }
                            }
                        }
                        psChTransactionValori.close();
                    }
                }
                ps.close();

                setPurchGrossTot = setPurchTotal + setPurchComm;
                // setSalesGrossTot = setSalesTotal - setSalesComm;
                setCashAdGrossTot = setCashAdNetTot + setCashAdComm;

                setPurchProfit = setPurchComm + setPurchSpread;
                setSalesProfit = setSalesComm + setSalesSpread;
                setCashAdProfit = setCashAdComm + setCashAdSpread;

                // NO CHANGE
                String sql1 = "SELECT causale_nc,supporto,total,pos,fg_inout,quantita FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                        + fil[0] + "' ";

                if (this.gf.isIs_UK()) {
                    sql1 = sql1 + "AND gruppo_nc <> 'DEPOS' ";
                }

                // String sql1 = "SELECT * FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                // + fil[0] + "' ";
                sql1 = sql1 + "AND data >= '" + datad1 + ":00' ";

                sql1 = sql1 + "AND data <= '" + datad2 + ":59' ";

                // sql1 = sql1 + " ORDER BY data";
                PreparedStatement ps1 = this.c.prepareStatement(sql1);
                ResultSet rs1 = ps1.executeQuery();
                double totalnotesnochange = 0.00;

                // ArrayList<String[]> list_nc_descr = list_nc_descr();
                ArrayList<DailyKind> dklist = new ArrayList<>();
                ArrayList<DailyKind> dkATL = new ArrayList<>();

                for (int i = 0; i < listkind.size(); i++) {
                    rs1.beforeFirst();

                    String kind = listkind.get(i)[0];
                    int setToNrTran = 0;
                    double setToTotal = 0.00;
                    double setToLocalCurr = 0.00;
                    double setToCC = 0.00;
                    double setToB = 0.00;
                    int setFromNrTran = 0;
                    double setFromTotal = 0.00;
                    double setFromLocalCurr = 0.00;
                    double setFromCC = 0.00;
                    double setFromB = 0.00;
                    while (rs1.next()) {
                        NC_causal nc2 = getNC_causal(nc_caus, rs1.getString("causale_nc"));
                        if (nc2 == null) {
                            continue;
                        }
                        String kindrs = nc2.getFg_tipo_transazione_nc();
                        if (kind.equals(kindrs)) {
                            String supporto = rs1.getString("supporto");
                            if (kindrs.equals("8")) {
                                if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {

                                    setFromNrTran = setFromNrTran + parseIntR(rs1.getString("quantita"));
                                    setFromTotal = setFromTotal + fd(rs1.getString("total"));
                                    if (supporto.equals("01") || supporto.equals("...")) {
                                        setFromLocalCurr = setFromLocalCurr + fd(rs1.getString("total"));
                                    } else if (supporto.equals("04") || supporto.equals("06")
                                            || supporto.equals("07")) {
                                        if (supporto.equals("06")) {
                                            setFromCC = setFromCC + fd(rs1.getString("total"));

                                            DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                            if (dc != null) {
                                                double start = fd(dc.getNC_ccNtrans());
                                                start++;
                                                dc.setNC_ccNtrans(roundDoubleandFormat(start, 0));
                                                double d1 = fd(dc.getNC_ccAmount());
                                                d1 = d1 + fd(rs1.getString("total"));
                                                dc.setNC_ccAmount(roundDoubleandFormat(d1, 2));
                                                // poamount = poamount + fd(rs1.getString("total"));
                                            }

                                        } else if (supporto.equals("07")) {
                                            setFromB = setFromB + +fd(rs1.getString("total"));
                                            DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                            if (dc != null) {
                                                double start = fd(dc.getNC_bankNtrans());
                                                start++;
                                                dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                double d1 = fd(dc.getNC_bankAmount());
                                                d1 = d1 + fd(rs1.getString("total"));
                                                dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                // poamount = poamount + fd(rs1.getString("total"));
                                            }
                                        }
                                    } else if (supporto.equals("08")) {
                                        DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                        if (dc != null) {
                                            double start = fd(dc.getNC_bankNtrans());
                                            start++;
                                            dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                            double d1 = fd(dc.getNC_bankAmount());
                                            d1 = d1 + fd(rs1.getString("total"));
                                            dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                            // poamount = poamount + fd(rs1.getString("total"));
                                        }
                                    }
                                } else {
                                    if (!nc2.getNc_de().equals("14")) {
                                        setToNrTran = setToNrTran + parseIntR(rs1.getString("quantita"));
                                        setToTotal = setToTotal + fd(rs1.getString("total"));
                                        if (supporto.equals("01") || supporto.equals("...")) {
                                            setToLocalCurr = setToLocalCurr + fd(rs1.getString("total"));
                                        } else if (supporto.equals("04") || supporto.equals("06")
                                                || supporto.equals("07")) {
                                            if (supporto.equals("06")) {
                                                setToCC = setToCC + fd(rs1.getString("total"));
                                            } else if (supporto.equals("07")) {
                                                setToB = setToB + fd(rs1.getString("total"));
                                            }
                                        }
                                    }
                                }
                            } else {

                                if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {

                                    setFromNrTran++;
                                    setFromTotal = setFromTotal + fd(rs1.getString("total"));
                                    if (supporto.equals("01") || supporto.equals("...")) {
                                        setFromLocalCurr = setFromLocalCurr + fd(rs1.getString("total"));
                                    } else if (supporto.equals("04") || supporto.equals("06")
                                            || supporto.equals("07")) {
                                        if (supporto.equals("06")) {
                                            setFromCC = setFromCC + fd(rs1.getString("total"));
                                            DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                            if (dc != null) {
                                                double start = fd(dc.getNC_ccNtrans());
                                                start++;
                                                dc.setNC_ccNtrans(roundDoubleandFormat(start, 0));
                                                double d1 = fd(dc.getNC_ccAmount());
                                                d1 = d1 + fd(rs1.getString("total"));
                                                dc.setNC_ccAmount(roundDoubleandFormat(d1, 2));
                                                // poamount = poamount + fd(rs1.getString("total"));
                                            }
                                        } else if (supporto.equals("07")) {
                                            setFromB = setFromB + +fd(rs1.getString("total"));
                                            DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                            if (dc != null) {
                                                double start = fd(dc.getNC_bankNtrans());
                                                start++;
                                                dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                                double d1 = fd(dc.getNC_bankAmount());
                                                d1 = d1 + fd(rs1.getString("total"));
                                                dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                                // poamount = poamount + fd(rs1.getString("total"));
                                            }
                                        }
                                    } else if (supporto.equals("08")) {
                                        // poamount = poamount + fd(rs1.getString("total"));
                                        DailyCOP dc = DailyCOP.get_obj(dclist, rs1.getString("pos"));
                                        if (dc != null) {
                                            double start = fd(dc.getNC_bankNtrans());
                                            start++;
                                            dc.setNC_bankNtrans(roundDoubleandFormat(start, 0));
                                            double d1 = fd(dc.getNC_bankAmount());
                                            d1 = d1 + fd(rs1.getString("total"));
                                            dc.setNC_bankAmount(roundDoubleandFormat(d1, 2));
                                            // poamount = poamount + fd(rs1.getString("total"));
                                        }
                                    }
                                } else {

                                    if (!nc2.getNc_de().equals("14")) { // solo gli acquisti non vengono considerati
                                        setToNrTran++;
                                        setToTotal = setToTotal + fd(rs1.getString("total"));
                                        if (supporto.equals("01") || supporto.equals("...")) {
                                            setToLocalCurr = setToLocalCurr + fd(rs1.getString("total"));
                                        } else if (supporto.equals("04") || supporto.equals("06")
                                                || supporto.equals("07")) {
                                            if (supporto.equals("06")) {
                                                setToCC = setToCC + fd(rs1.getString("total"));
                                            } else if (supporto.equals("07")) {
                                                setToB = setToB + fd(rs1.getString("total"));
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    ps1.close();

                    DailyKind dk = new DailyKind();
                    dk.setKind(listkind.get(i)[1]);

                    dk.setTo(true);

                    if (kind.equals("3") || kind.equals("8")) {
                        dk.setFrom(false);
                    } else {
                        dk.setFrom(true);
                    }
                    dk.setToNrTran(setToNrTran + "");
                    dk.setToTotal(roundDoubleandFormat(setToTotal, 2));
                    dk.setToLocalCurr(roundDoubleandFormat(setToLocalCurr, 2));
                    dk.setToCC(roundDoubleandFormat(setToCC, 2));
                    dk.setToB(roundDoubleandFormat(setToB, 2));
                    dk.setFromNrTran(setFromNrTran + "");
                    dk.setFromTotal(roundDoubleandFormat(setFromTotal, 2));
                    dk.setFromLocalCurr(roundDoubleandFormat(setFromLocalCurr, 2));
                    dk.setFromCC(roundDoubleandFormat(setFromCC, 2));
                    dk.setFromB(roundDoubleandFormat(setFromB, 2));
                    dk.setEtichetta1(listkind.get(i)[2]);
                    dk.setEtichetta2(listkind.get(i)[3]);

                    if (kind.equals("8")) {
                        dkATL.add(dk);
                    } else {
                        dklist.add(dk);
                        totalnotesnochange = totalnotesnochange + setToLocalCurr + setFromLocalCurr;
                    }
                }

                d.setDati(dklist);
                d.setDatiatl(dkATL);

                double setBaPurchTotal = 0.00;
                double setBaPurchSpread = 0.00;
                double setBaPurchCreditCard = 0.00;
                double setBaPurchTransfNotes = 0.00;
                double setBaPurchTransfOther = 0.00;
                double setBaSalesTotal = 0.00;
                double setBaSalesSpread = 0.00;
                double setBaSalesCreditCard = 0.00;
                double setBaSalesTransfNotes = 0.00;
                double setBaSalesTransfOther = 0.00;
                double setBraPurchTotal = 0.00;
                double setBraPurchSpread = 0.00;
                double setBraPurchLocalCurr = 0.00;
                double setBraSalesTotal = 0.00;
                double setBraSalesSpread = 0.00;
                double setBraSalesLocalCurr = 0.00;

                // EXTERNAL TRANSFER
                String sql2 = "SELECT * FROM et_change WHERE fg_annullato = '0' AND filiale = '" + fil[0] + "' ";

                if (contuk) {
                    sql2 = sql2 + "AND cod_dest <> '000' ";
                }

                sql2 = sql2 + "AND dt_it >= '" + datad1 + ":00' ";

                sql2 = sql2 + "AND dt_it <= '" + datad2 + ":59' ";

                sql2 = sql2 + " ORDER BY dt_it";
                PreparedStatement ps2 = this.c.prepareStatement(sql2);
                ResultSet rs2 = ps2.executeQuery();

                while (rs2.next()) {
                    String etChangeValoriQuery = "SELECT * FROM et_change_valori WHERE cod = ?";
                    PreparedStatement psEtChangeValori = this.c.prepareStatement(etChangeValoriQuery);
                    psEtChangeValori.setString(1, rs2.getString("cod"));
                    ResultSet rs2val = psEtChangeValori.executeQuery();

                    if (rs2.getString("fg_tofrom").equals("T")) { // sales
                        if (rs2.getString("fg_brba").equals("BA")) { // BANK
                            while (rs2val.next()) {
                                setBaSalesSpread += fd(rs2val.getString("ip_spread"));
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        setBaSalesTransfNotes += fd(rs2val.getString("ip_total"));
                                    } else {
                                        setBaSalesTotal += fd(rs2val.getString("ip_total"));
                                    }
                                } else if (rs2val.getString("kind").equals("02")
                                        || rs2val.getString("kind").equals("03")) {
                                    setBaSalesTotal += fd(rs2val.getString("ip_total"));
                                } else if (rs2val.getString("kind").equals("04")) {
                                    setBaSalesCreditCard += fd(rs2val.getString("ip_total"));
                                } else {
                                    setBaSalesTransfOther += fd(rs2val.getString("ip_total"));
                                }
                            }
                            psEtChangeValori.close();
                        } else { // BRANCH
                            while (rs2val.next()) {
                                setBraSalesSpread += fd(rs2val.getString("ip_spread"));
                                if (rs2val.getString("currency").equals(valutalocale)
                                        && rs2val.getString("kind").equals("01")) {
                                    setBraSalesLocalCurr += fd(rs2val.getString("ip_total"));
                                } else {
                                    setBraSalesTotal += fd(rs2val.getString("ip_total"));
                                }
                            }
                        }
                    } else if (rs2.getString("fg_brba").equals("BA")) { // BANK
                        while (rs2val.next()) {
                            setBaPurchSpread += fd(rs2val.getString("ip_spread"));
                            if (rs2val.getString("kind").equals("01")) {
                                if (rs2val.getString("currency").equals(valutalocale)) {
                                    setBaPurchTransfNotes += fd(rs2val.getString("ip_total"));
                                } else {
                                    setBaPurchTotal += fd(rs2val.getString("ip_total"));
                                }
                            } else if (rs2val.getString("kind").equals("02") || rs2val.getString("kind").equals("03")) {
                                setBaPurchTotal += fd(rs2val.getString("ip_total"));
                            } else if (rs2val.getString("kind").equals("04")) {
                                setBaPurchCreditCard += fd(rs2val.getString("ip_total"));
                            } else {
                                setBaPurchTransfOther += fd(rs2val.getString("ip_total"));
                            }
                        }
                    } else { // BRANCH
                        while (rs2val.next()) {
                            setBraPurchSpread += fd(rs2val.getString("ip_spread"));
                            if (rs2val.getString("currency").equals(valutalocale)
                                    && rs2val.getString("kind").equals("01")) {
                                setBraPurchLocalCurr += fd(rs2val.getString("ip_total"));
                            } else {
                                setBraPurchTotal += fd(rs2val.getString("ip_total"));
                            }
                        }
                    }
                }
                ps2.close();

                d.setPurchTotal(roundDoubleandFormat(setPurchTotal, 2));
                d.setPurchComm(roundDoubleandFormat(setPurchComm, 2));
                d.setPurchGrossTot(roundDoubleandFormat(setPurchGrossTot, 2));
                d.setPurchSpread(roundDoubleandFormat(setPurchSpread, 2));
                d.setPurchProfit(roundDoubleandFormat(setPurchProfit, 2));

                d.setSalesTotal(roundDoubleandFormat(setSalesTotal, 2));
                d.setSalesComm(roundDoubleandFormat(setSalesComm, 2));
                d.setSalesGrossTot(roundDoubleandFormat(setSalesGrossTot, 2));
                d.setSalesSpread(roundDoubleandFormat(setSalesSpread, 2));
                d.setSalesProfit(roundDoubleandFormat(setSalesProfit, 2));

                d.setCashAdNetTot(roundDoubleandFormat(setCashAdNetTot, 2));
                d.setCashAdComm(roundDoubleandFormat(setCashAdComm, 2));
                d.setCashAdGrossTot(roundDoubleandFormat(setCashAdGrossTot, 2));
                d.setCashAdSpread(roundDoubleandFormat(setCashAdSpread, 2));
                d.setCashAdProfit(roundDoubleandFormat(setCashAdProfit, 2));

                d.setDatiCOP(dclist);

                d.setBaPurchTotal(roundDoubleandFormat(setBaPurchTotal, 2));
                d.setBaPurchSpread(roundDoubleandFormat(setBaPurchSpread, 2));
                d.setBaPurchCreditCard(roundDoubleandFormat(setBaPurchCreditCard, 2));
                d.setBaPurchTransfNotes(roundDoubleandFormat(setBaPurchTransfNotes, 2));
                d.setBaPurchTransfOther(roundDoubleandFormat(setBaPurchTransfOther, 2));
                d.setBaSalesTotal(roundDoubleandFormat(setBaSalesTotal, 2));
                d.setBaSalesSpread(roundDoubleandFormat(setBaSalesSpread, 2));
                d.setBaSalesCreditCard(roundDoubleandFormat(setBaSalesCreditCard, 2));
                d.setBaSalesTransfNotes(roundDoubleandFormat(setBaSalesTransfNotes, 2));
                d.setBaSalesTransfOther(roundDoubleandFormat(setBaSalesTransfOther, 2));

                d.setBraPurchTotal(roundDoubleandFormat(setBraPurchTotal, 2));
                d.setBraPurchSpread(roundDoubleandFormat(setBraPurchSpread, 2));
                d.setBraPurchLocalCurr(roundDoubleandFormat(setBraPurchLocalCurr, 2));
                d.setBraSalesTotal(roundDoubleandFormat(setBraSalesTotal, 2));
                d.setBraSalesSpread(roundDoubleandFormat(setBraSalesSpread, 2));
                d.setBraSalesLocalCurr(roundDoubleandFormat(setBraSalesLocalCurr, 2));

                d.setRefund(roundDoubleandFormat(refundshow, 2));

                double setGroffTurnover = setPurchTotal + setSalesTotal + setCashAdNetTot;
                // double setGrossProfit = setPurchComm + setSalesComm + setCashAdComm +
                // setSalesSpread + setBaSalesSpread + setBraSalesSpread;
                double setGrossProfit = setPurchProfit + setCashAdProfit + setSalesProfit + setBaSalesSpread
                        + setBaPurchSpread;
                double setLastCashOnPrem = 0.0;
                double setFx = 0.0;
                ArrayList<Office_sp> li = list_query_officesp2(fil[0], subDays(datad1.substring(0, 10), patternsql, 1));
                if (!li.isEmpty()) {
                    Office_sp o = li.get(0);
                    d.setOfficesp(o.getCodice());
                    setLastCashOnPrem = fd(o.getTotal_cod());
                } else {
                    d.setOfficesp(null);
                }
                if (now) {
                    ArrayList<OfficeStockPrice_value> dati = list_OfficeStockPrice_value(fil[0]);
                    for (int x = 0; x < dati.size(); x++) {
                        OfficeStockPrice_value t = dati.get(x);
                        if (t.getCurrency().equals(valutalocale) && t.getSupporto().contains("01")) {
                        } else {
                            setFx = setFx + fd(t.getControvaloreSenzaFormattazione());
                        }
                    }
                } else {
                    Office_sp o = list_query_last_officesp(fil[0], datad2);
                    if (o != null) {

                        double[] d1 = list_dettagliotransazioni(fil, o.getData(), datad2, valutalocale);
                        setFx = fd(o.getTotal_fx()) + d1[1];
                    }
                }

                String oper = get_national_office().getChangetype();
                boolean dividi = oper.equals("/");

                double setCashOnPremFromTrans = setSalesTotal
                        - setPurchTotal
                        + setBaPurchTransfNotes
                        - setBaSalesTransfNotes
                        + setBraPurchLocalCurr
                        - setBraSalesLocalCurr
                        + totalnotesnochange
                        + setLastCashOnPrem
                        - refund
                        - setCashAdNetTot
                        - poamount;

                double setFxClosureErrorDeclared = 0.0;
                double setCashOnPremError = 0.0;
                String sql10 = "SELECT * FROM oc_errors where filiale = '" + fil[0]
                        + "' AND cod IN (SELECT cod FROM oc_lista where data like '" + datad1.substring(0, 10)
                        + "%' AND errors='Y') AND tipo='CH' AND (kind='01' OR kind='02' OR kind='03')";
                PreparedStatement ps10 = this.c.prepareStatement(sql10);
                ResultSet rs10 = ps10.executeQuery();
                while (rs10.next()) {
                    if (rs10.getString("valuta").equals(valutalocale) && rs10.getString("kind").equals("01")) {
                        // calcolare
                        double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                        setCashOnPremError = setCashOnPremError + eurerr;
                    } else {
                        double fxerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                        setFxClosureErrorDeclared = setFxClosureErrorDeclared
                                + getControvalore(fxerr, fd(rs10.getString("rate")), dividi);
                        // setFxClosureErrorDeclared = setFxClosureErrorDeclared +
                        // getControvalore(fd(rs10.getString("total_diff")), fd(rs10.getString("rate")),
                        // dividi);
                    }
                }
                ps10.close();

                double setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;
                d.setGroffTurnover(roundDoubleandFormat(setGroffTurnover, 2));
                d.setGrossProfit(roundDoubleandFormat(setGrossProfit, 2));
                d.setLastCashOnPrem(roundDoubleandFormat(setLastCashOnPrem, 2));
                d.setCashOnPrem(roundDoubleandFormat(setCashOnPrem, 2));
                d.setFx(roundDoubleandFormat(setFx, 2));
                d.setCashOnPremFromTrans(roundDoubleandFormat(setCashOnPremFromTrans, 2));

                if (setCashOnPremError == 0) {
                    d.setCashOnPremError(roundDoubleandFormat(setCashOnPremError, 2).replaceAll("-", ""));
                } else {
                    d.setCashOnPremError(roundDoubleandFormat(setCashOnPremError, 2));
                }

                d.setFxClosureErrorDeclared(roundDoubleandFormat(setFxClosureErrorDeclared, 2));
                d.setNoTransPurch(String.valueOf(setNoTransPurch));
                d.setNoTransCC(String.valueOf(setNoTransCC));
                d.setNoTransSales(String.valueOf(setNoTransSales));

                d.setTotal(String.valueOf(setNoTransPurch + setNoTransCC + setNoTransSales));

                d.setTotPos(String.valueOf(setTotPos));
                d.setTotAcc(String.valueOf(setTotAcc));
                d.setDatiBank(listdb);
                d.setId_filiale(fil[0]);
                d.setDe_filiale(fil[1]);
                d.setData(formatStringtoStringDate(datad1.substring(0, 10), patternsql, patternnormdate_filter));
                return d;
            } catch (SQLException | NumberFormatException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public ArrayList<String[]> credit_card_enabled() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT carta_credito,de_carta_credito,fg_annullato FROM carte_credito WHERE fg_annullato = ? AND filiale = ? order by carta_credito";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_bankAccount() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,de_bank,conto FROM bank where fg_annullato = ? AND bank_account = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, "Y");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), rs.getString(2), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_causal> query_nc_causal_filial(String filiale, String status) {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {

            String sql = "SELECT * FROM nc_causali WHERE filiale = '" + filiale + "' ";

            if (status == null || status.equals("...")) {

            } else {
                sql += " AND annullato = '" + status + "' ";
            }
            // sql += "ORDER BY gruppo_nc";

            PreparedStatement ps = this.c.prepareStatement(sql);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                NC_causal nc1 = new NC_causal();

                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setCausale_nc(rs.getString("causale_nc"));
                nc1.setDe_causale_nc(visualizzaStringaMySQL(rs.getString("de_causale_nc")));
                nc1.setFg_in_out(rs.getString("fg_in_out"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setNc_de(rs.getString("nc_de"));
                nc1.setFg_batch(rs.getString("fg_batch"));
                nc1.setFg_gruppo_stampa(rs.getString("fg_gruppo_stampa"));
                nc1.setFg_scontrino(rs.getString("fg_scontrino"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setData(rs.getString("data"));
                nc1.setBonus(rs.getString("bonus"));
                nc1.setCodice_integr(rs.getString("codice_integr"));
                nc1.setPaymat(rs.getString("paymat"));
                nc1.setDocric(rs.getString("docric"));

                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> nc_kind_order() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT codice,descrizione,et1,et2 FROM nc_kind WHERE attivo = ? order by codice";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "1");
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

    public ArrayList<OfficeStockPrice_value> list_OfficeStockPrice_value(String filiale) {
        ArrayList<OfficeStockPrice_value> out = new ArrayList<>();
        ArrayList<Currency> licur = list_figures_query_edit(filiale);
        ArrayList<Figures> lifg = list_all_figures();
        String valutalocale = get_local_currency()[0];
        try {
            String oper = get_national_office().getChangetype();
            boolean dividi = oper.equals("/");
            String sql = "SELECT cod_value,kind,total,controval FROM stock WHERE filiale = '" + filiale + "' "
                    + "AND tipostock = 'CH' AND (kind ='01' or kind ='02' or kind ='03') "
                    + "ORDER BY cod_value,kind,date";

            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ArrayList<String> listval = new ArrayList<>();
            List<Stock> content = new ArrayList<>();

            while (rs.next()) {
                content.add(new Stock(rs.getString("cod_value"), rs.getString("kind"), rs.getString("total"),
                        rs.getString("controval")));
                listval.add(rs.getString("cod_value"));
            }
            ps.close();
            removeDuplicatesAL(listval);

            for (int i = 0; i < listval.size(); i++) {
                String val = listval.get(i);

                OfficeStockPrice_value osp01 = new OfficeStockPrice_value();
                osp01.setCurrency(val);
                osp01.setDecurrency(formatALCurrency(val, licur));
                osp01.setSupporto("01 " + get_figures(lifg, "01").getDe_supporto());
                OfficeStockPrice_value osp02 = new OfficeStockPrice_value();
                osp02.setCurrency(val);
                osp02.setDecurrency(formatALCurrency(val, licur));
                osp02.setSupporto("02 " + get_figures(lifg, "02").getDe_supporto());

                OfficeStockPrice_value osp03 = new OfficeStockPrice_value();
                osp03.setCurrency(val);
                osp03.setDecurrency(formatALCurrency(val, licur));
                osp03.setSupporto("03 " + get_figures(lifg, "03").getDe_supporto());

                // double setQtaosp01 = 0.00;
                // double controv01 = 0.00;
                //
                // double setQtaosp02 = 0.00;
                // double controv02 = 0.00;
                //
                // double setQtaosp03 = 0.00;
                // double controv03 = 0.00;
                AtomicDouble ad_setQtaosp01 = new AtomicDouble(0.0);
                AtomicDouble ad_controv01 = new AtomicDouble(0.0);
                AtomicDouble ad_setQtaosp02 = new AtomicDouble(0.0);
                AtomicDouble ad_controv02 = new AtomicDouble(0.0);
                AtomicDouble ad_setQtaosp03 = new AtomicDouble(0.0);
                AtomicDouble ad_controv03 = new AtomicDouble(0.0);

                List<Stock> content_def = content.stream()
                        .filter(result2 -> result2.getCod_value().equalsIgnoreCase(val)).collect(Collectors.toList());
                content_def.forEach(c1 -> {
                    if (c1.getKind().equals("01")) {
                        ad_setQtaosp01.addAndGet(fd(c1.getTotal()));
                        if (c1.getCod_value().equalsIgnoreCase(valutalocale)) {
                            ad_controv01.addAndGet(fd(c1.getTotal()));
                        } else {
                            ad_controv01.addAndGet(fd(c1.getControval()));
                        }
                    } else if (c1.getKind().equals("02")) {
                        ad_setQtaosp02.addAndGet(fd(c1.getTotal()));
                        ad_controv02.addAndGet(fd(c1.getControval()));
                    } else if (c1.getKind().equals("03")) {
                        ad_setQtaosp03.addAndGet(fd(c1.getTotal()));
                        ad_controv03.addAndGet(fd(c1.getControval()));
                    }
                });

                // while (rs.next()) {
                // if (rs.getString("cod_value").equals(val)) {
                // if (rs.getString("kind").equals("01")) {
                // setQtaosp01 = setQtaosp01 + fd(rs.getString("total"));
                //
                // if (rs.getString("cod_value").equals(valutalocale)) {
                // controv01 = controv01 + fd(rs.getString("total"));
                // } else {
                // controv01 = controv01 + fd(rs.getString("controval"));
                // }
                // } else if (rs.getString("kind").equals("02")) {
                // setQtaosp02 = setQtaosp02 + fd(rs.getString("total"));
                // controv02 = controv02 + fd(rs.getString("controval"));
                // } else if (rs.getString("kind").equals("03")) {
                // setQtaosp03 = setQtaosp03 + fd(rs.getString("total"));
                // controv03 = controv03 + fd(rs.getString("controval"));
                // }
                // }
                // }
                double setQtaosp01 = Utility.roundDouble(ad_setQtaosp01.get(), 2);
                double controv01 = Utility.roundDouble(ad_controv01.get(), 2);

                double setQtaosp02 = Utility.roundDouble(ad_setQtaosp02.get(), 2);
                double controv02 = Utility.roundDouble(ad_controv02.get(), 2);

                double setQtaosp03 = Utility.roundDouble(ad_setQtaosp03.get(), 2);
                double controv03 = Utility.roundDouble(ad_controv03.get(), 2);

                if (setQtaosp01 > 0) {
                    osp01.setQta(roundDoubleandFormat(setQtaosp01, 2));
                    double mediarate = getControvaloreOFFSET(setQtaosp01, controv01, dividi);
                    osp01.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp01.setControvalore(roundDoubleandFormat(controv01, 2));
                    out.add(osp01);
                }
                if (setQtaosp02 > 0) {
                    osp02.setQta(roundDoubleandFormat(setQtaosp02, 2));
                    double mediarate = getControvaloreOFFSET(setQtaosp02, controv02, dividi);
                    osp02.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp02.setControvalore(roundDoubleandFormat(controv02, 2));
                    out.add(osp02);
                }
                if (setQtaosp03 > 0) {
                    osp03.setQta(roundDoubleandFormat(setQtaosp03, 2));
                    double mediarate = getControvaloreOFFSET(setQtaosp03, controv03, dividi);
                    osp03.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp03.setControvalore(roundDoubleandFormat(controv03, 2));
                    out.add(osp03);
                }

                // rs.beforeFirst();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Office_sp> list_query_officesp2(String filiale, String data) {
        ArrayList<Office_sp> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM office_sp where filiale = '" + filiale + "' AND data <= '" + data
                    + " 23:59:59' ORDER BY data DESC";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                out.add(new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10)));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public Office_sp list_query_last_officesp(String filiale, String data) {
        try {
            String sql = "SELECT * FROM office_sp where filiale = '" + filiale
                    + "' AND data < '" + data + "' ORDER BY data DESC LIMIT 1";

            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public double[] list_dettagliotransazioni(String[] fil, String datad1, String datad2, String valutalocale) {
        double[] d = new double[2];
        double lo = 0.00;
        double fx = 0.00;
        try {
            String sql0 = "SELECT * FROM ch_transaction_refund WHERE status = '1' AND method = 'BR' AND branch_cod = ? AND dt_refund >= ? AND dt_refund <= ?";
            PreparedStatement ps0 = this.c.prepareStatement(sql0);
            ps0.setString(1, fil[0]);
            ps0.setString(2, datad1);
            ps0.setString(3, datad2);
            ResultSet rs0 = ps0.executeQuery();
            while (rs0.next()) {
                lo = lo - fd(rs0.getString("value"));
            }
            ps0.close();

            String sql = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg = '0' AND tr1.filiale = ? AND tr1.data >= ? AND tr1.data <= ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, fil[0]);
            ps.setString(2, datad1);
            ps.setString(3, datad2);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.getString("tr1.tipotr").equals("B")) {
                    PreparedStatement psval = this.c
                            .prepareStatement("SELECT * FROM ch_transaction_valori WHERE cod_tr = ?");
                    psval.setString(1, rs.getString("cod"));
                    ResultSet rsval = psval.executeQuery();
                    while (rsval.next()) {
                        if (rsval.getString("supporto").equals("01") || rsval.getString("supporto").equals("02")
                                || rsval.getString("supporto").equals("03")) {
                            if (rsval.getString("supporto").equals("01")
                                    && rsval.getString("valuta").equals(valutalocale)) {
                                lo = lo + fd(rsval.getString("total"));
                            } else {
                                fx = fx + fd(rsval.getString("total"));
                            }
                        }
                        lo = lo - fd(rsval.getString("net"));
                    }
                    psval.close();
                } else {
                    PreparedStatement psval = this.c
                            .prepareStatement("SELECT * FROM ch_transaction_valori WHERE cod_tr = ?");
                    psval.setString(1, rs.getString("cod"));
                    ResultSet rsval = psval.executeQuery();
                    while (rsval.next()) {
                        if (rsval.getString("valuta").equals(valutalocale)) {
                            lo = lo - fd(rsval.getString("total"));
                        } else {
                            fx = fx - fd(rsval.getString("total"));
                        }
                    }
                    if (rs.getString("tr1.localfigures").equals("01")) {
                        lo = lo + fd(rs.getString("pay"));
                    }
                    psval.close();
                }
            }
            ps.close();

            // NO CHANGE
            String sql1 = "SELECT supporto,fg_inout,total FROM nc_transaction WHERE del_fg = '0' AND filiale = ? AND data >= ? AND data <= ?";
            PreparedStatement ps1 = this.c.prepareStatement(sql1);
            ps1.setString(1, fil[0]);
            ps1.setString(2, datad1);
            ps1.setString(3, datad2);
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()) {
                String supporto = rs1.getString("supporto");
                if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {
                    if (supporto.equals("01")) {
                        lo = lo + fd(rs1.getString("total"));
                    }
                } else if (supporto.equals("01")) {
                    lo = lo - fd(rs1.getString("total"));
                }
            }
            ps1.close();

            // EXTERNAL TRANSFER
            String sql2 = "SELECT * FROM et_change WHERE fg_annullato = '0' AND filiale = ? AND dt_it >= ? AND dt_it <= ?";
            PreparedStatement ps2 = this.c.prepareStatement(sql2);
            ps2.setString(1, fil[0]);
            ps2.setString(2, datad1);
            ps2.setString(3, datad2);
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                PreparedStatement ps2val = this.c.prepareStatement("SELECT * FROM et_change_valori WHERE cod = ?");
                ps2val.setString(1, rs2.getString("cod"));
                ResultSet rs2val = ps2val.executeQuery();
                if (rs2.getString("fg_tofrom").equals("T")) { // sales
                    while (rs2val.next()) {
                        if (rs2val.getString("kind").equals("01")) {
                            if (rs2val.getString("currency").equals(valutalocale)) {
                                lo = lo - fd(rs2val.getString("ip_total"));
                            } else {
                                fx = fx - fd(rs2val.getString("ip_total"));
                            }
                        } else {
                            fx = fx - fd(rs2val.getString("ip_total"));
                        }
                        ps2val.close();
                    }
                    ps2.close();
                } else { // buy
                    while (rs2val.next()) {
                        if (rs2val.getString("kind").equals("01")) {
                            if (rs2val.getString("currency").equals(valutalocale)) {
                                lo = lo + fd(rs2val.getString("ip_total"));
                            } else {
                                fx = fx + fd(rs2val.getString("ip_total"));
                            }
                        } else {
                            fx = fx + fd(rs2val.getString("ip_total"));
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        d[0] = lo;
        d[1] = fx;
        return d;
    }

    public Office get_national_office() {
        try {
            String sql = "SELECT * FROM office ";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                Office of = new Office();
                of.setCod(rs.getString("cod"));
                of.setDe_office(rs.getString("de_office"));
                of.setAdd_city(rs.getString("add_city"));
                of.setAdd_cap(rs.getString("add_cap"));
                of.setAdd_via(rs.getString("add_via"));
                of.setVat(rs.getString("vat"));
                of.setReg_impr(rs.getString("reg_impr"));
                of.setRea(rs.getString("rea"));
                of.setChangetype(rs.getString("changetype"));
                of.setDecimalround(rs.getString("decimalround"));
                of.setUrl_bl(rs.getString("url_bl"));
                of.setTxt_ricev_1(rs.getString("txt_ricev_1"));
                of.setTxt_ricev_2(rs.getString("txt_ricev_2"));
                of.setTxt_alert_threshold_1(rs.getString("txt_alert_threshold_1"));
                of.setTxt_alert_threshold_2(rs.getString("txt_alert_threshold_2"));
                of.setTxt_ricev_bb(rs.getString("txt_ricev_bb"));
                of.setScadenza_bb(rs.getString("scadenza_bb"));
                of.setShowagency(rs.getString("showagency"));
                of.setMinutes(rs.getString("minutes"));
                of.setKyc_mesi(rs.getString("kyc_mesi"));
                of.setKyc_soglia(rs.getString("kyc_soglia"));
                of.setRisk_days(rs.getString("risk_days"));
                of.setRisk_ntr(rs.getString("risk_ntr"));
                of.setRisk_soglia(rs.getString("risk_soglia"));
                of.setTxt_nopep(rs.getString("txt_nopep"));
                return of;
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<Currency> list_figures_query_edit(String filial) {
        ArrayList<Currency> out = new ArrayList<>();
        try {
            String sql;
            PreparedStatement ps;
            if (filial == null) {
                sql = "SELECT filiale,valuta,de_valuta,cambio_bce,enable_buy,enable_sell,buy_std_type,"
                        + "buy_std,buy_std_value,sell_std_type,sell_std,sell_std_value FROM valute GROUP BY valuta ORDER BY valuta";
                ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            } else {
                sql = "SELECT filiale,valuta,de_valuta,cambio_bce,enable_buy,enable_sell,buy_std_type,"
                        + "buy_std,buy_std_value,sell_std_type,sell_std,sell_std_value FROM valute WHERE filiale = ? ORDER BY valuta";
                ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, filial);
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Currency cu = new Currency();
                cu.setCode(rs.getString("valuta"));
                cu.setDescrizione((rs.getString("de_valuta")));
                cu.setFilial(rs.getString("filiale"));
                cu.setCambio_bce(rs.getString("cambio_bce"));
                cu.setEnable_buy(rs.getString("enable_buy"));
                cu.setEnable_sell(rs.getString("enable_sell"));
                cu.setBuy_std_type(rs.getString("buy_std_type"));
                cu.setSell_std_type(rs.getString("sell_std_type"));
                double bce = fd(cu.getCambio_bce());
                if (cu.getEnable_buy().equals("1")) {
                    if (cu.getBuy_std_type().equals("0")) {
                        double standard = fd(rs.getString("buy_std"));
                        double buy_st = bce * (100.0D + standard) / 100.0D;
                        cu.setEnable_buy(formatMysqltoDisplay(roundDoubleandFormat(buy_st, 8)));
                    } else {
                        cu.setEnable_buy(rs.getString("buy_std_value"));
                    }
                } else {
                    cu.setEnable_buy("<div class='font-red'>Disabled <i class='fa fa-close'></i></div>");
                }
                if (cu.getEnable_sell().equals("1")) {
                    if (cu.getSell_std_type().equals("0")) {
                        double standard = fd(rs.getString("sell_std"));
                        double buy_st = bce * (100.0D + standard) / 100.0D;
                        cu.setEnable_sell(formatMysqltoDisplay(roundDoubleandFormat(buy_st, 8)));
                    } else {
                        cu.setEnable_sell(rs.getString("sell_std_value"));
                    }
                } else {
                    cu.setEnable_sell("<div class='font-red'>Disabled <i class='fa fa-close'></i></div>");
                }
                out.add(cu);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_branch_group() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branchgroup ORDER BY descrizione";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] ar = { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8) };
                out.add(ar);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public String get_BCE_USD(String giorno) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql = "SELECT modify FROM rate_history where filiale = '000' AND valuta = 'USD' "
                    + "AND modify like '%bce value%' AND dt_mod < '" + giorno + " 23:59:59' order by dt_mod DESC";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (giorno.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(this.gf, bce);
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

    public ArrayList<NC_transaction> query_NC_transaction_enable(String d1, String d2, String filiale) {
        ArrayList<NC_transaction> out = new ArrayList<>();
        try {
            if (d1 == null || d1.equals("...")) {
                d1 = "";
            }
            d1 = d1.trim() + " 00:00:00";
            if (d2 == null || d2.equals("...")) {
                d2 = "";
            }
            d2 = d2.trim() + " 23:59:59";
            d1 = Utility.formatStringtoStringDate(d1, patternnormdate, patternsqldate);
            d2 = Utility.formatStringtoStringDate(d2, patternnormdate, patternsqldate);
            String sql = "SELECT data,quantita,total,gruppo_nc FROM nc_transaction WHERE data >= ? AND data <= ? AND filiale = ? AND del_fg = ? AND fg_tipo_transazione_nc <> '1' AND total NOT LIKE '-%' ";
            // String sql = "SELECT * FROM nc_transaction WHERE data >= ? AND data <= ? AND
            // filiale = ? AND del_fg = ?";

            // !nc.getFg_tipo
            // && !nc.getTotal().contains("-")_transazione_nc().equals("1")
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, d1);
            ps.setString(2, d2);
            ps.setString(3, filiale);
            ps.setString(4, "0");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_transaction nc = new NC_transaction(this.gf);
                nc.setData(rs.getString(1));
                nc.setQuantita(rs.getString(2));
                nc.setTotal(rs.getString(3));
                nc.setGruppo_nc(rs.getString(4));
                nc.setFiliale(filiale);
                out.add(nc);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String> list_branch_RM() {
        ArrayList<String> list = new ArrayList<>();
        try {
            String sql = "SELECT * FROM branch WHERE brgr_06 IN  (SELECT cod FROM branchgroup WHERE type ='06' AND descrizione LIKE 'ROMA%') AND fg_annullato = '0'";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getString(1).replaceAll("'", "''"));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return list;

    }

    public ArrayList<String[]> undermincommjustify() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT id_under_min_comm_justify,de_under_min_comm_justify,fg_blocco FROM under_min_comm_justify ORDER BY de_under_min_comm_justify";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> kindcommissionefissa() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT id_kind_commissione_fissa,de_kind_commissione_fissa,ip_kind_commissione_fissa,fg_blocco FROM kind_commissione_fissa WHERE filiale = ? ORDER BY de_kind_commissione_fissa";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = {
                        StringUtils.leftPad(rs.getString(1), 3, "0"),
                        visualizzaStringaMySQL(rs.getString(2)), rs.getString(3), rs.getString(4) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_bank_pos_enabled() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            if (this.gf.isIs_IT()) {
                String sql = "SELECT cod,de_bank,conto,bank_account FROM bank WHERE fg_annullato = ? AND (bank_account = ? OR cod in (select distinct(carta_credito) "
                        + "from carte_credito)) ORDER BY de_bank";
                PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, "0");
                ps.setString(2, "Y");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                            rs.getString(4) };
                    out.add(o1);
                }
                ps.close();
            } else {
                String sql = "SELECT carta_credito,de_carta_credito FROM carte_credito WHERE fg_annullato = ? AND filiale = ? ORDER BY de_carta_credito";
                PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, "0");
                ps.setString(2, "000");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), "000000", "Y" };
                    out.add(o1);
                }
                ps.close();
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_bank() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,de_bank,conto,da_annull,fg_annullato,bank_account FROM bank ORDER BY de_bank";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> history_BB() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM bb_story";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] s1 = { rs.getString(2), rs.getString(3), rs.getString(4) };
                out.add(s1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_category> list_nc_category() {
        ArrayList<NC_category> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_tipologia WHERE filiale = ? order by gruppo_nc";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "000");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_category nc1 = new NC_category();
                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setDe_gruppo_nc(visualizzaStringaMySQL(rs.getString("de_gruppo_nc")));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setConto_coge_01(rs.getString("conto_coge_01"));
                nc1.setConto_coge_02(rs.getString("conto_coge_02"));
                nc1.setDe_scontrino(visualizzaStringaMySQL(rs.getString("de_scontrino")));
                nc1.setDe_riga(visualizzaStringaMySQL(rs.getString("de_riga")));
                nc1.setPc_iva_corrispettivo(rs.getString("pc_iva_corrispettivo"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_enabled(rs.getString("ticket_enabled"));
                nc1.setTimestamp(rs.getString("data"));
                nc1.setInt_code(rs.getString("int_code"));
                nc1.setInt_corrisp(rs.getString("int_corrisp"));
                nc1.setInt_iva(rs.getString("int_iva"));
                nc1.setFg_registratore(rs.getString("fg_registratore"));
                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<NC_causal> list_nc_causal_freetax() {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_causali WHERE fg_tipo_transazione_nc = ? order by causale_nc";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "3");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_causal nc1 = new NC_causal();
                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setCausale_nc(rs.getString("causale_nc"));
                nc1.setDe_causale_nc(rs.getString("de_causale_nc"));
                nc1.setFg_in_out(rs.getString("fg_in_out"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setNc_de(rs.getString("nc_de"));
                nc1.setFg_batch(rs.getString("fg_batch"));
                nc1.setFg_gruppo_stampa(rs.getString("fg_gruppo_stampa"));
                nc1.setFg_scontrino(rs.getString("fg_scontrino"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setData(rs.getString("data"));
                nc1.setBonus(rs.getString("bonus"));
                nc1.setCodice_integr(rs.getString("codice_integr"));
                nc1.setPaymat(rs.getString("paymat"));
                nc1.setDocric(rs.getString("docric"));
                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        Collections.sort(out);
        return out;
    }

    public String[] internetbooking_ch(String cod_tr) {
        try {
            String sql = "SELECT canale, codiceprenotazione FROM internetbooking_ch WHERE cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, cod_tr);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String[] out = { rs.getString(1), rs.getString(2) };
                return out;
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Codici_sblocco getCod_tr(String cod_tr, String tipoutil) {
        try {
            String sql = "SELECT * FROM codici_sblocco WHERE cod_tr = ? AND ty_util = ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, cod_tr);
            ps.setString(2, tipoutil);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Codici_sblocco(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6),
                        rs.getString(7));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<String[]> unlockratejustify() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT id_unlockrate_justify,de_unlockrate_justify,fg_blocco FROM unlockrate_justify ORDER BY de_unlockrate_justify";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<String[]> list_internetbooking() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT internet_booking,de_internet_booking FROM internetbooking order by internet_booking";
            PreparedStatement ps = this.c.prepareStatement(sql, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { leftPad(rs.getString(1), 2, "0"), visualizzaStringaMySQL(rs.getString(2)) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public List<Booking> getDetailBooking() {

        List<Booking> out = new ArrayList<>();

        try {
            String sql = "SELECT cod,cod_tr,pan,canale FROM sito_prenotazioni WHERE cod_tr <>'-'";
            // String sql = "SELECT cod,cod_tr,pan FROM sito_prenotazioni WHERE cod_tr <>'-'
            // AND pan LIKE '%$%'";
            try (PreparedStatement ps = this.c.prepareStatement(sql, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE);
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Booking b = new Booking();
                    b.setCod(rs.getString(1));
                    b.setCod_tr(rs.getString(2));
                    b.setPan(rs.getString(3));
                    b.setCanale(rs.getString(4));
                    out.add(b);
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<DailyChange_CG> list_DailyChange_CG(Branch b1, String datad1, String datad2, boolean deleted) {
        ArrayList<DailyChange_CG> out = new ArrayList<>();
        try {
            DateTime inizionuovospreadCZ = new DateTime(2021, 10, 1, 0, 0).withMillisOfDay(0);
            ArrayList<Figures> fig = list_all_figures();
            ArrayList<String[]> list_group = list_branch_group();
            ArrayList<Users> list_users = list_all_users();
            ArrayList<String[]> nazioni = country();
            ArrayList<String[]> array_undermincommjustify = undermincommjustify();
            ArrayList<String[]> array_kindcommissionefissa = kindcommissionefissa();
            ArrayList<String[]> array_unlockrate = unlockratejustify();
            ArrayList<String[]> bank = list_bank_pos_enabled();
            ArrayList<String[]> bank2 = list_bank();
            ArrayList<String[]> history_BB = history_BB();
            ArrayList<NC_category> listcat = list_nc_category();
            ArrayList<NC_causal> listcaus = list_nc_causal_freetax();
            ArrayList<String[]> ib = list_internetbooking();
            List<Booking> listbook = getDetailBooking();
            double cb_comm = fd(getConf("chebanca.commission"));

            String sqlet = "SELECT * FROM et_change e, et_change_valori ev WHERE ev.cod=e.cod and e.filiale='"
                    + b1.getCod() + "' ";
            if (datad1 != null) {
                sqlet = sqlet + "AND e.dt_it >= '" + datad1 + " 00:00:00' ";
            }
            if (datad2 != null) {
                sqlet = sqlet + "AND e.dt_it <= '" + datad2 + " 23:59:59' ";
            }

            if (!deleted) {
                sqlet = sqlet + "AND e.fg_annullato = '0' ";
            }

            sqlet = sqlet + " ORDER BY e.dt_it";

            PreparedStatement pset = this.c.prepareStatement(sqlet);

            ResultSet rset = pset.executeQuery();

            while (rset.next()) {

                DailyChange_CG d1 = new DailyChange_CG();
                DateTime dt_it = getDT(rset.getString("e.dt_it"), patternsqldate);
                Users g = get_user(rset.getString("e.user"), list_users);
                Figures f = get_figures(fig, rset.getString("ev.kind"));
                d1.setCDC(b1.getCod());
                d1.setSPORTELLO(b1.getDe_branch().toUpperCase());
                d1.setID(rset.getString("e.id"));
                if (rset.getString("e.fg_annullato").equals("1")) {
                    d1.setDELETE("SI");
                } else {
                    d1.setDELETE("");
                }

                d1.setAREA(Utility.formatAL(b1.getBrgr_01(), list_group, 1));
                d1.setCITTA(Utility.formatAL(b1.getBrgr_04(), list_group, 1));
                d1.setUBICAZIONE(Utility.formatAL(b1.getBrgr_02(), list_group, 1));
                d1.setGRUPPO(Utility.formatAL(b1.getBrgr_03(), list_group, 1));
                d1.setDATA(dt_it.toString(patternnormdate_filter));
                d1.setORA(dt_it.toString(patternhours_d));
                d1.setMESE(dt_it.monthOfYear().getAsText(Locale.ITALY).toUpperCase());
                d1.setANNO(dt_it.year().getAsText());
                d1.setCODUSER(g.getCod());
                d1.setUSERNOME(g.getDe_nome().toUpperCase());
                d1.setUSERCOGNOME(g.getDe_cognome().toUpperCase());
                d1.setMETODOPAGAMENTO("");
                d1.setRESIDENZACLIENTE("");
                d1.setNAZIONALITACLIENTE("");
                d1.setCOMMENTI(rset.getString("e.note"));
                d1.setACQUISTOVENDITA(format_tofrom_brba_new(rset.getString("e.fg_tofrom"),
                        rset.getString("e.fg_brba"), rset.getString("e.cod_dest"),
                        bank, bank2));
                d1.setTIPOLOGIAACQOVEND(f.getDe_supporto());
                d1.setVALUTA(rset.getString("ev.currency"));
                d1.setQUANTITA((rset.getString("ev.ip_quantity")));
                d1.setTASSODICAMBIO((rset.getString("ev.ip_rate")));
                d1.setCONTROVALORE((rset.getString("ev.ip_total")));
                d1.setCOMMVARIABILE("");
                d1.setCOMMFISSA("");

                String SPREADBANK = "";

                if (rset.getString("e.fg_brba").equals("BR")) {
                    // SPREADBRANCH = (rset.getString("ev.ip_spread"));
                } else {
                    SPREADBANK = (rset.getString("ev.ip_spread"));
                }

                // if (Constant.is_IT) {
                d1.setSPREADBRANCH("");
                // } else {
                // if (dt_it.isBefore(inizionuovospreadCZ)) {
                // d1.setSPREADBRANCH(SPREADBRANCH);
                // } else {
                // d1.setSPREADBRANCH("");
                // }
                // }

                d1.setSPREADBANK(SPREADBANK);

                d1.setSPREADVEND("");

                double totgm = fd(rset.getString("ev.ip_spread"));

                if (rset.getString("e.fg_brba").equals("BR")) {

                    if (gf.isIs_IT()) {
                        d1.setTOTGM("0.00");
                    } else {
                        if (dt_it.isBefore(inizionuovospreadCZ)) {
                            d1.setTOTGM((roundDoubleandFormat(totgm, 2)));
                        } else {
                            d1.setTOTGM("0.00");
                        }
                    }
                } else {
                    d1.setTOTGM((roundDoubleandFormat(totgm, 2)));
                }

                d1.setPERCCOMM("");
                d1.setPERCSPREADVENDITA("");
                d1.setVENDITABUYBACK("");
                d1.setVENDITASELLBACK("");
                d1.setCODICEINTERNETBOOKING("");
                d1.setMOTIVOPERRIDUZIONEDELLACOMM("");
                d1.setMOTIVOPERRIDUZIONEDELLACOMMFISSA("");

                d1.setCODICESBLOCCO("");
                d1.setAGCODE("");
                d1.setAGNUMBER("");
                d1.setCODICEINTERNETBOOKING("");
                d1.setIBCHAN("");
                d1.setCBCOMM("");
                out.add(d1);

            }
            pset.close();

            String sql = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2 WHERE tr1.cod=tr2.cod_tr AND tr1.filiale = '"
                    + b1.getCod() + "' ";
            if (datad1 != null) {
                sql = sql + "AND tr1.data >= '" + datad1 + " 00:00:00' ";
            }
            if (datad2 != null) {
                sql = sql + "AND tr1.data <= '" + datad2 + " 23:59:59' ";
            }

            if (!deleted) {
                sql = sql + "AND tr1.del_fg <= '0' ";
            }

            sql = sql + " ORDER BY tr1.data";

            PreparedStatement ps = this.c.prepareStatement(sql);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                DailyChange_CG d1 = new DailyChange_CG();

                DateTime dt_tr = getDT(rs.getString("tr1.data"), patternsqldate);
                Users g = get_user(rs.getString("tr1.user"), list_users);
                Figures f = get_figures(fig, rs.getString("tr2.supporto"));
                Figures p = get_figures(fig, rs.getString("tr1.localfigures"));

                d1.setCDC(b1.getCod());
                d1.setSPORTELLO(b1.getDe_branch().toUpperCase());
                d1.setID(rs.getString("tr1.id"));
                if (rs.getString("tr1.del_fg").equals("1")) {
                    d1.setDELETE("SI");
                } else {
                    d1.setDELETE("");
                }

                d1.setAREA(Utility.formatAL(b1.getBrgr_01(), list_group, 1));
                d1.setCITTA(Utility.formatAL(b1.getBrgr_04(), list_group, 1));
                d1.setUBICAZIONE(Utility.formatAL(b1.getBrgr_02(), list_group, 1));
                d1.setGRUPPO(Utility.formatAL(b1.getBrgr_03(), list_group, 1));
                d1.setDATA(dt_tr.toString(patternnormdate_filter));
                d1.setORA(dt_tr.toString(patternhours_d));
                d1.setMESE(dt_tr.monthOfYear().getAsText(Locale.ITALY).toUpperCase());
                d1.setANNO(dt_tr.year().getAsText());
                d1.setCODUSER(g.getCod());
                d1.setUSERNOME(g.getDe_nome().toUpperCase());
                d1.setUSERCOGNOME(g.getDe_cognome().toUpperCase());

                if (rs.getString("tr1.tipotr").equals("B")) {
                    d1.setMETODOPAGAMENTO(f.getDe_supporto());
                } else {
                    d1.setMETODOPAGAMENTO(p.getDe_supporto());
                }

                Client c0 = query_Client_transaction(rs.getString("tr1.cod"), rs.getString("tr1.cl_cod"));
                d1.setRESIDENZACLIENTE(Utility.formatALN(c0.getNazione(), nazioni, 1));
                d1.setNAZIONALITACLIENTE(Utility.formatALN(c0.getNazione_nascita(), nazioni, 1));

                d1.setCOMMENTI(rs.getString("tr1.note"));

                d1.setACQUISTOVENDITA(
                        formatType_new(rs.getString("tr1.tipotr"),
                                rs.getString("tr1.intbook"),
                                rs.getString("tr1.intbook_type"),
                                rs.getString("tr1.intbook_1_tf"),
                                rs.getString("tr1.intbook_2_tf"),
                                rs.getString("tr1.intbook_3_tf"), listcat, listcaus));

                d1.setTIPOLOGIAACQOVEND(f.getDe_supporto());

                d1.setVALUTA(rs.getString("tr2.valuta"));
                d1.setQUANTITA((rs.getString("tr2.quantita")));
                d1.setTASSODICAMBIO((rs.getString("tr2.rate")));
                d1.setCONTROVALORE((rs.getString("tr2.total")));
                d1.setCOMMVARIABILE((rs.getString("tr2.com_perc_tot")));
                d1.setCOMMFISSA((rs.getString("tr2.fx_com")));

                String SPREADBRANCH = "", SPREADBANK = "";
                d1.setSPREADBRANCH(SPREADBRANCH);
                d1.setSPREADBANK(SPREADBANK);
                d1.setSPREADVEND((rs.getString("tr2.spread")));

                double totgm = fd(rs.getString("tr2.spread"))
                        + fd(rs.getString("tr2.tot_com"))
                        + parseDoubleR(gf, rs.getString("tr2.roundvalue"));

                if (gf.isIs_CZ()) {
                    double rv = parseDoubleR_CZ(gf, rs.getString("tr2.roundvalue"),
                            rs.getString("tr1.tipotr").equals("B"));
                    totgm = fd(rs.getString("tr2.spread"))
                            + fd(rs.getString("tr2.tot_com"))
                            + rv;
                }

                d1.setTOTGM((roundDoubleandFormat(totgm, 2)));

                d1.setPERCCOMM(rs.getString("tr2.com_perc"));

                double perc_spread = 0.0;
                if (totgm != 0.0) {
                    perc_spread = fd(rs.getString("tr2.spread")) * 100.0 / totgm;
                }

                d1.setPERCSPREADVENDITA((roundDoubleandFormat(perc_spread, 2)));

                String bb_status = "N";
                String sb_status = "N";

                if (rs.getString("tr1.tipotr").equals("S")) {

                    if (rs.getString("tr1.bb").equals("1") || rs.getString("tr1.bb").equals("2")) {
                        if (rs.getString("tr2.bb").equals("Y") || rs.getString("tr2.bb").equals("F")) {
                            bb_status = rs.getString("tr2.bb");
                        }
                    }
                    if (rs.getString("tr1.bb").equals("3") || rs.getString("tr1.bb").equals("4")) {
                        if (rs.getString("tr2.bb").equals("Y") || rs.getString("tr2.bb").equals("F")) {
                            sb_status = rs.getString("tr2.bb");
                        }
                    }

                    if (bb_status.equals("Y")) {
                        d1.setVENDITABUYBACK(get_Value_history_BB(history_BB, dt_tr, f));
                    } else if (bb_status.equals("F")) {
                        d1.setVENDITABUYBACK("FREE");
                    } else {
                        d1.setVENDITABUYBACK("");
                    }
                    d1.setVENDITASELLBACK("");
                } else {

                    if (rs.getString("tr1.bb").equals("1")) {
                        if (rs.getString("tr2.bb").equals("Y") || rs.getString("tr2.bb").equals("F")) {
                            bb_status = rs.getString("tr2.bb");
                        }
                    }
                    if (rs.getString("tr1.bb").equals("3") || rs.getString("tr1.bb").equals("4")) {
                        if (rs.getString("tr2.bb").equals("Y") || rs.getString("tr2.bb").equals("F")) {
                            sb_status = rs.getString("tr2.bb");
                        }
                    }

                    if (sb_status.equals("Y")) {
                        d1.setVENDITASELLBACK(get_Value_history_BB(history_BB, dt_tr, f));
                    } else if (sb_status.equals("F")) {
                        d1.setVENDITASELLBACK("FREE");
                    } else {
                        d1.setVENDITASELLBACK("");
                    }
                    d1.setVENDITABUYBACK("");
                }

                d1.setAGCODE("");
                d1.setAGNUMBER("");
                d1.setCODICEINTERNETBOOKING("");
                d1.setIBCHAN("");
                d1.setCBCOMM("");

                Optional<String> intBook = Optional.ofNullable(rs.getString("tr1.intbook"));
                if (intBook.isPresent() && intBook.get().equals("1")) {
                    try {
                        String cotr = rs.getString("tr1.cod");
                        Optional<Booking> optionalBooking = listbook.stream()
                                .filter(b00 -> b00.getCod_tr().equals(cotr)).findAny();
                        if (optionalBooking.isPresent()) {
                            Booking b0 = optionalBooking.get();
                            d1.setCODICEINTERNETBOOKING(b0.getCod());
                            d1.setIBCHAN(formatAL(b0.getCanale(), ib, 1));
                            if (b0.getCanale().contains("4")) {
                                double cb_sp_com = fd(d1.getSPREADVEND()) * cb_comm;
                                d1.setCBCOMM(String.valueOf(roundDouble(cb_sp_com, 2)));
                            }
                            if (b0.getPan().contains("$")) {
                                List<String> lb11 = Splitter.on("$").splitToList(b0.getPan());
                                if (lb11.size() == 2) {
                                    d1.setAGCODE(lb11.get(0));
                                    d1.setAGNUMBER(lb11.get(1));
                                }
                            }
                        } else {
                            String[] ib1 = internetbooking_ch(rs.getString("tr1.cod"));
                            if (ib1 != null) {
                                d1.setIBCHAN(formatAL(ib1[0], ib, 1));
                                if (ib1[0].contains("4")) {
                                    double cb_sp_com = fd(d1.getSPREADVEND()) * cb_comm;
                                    d1.setCBCOMM(String.valueOf(roundDouble(cb_sp_com, 2)));
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    d1.setMOTIVOPERRIDUZIONEDELLACOMM("Internet Booking");
                    d1.setMOTIVOPERRIDUZIONEDELLACOMMFISSA("Internet Booking");
                } else {
                    // Gestire il caso in cui tr1.intbook non è presente o non è uguale a "1"

                    d1.setMOTIVOPERRIDUZIONEDELLACOMM(
                            formatAL(rs.getString("tr2.low_com_ju"), array_undermincommjustify, 1));
                    d1.setMOTIVOPERRIDUZIONEDELLACOMMFISSA(
                            formatAL(rs.getString("tr2.kind_fix_comm"), array_kindcommissionefissa, 1));
                }

                Codici_sblocco cs1 = getCod_tr(rs.getString("tr1.cod"), "01");
                String cs = "";
                if (cs1 != null) {
                    cs = cs1.getCodice();
                    if (cs1.getCodice().contains("###")) {
                        cs = Utility
                                .formatAL(Splitter.on("###").splitToList(cs1.getCodice()).get(1), array_unlockrate, 1)
                                .toUpperCase();
                    }
                }
                d1.setCODICESBLOCCO(cs);

                // String loy = Engine.query_LOY_transaction(rs.getString("tr1.cod"), null,
                // "000");
                String loy = query_LOY_transaction(rs.getString("tr1.cod"));

                if (loy == null) {
                    loy = "";
                }
                d1.setLOYALTYCODE(loy);

                out.add(d1);

            }
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    // public ArrayList<DailyChange_CG> list_DailyChange_CG_OLD(Branch b1, String
    // datad1, String datad2, boolean deleted) {
    // ArrayList<DailyChange_CG> out = new ArrayList<>();
    // try {
    //
    // ArrayList<Figures> fig = list_all_figures();
    // ArrayList<String[]> list_group = list_branch_group();
    // ArrayList<Users> list_users = list_all_users();
    // ArrayList<String[]> nazioni = country();
    // ArrayList<String[]> array_undermincommjustify = undermincommjustify();
    // ArrayList<String[]> array_kindcommissionefissa = kindcommissionefissa();
    // ArrayList<String[]> bank = list_bank_pos_enabled();
    // ArrayList<String[]> bank2 = list_bank();
    // ArrayList<String[]> history_BB = history_BB();
    // ArrayList<NC_category> listcat = list_nc_category_enabled();
    // ArrayList<NC_causal> listcaus = list_nc_causal_enabled_freetax();
    //
    // String sqlet = "SELECT * FROM et_change e, et_change_valori ev WHERE
    // ev.cod=e.cod and e.filiale='" + b1.getCod() + "' ";
    // if (datad1 != null) {
    // sqlet = sqlet + "AND e.dt_it >= '" + datad1 + " 00:00:00' ";
    // }
    // if (datad2 != null) {
    // sqlet = sqlet + "AND e.dt_it <= '" + datad2 + " 23:59:59' ";
    // }
    //
    // if (!deleted) {
    // sqlet = sqlet + "AND e.fg_annullato = '0' ";
    // }
    //
    // sqlet = sqlet + " ORDER BY e.dt_it";
    //
    // ResultSet rset = this.c.createStatement().executeQuery(sqlet);
    //
    // while (rset.next()) {
    //
    // DailyChange_CG d1 = new DailyChange_CG();
    // DateTime dt_it = getDT(rset.getString("e.dt_it"), patternsqldate);
    // Users g = get_user(rset.getString("e.user"), list_users);
    // Figures f = get_figures(fig, rset.getString("ev.kind"));
    // d1.setCDC(b1.getCod());
    // d1.setSPORTELLO(b1.getDe_branch().toUpperCase());
    // d1.setID(rset.getString("e.id"));
    // if (rset.getString("e.fg_annullato").equals("1")) {
    // d1.setDELETE("SI");
    // } else {
    // d1.setDELETE("");
    // }
    //
    // d1.setAREA(Utility.formatAL(b1.getBrgr_01(), list_group, 1));
    // d1.setCITTA(Utility.formatAL(b1.getBrgr_04(), list_group, 1));
    // d1.setUBICAZIONE(Utility.formatAL(b1.getBrgr_02(), list_group, 1));
    // d1.setGRUPPO(Utility.formatAL(b1.getBrgr_03(), list_group, 1));
    // d1.setDATA(dt_it.toString(patternnormdate_filter));
    // d1.setORA(dt_it.toString(patternhours_d));
    // d1.setMESE(dt_it.monthOfYear().getAsText(Locale.ITALY).toUpperCase());
    // d1.setANNO(dt_it.year().getAsText());
    // d1.setCODUSER(g.getCod());
    // d1.setUSERNOME(g.getDe_nome().toUpperCase());
    // d1.setUSERCOGNOME(g.getDe_cognome().toUpperCase());
    // d1.setMETODOPAGAMENTO("");
    // d1.setRESIDENZACLIENTE("");
    // d1.setNAZIONALITACLIENTE("");
    // d1.setCOMMENTI(rset.getString("e.note"));
    // d1.setACQUISTOVENDITA(format_tofrom_brba_new(rset.getString("e.fg_tofrom"),
    // rset.getString("e.fg_brba"), rset.getString("e.cod_dest"),
    // bank, bank2));
    // d1.setTIPOLOGIAACQOVEND(f.getDe_supporto());
    // d1.setVALUTA(rset.getString("ev.currency"));
    // d1.setQUANTITA((rset.getString("ev.ip_quantity")));
    // d1.setTASSODICAMBIO((rset.getString("ev.ip_rate")));
    // d1.setCONTROVALORE((rset.getString("ev.ip_total")));
    // d1.setCOMMVARIABILE("");
    // d1.setCOMMFISSA("");
    //
    // String SPREADBRANCH = "";
    // String SPREADBANK = "";
    //
    // if (rset.getString("e.fg_brba").equals("BR")) {
    // SPREADBRANCH = (rset.getString("ev.ip_spread"));
    // } else {
    // SPREADBANK = (rset.getString("ev.ip_spread"));
    // }
    //
    // d1.setSPREADBRANCH(SPREADBRANCH);
    // d1.setSPREADBANK(SPREADBANK);
    //
    // d1.setSPREADVEND("");
    //
    // double totgm = fd(rset.getString("ev.ip_spread"));
    // d1.setTOTGM((roundDoubleandFormat(totgm, 2)));
    //
    // d1.setPERCCOMM(""); //verificare
    // d1.setPERCSPREADVENDITA("");//verificare
    // d1.setVENDITABUYBACK("");
    // d1.setCODICEINTERNETBOOKING("");
    // String FASCEIMPORTO = "";
    // d1.setFASCEIMPORTO(FASCEIMPORTO);
    // d1.setMOTIVOPERRIDUZIONEDELLACOMM("");
    // d1.setMOTIVOPERRIDUZIONEDELLACOMMFISSA("");
    // d1.setCODICESBLOCCO("");
    //
    // out.add(d1);
    //
    // }
    //
    // String sql = "SELECT * FROM ch_transaction tr1, ch_transaction_valori tr2
    // WHERE tr1.cod=tr2.cod_tr AND tr1.filiale = '" + b1.getCod() + "' ";
    // if (datad1 != null) {
    // sql = sql + "AND tr1.data >= '" + datad1 + " 00:00:00' ";
    // }
    // if (datad2 != null) {
    // sql = sql + "AND tr1.data <= '" + datad2 + " 23:59:59' ";
    // }
    //
    // if (!deleted) {
    // sql = sql + "AND tr1.del_fg <= '0' ";
    // }
    //
    // sql = sql + " ORDER BY tr1.data";
    //
    // ResultSet rs = this.c.createStatement().executeQuery(sql);
    //
    // while (rs.next()) {
    // DailyChange_CG d1 = new DailyChange_CG();
    //
    // DateTime dt_tr = getDT(rs.getString("tr1.data"), patternsqldate);
    // Users g = get_user(rs.getString("tr1.user"), list_users);
    // Figures f = get_figures(fig, rs.getString("tr2.supporto"));
    // Figures p = get_figures(fig, rs.getString("tr1.localfigures"));
    //
    // d1.setCDC(b1.getCod());
    // d1.setSPORTELLO(b1.getDe_branch().toUpperCase());
    // d1.setID(rs.getString("tr1.id"));
    // if (rs.getString("tr1.del_fg").equals("1")) {
    // d1.setDELETE("SI");
    // } else {
    // d1.setDELETE("");
    // }
    //
    // d1.setAREA(Utility.formatAL(b1.getBrgr_01(), list_group, 1));
    // d1.setCITTA(Utility.formatAL(b1.getBrgr_04(), list_group, 1));
    // d1.setUBICAZIONE(Utility.formatAL(b1.getBrgr_02(), list_group, 1));
    // d1.setGRUPPO(Utility.formatAL(b1.getBrgr_03(), list_group, 1));
    // d1.setDATA(dt_tr.toString(patternnormdate_filter));
    // d1.setORA(dt_tr.toString(patternhours_d));
    // d1.setMESE(dt_tr.monthOfYear().getAsText(Locale.ITALY).toUpperCase());
    // d1.setANNO(dt_tr.year().getAsText());
    // d1.setCODUSER(g.getCod());
    // d1.setUSERNOME(g.getDe_nome().toUpperCase());
    // d1.setUSERCOGNOME(g.getDe_cognome().toUpperCase());
    //
    // if (rs.getString("tr1.tipotr").equals("B")) {
    // d1.setMETODOPAGAMENTO(f.getDe_supporto());
    // } else {
    // d1.setMETODOPAGAMENTO(p.getDe_supporto());
    // }
    //
    // Client c0 = query_Client_transaction(rs.getString("tr1.cod"),
    // rs.getString("tr1.cl_cod"));
    // d1.setRESIDENZACLIENTE(Utility.formatALN(c0.getNazione(), nazioni, 1));
    // d1.setNAZIONALITACLIENTE(Utility.formatALN(c0.getNazione_nascita(), nazioni,
    // 1));
    //
    // d1.setCOMMENTI(rs.getString("tr1.note"));
    //
    // d1.setACQUISTOVENDITA(formatType_new(rs.getString("tr1.tipotr"),
    // rs.getString("tr1.intbook"),
    // rs.getString("tr1.intbook_type"),
    // rs.getString("tr1.intbook_1_tf"),
    // rs.getString("tr1.intbook_2_tf"),
    // rs.getString("tr1.intbook_3_tf"), listcat, listcaus));
    //
    // d1.setTIPOLOGIAACQOVEND(f.getDe_supporto());
    //
    // d1.setVALUTA(rs.getString("tr2.valuta"));
    // d1.setQUANTITA((rs.getString("tr2.quantita")));
    // d1.setTASSODICAMBIO((rs.getString("tr2.rate")));
    // d1.setCONTROVALORE((rs.getString("tr2.total")));
    // d1.setCOMMVARIABILE((rs.getString("tr2.com_perc_tot")));
    // d1.setCOMMFISSA((rs.getString("tr2.fx_com")));
    //
    // String SPREADBRANCH = "", SPREADBANK = "";
    // d1.setSPREADBRANCH(SPREADBRANCH);
    // d1.setSPREADBANK(SPREADBANK);
    //
    // d1.setSPREADVEND((rs.getString("tr2.spread")));
    //
    // double totgm = fd(rs.getString("tr2.spread"))
    // + fd(rs.getString("tr2.tot_com"))
    // + parseDoubleR(gf, rs.getString("tr2.roundvalue"));
    //
    // if (gf.isIs_CZ()) {
    // double rv = parseDoubleR_CZ(gf, rs.getString("tr2.roundvalue"),
    // rs.getString("tr1.tipotr").equals("B"));
    // totgm = fd(rs.getString("tr2.spread"))
    // + fd(rs.getString("tr2.tot_com"))
    // + rv;
    // }
    //
    // d1.setTOTGM((roundDoubleandFormat(totgm, 2)));
    //
    // d1.setPERCCOMM(""); //verificare
    // d1.setPERCSPREADVENDITA("");//verificare
    //
    // if (rs.getString("tr2.bb").equals("Y")) {
    // d1.setVENDITABUYBACK(formatMysqltoDisplay(get_Value_history_BB(history_BB,
    // dt_tr, f)));
    // } else if (rs.getString("tr2.bb").equals("F")) {
    // d1.setVENDITABUYBACK(formatMysqltoDisplay("0.00"));
    // } else {
    // d1.setVENDITABUYBACK("");
    // }
    //
    // if (rs.getString("tr1.intbook").equals("1")) {
    // String[] ib = internetbooking_ch(rs.getString("tr1.cod"));
    // if (ib != null) {
    // d1.setCODICEINTERNETBOOKING(ib[1].toUpperCase());
    // } else {
    // d1.setCODICEINTERNETBOOKING("");
    // }
    // } else {
    // d1.setCODICEINTERNETBOOKING("");
    // }
    //
    // String FASCEIMPORTO = "";
    // d1.setFASCEIMPORTO(FASCEIMPORTO);
    //
    // d1.setMOTIVOPERRIDUZIONEDELLACOMM(formatAL(rs.getString("tr2.kind_fix_comm"),
    // array_undermincommjustify, 1));
    // d1.setMOTIVOPERRIDUZIONEDELLACOMMFISSA(formatAL(rs.getString("tr2.low_com_ju"),
    // array_kindcommissionefissa, 1));
    //
    // Codici_sblocco cs1 = getCod_tr(rs.getString("tr1.cod"), "01");
    // String cs = "";
    // if (cs1 != null) {
    // cs = cs1.getCodice();
    // }
    // d1.setCODICESBLOCCO(cs);
    //
    //// String loy = Engine.query_LOY_transaction(rs.getString("tr1.cod"), null,
    // "000");
    // String loy = query_LOY_transaction(rs.getString("tr1.cod"));
    //
    // if (loy == null) {
    // loy = "";
    // }
    // d1.setLOYALTYCODE(loy);
    //
    // out.add(d1);
    //
    // }
    //
    // } catch (SQLException ex) {
    // ex.printStackTrace();
    // }
    // return out;
    // }
    public ArrayList<Openclose> list_openclose_errors_report(String filiale, String datad1, String datad2) {
        ArrayList<Openclose> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM oc_lista where filiale='" + filiale + "' AND errors ='Y' ";

            if (datad1 != null) {
                sql = sql + "AND data >= '" + datad1 + " 00:00:00' ";
            }
            if (datad2 != null) {
                sql = sql + "AND data <= '" + datad2 + " 23:59:59' ";
            }
            sql = sql + " ORDER BY data";

            PreparedStatement ps = this.c.prepareStatement(sql);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Openclose oc = new Openclose(rs.getString(1), rs.getString(2),
                        StringUtils.leftPad(rs.getString(3), 15, "0"),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8));
                oc.setCod_it(rs.getString("cod_it"));
                oc.setCod_itnc(rs.getString("cod_itnc"));
                oc.setForeign_tr(rs.getString("foreign_tr"));
                oc.setLocal_tr(rs.getString("local_tr"));
                oc.setStock_tr(rs.getString("stock_tr"));
                out.add(oc);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Openclose_Error_value> list_openclose_errors_report(
            Openclose oc,
            ArrayList<String[]> list_oc_errors,
            ArrayList<Branch> allbr, String soglia) {
        ArrayList<Openclose_Error_value> out = new ArrayList<>();

        String oper = get_national_office().getChangetype();
        boolean dividi = oper.equals("/");
        ArrayList<String[]> list_cc = credit_card_enabled();
        ArrayList<String[]> ba = list_bankAccount();
        ArrayList<NC_category> li = query_nc_category("", "000");
        double finalbalance = 0.00;

        ArrayList<Openclose_Error_value> temp = new ArrayList<>();

        for (int i = 0; i < list_oc_errors.size(); i++) {
            String[] value = list_oc_errors.get(i);

            if (value[1].equals("CH")) {
                Openclose_Error_value osp = new Openclose_Error_value();
                if (allbr != null) {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(formatBankBranch(oc.getFiliale(), "BR", null, allbr, null));
                } else {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(oc.getFiliale());
                }
                osp.setUser(oc.getUser());
                osp.setData(formatStringtoStringDate(oc.getData(), patternsqldate, patternnormdate));
                osp.setTipo(oc.getFg_tipo());
                osp.setCod(oc.getId());
                osp.setType(value[1]);
                osp.setCurrency(value[2]);
                osp.setKind(value[3]);
                osp.setNc("-");
                osp.setPos("-");
                osp.setNote(value[6]);
                osp.setTotal_diff(value[7]);
                osp.setRate(value[8]);
                osp.setTill(oc.getTill());
                osp.setQuantityUser(value[11]);
                osp.setQuantitySystem(value[13]);

                osp.setAmountuser(value[11]);
                osp.setAmountsystem(value[13]);

                String diff = getValueDiff_R(value[11], value[13], value[7], value[8], dividi);
                osp.setDiffContr(diff);
                osp.setDiffAmount(diff);
                osp.setQuantitydiff(diff);
                osp.setLocalamount(value[11]);
                osp.setOperazione(oc.getId());
                temp.add(osp);

                finalbalance = finalbalance + fd(osp.getDiffContr());

            } else if (value[1].equals("NC")) {
                Openclose_Error_value osp = new Openclose_Error_value();
                if (allbr != null) {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(formatBankBranch(oc.getFiliale(), "BR", null, allbr, null));
                } else {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(oc.getFiliale());
                }
                osp.setUser(oc.getUser());
                osp.setData(formatStringtoStringDate(oc.getData(), patternsqldate, patternnormdate));
                osp.setTipo(oc.getFg_tipo());
                osp.setCod(oc.getId());
                osp.setType(value[1]);
                osp.setCurrency("-");
                osp.setKind("-");
                NC_category nc = getNC_category(li, value[4]);
                if (nc != null) {
                    osp.setNc(nc.getGruppo_nc() + " " + nc.getDe_gruppo_nc());
                    osp.setPos("-");
                    osp.setNote(value[6]);
                    osp.setTotal_diff(formatDoubleforMysql(this.gf, value[7]));
                    osp.setRate("-");
                    osp.setTill(oc.getTill());

                    osp.setQuantityUser(value[10]);
                    osp.setQuantitySystem(value[12]);

                    osp.setLocalamount(
                            roundDoubleandFormat(fd(nc.getIp_prezzo_nc()) * (fd(value[10]) - fd(value[12])), 2));
                    osp.setNcprice(nc.getIp_prezzo_nc());
                    // osp.setLocalamount(value[10]);

                    osp.setDiffContr("0.00");
                    osp.setDiffAmount(value[7]);
                    osp.setOperazione(oc.getId());

                    osp.setAmountuser(value[10]);
                    osp.setAmountsystem(value[12]);
                    osp.setQuantitydiff(value[7]);
                    // out.add(osp);
                    temp.add(osp);

                    finalbalance = finalbalance + fd(osp.getLocalamount());
                }
            } else if (value[1].equals("PO")) {
                Openclose_Error_value osp = new Openclose_Error_value();
                if (allbr != null) {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(formatBankBranch(oc.getFiliale(), "BR", null, allbr, null));
                } else {
                    osp.setId_filiale(oc.getFiliale());
                    osp.setDe_filiale(oc.getFiliale());
                }

                osp.setUser(oc.getUser());
                osp.setData(formatStringtoStringDate(oc.getData(), patternsqldate, patternnormdate));
                osp.setTipo(oc.getFg_tipo());
                osp.setCod(oc.getId());
                osp.setType(value[1]);
                osp.setCurrency(value[2]);
                osp.setKind(value[3]);
                osp.setNc("-");

                String po = Utility.formatAL(value[5], list_cc, 1);
                if (po.equals("-")) {
                    po = Utility.formatAL(value[5], ba, 1);
                }

                osp.setPos(value[5] + " - " + po);

                osp.setNote(value[6]);
                osp.setTotal_diff(formatDoubleforMysql(this.gf, value[7]));
                osp.setRate(value[8]);
                osp.setTill(oc.getTill());
                osp.setQuantityUser(value[10]);
                osp.setQuantitySystem(value[12]);
                osp.setLocalamount(formatDoubleforMysql(this.gf, value[10]));
                String diff = getValueDiff_R(value[11], value[13], value[7], value[8], dividi);
                osp.setDiffContr(diff);
                osp.setDiffAmount((value[7]));
                osp.setAmountuser(value[11]);
                osp.setAmountsystem(value[13]);
                osp.setQuantitydiff(roundDoubleandFormat(fd(value[10]) - fd(value[12]), 0));
                osp.setOperazione(oc.getId());
                temp.add(osp);
                finalbalance = finalbalance + fd(osp.getDiffAmount());
            }
        }
        if (soglia == null) {
            out.addAll(temp);
        } else if (parseDoubleR(this.gf, roundDoubleandFormat(finalbalance, 2)) >= parseDoubleR(this.gf, soglia)) {
            out.addAll(temp);
        }
        return out;
    }

    public ArrayList<NC_category> query_nc_category(String nc_kind, String filiale) {
        ArrayList<NC_category> out = new ArrayList<>();
        try {
            PreparedStatement ps;
            String sql;
            if ((nc_kind.equals("")) || (nc_kind.equals("..."))) {
                sql = "SELECT * FROM nc_tipologia WHERE filiale = ? order by gruppo_nc";
                ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, filiale);
            } else {
                sql = "SELECT * FROM nc_tipologia WHERE fg_tipo_transazione_nc = ? AND filiale = ? order by gruppo_nc";
                ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ps.setString(1, nc_kind);
                ps.setString(2, filiale);
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                NC_category nc1 = new NC_category();
                nc1.setFiliale(rs.getString("filiale"));
                nc1.setGruppo_nc(rs.getString("gruppo_nc"));
                nc1.setDe_gruppo_nc(visualizzaStringaMySQL(rs.getString("de_gruppo_nc")));
                nc1.setFg_tipo_transazione_nc(rs.getString("fg_tipo_transazione_nc"));
                nc1.setIp_prezzo_nc(rs.getString("ip_prezzo_nc"));
                nc1.setAnnullato(rs.getString("annullato"));
                nc1.setConto_coge_01(rs.getString("conto_coge_01"));
                nc1.setConto_coge_02(rs.getString("conto_coge_02"));
                nc1.setDe_scontrino(visualizzaStringaMySQL(rs.getString("de_scontrino")));
                nc1.setDe_riga(visualizzaStringaMySQL(rs.getString("de_riga")));
                nc1.setPc_iva_corrispettivo(rs.getString("pc_iva_corrispettivo"));
                nc1.setTicket_fee(rs.getString("ticket_fee"));
                nc1.setMax_ticket(rs.getString("max_ticket"));
                nc1.setTicket_fee_type(rs.getString("ticket_fee_type"));
                nc1.setTicket_enabled(rs.getString("ticket_enabled"));
                nc1.setTimestamp(rs.getString("data"));
                nc1.setFg_registratore(rs.getString("fg_registratore"));
                nc1.setInt_code(rs.getString("int_code"));
                nc1.setInt_corrisp(rs.getString("int_corrisp"));
                nc1.setInt_iva(rs.getString("int_iva"));

                out.add(nc1);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public boolean insert_Qlik_VAT(Qlik_ref value) {
        String ins = "INSERT INTO qlik_vat VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement ps = this.c.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, value.getCod());
            ps.setString(2, value.getId());
            ps.setString(3, value.getData());
            ps.setString(4, value.getFiliale());
            ps.setString(5, value.getDel_fg());
            ps.setString(6, value.getUser());
            ps.setString(7, value.getQuantity());
            ps.setString(8, value.getPrice());
            ps.setString(9, value.getFee());
            ps.setString(10, value.getGruppo_nc());
            ps.setString(11, value.getCausale_nc());
            ps.setString(12, value.getClient());
            ps.setString(13, value.getType());
            ps.setString(14, value.getFix_com());
            ps.setString(15, value.getVar_com());
            ps.setString(16, value.getRound());
            ps.setString(17, value.getCommission());
            ps.setString(18, value.getVolume());
            ps.setString(19, value.getEstimated_CO());
            ps.setString(20, value.getEstimated_GM());
            ps.execute();
            ps.close();
            return true;
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate entry")) {
                return true;
            }
            ex.printStackTrace();
        }
        return false;
    }

    public boolean verifica_Delete() {
        try {
            String sql = "SELECT cod FROM nc_transaction WHERE DATA >= ? AND fg_tipo_transazione_nc = ? AND del_fg = ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, "2020-01-01 00:00:00");
            ps.setString(2, "3");
            ps.setString(3, "1");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String cod = rs.getString(1);
                String upd = "UPDATE qlik_vat SET del_fg = ? WHERE cod = ?";
                PreparedStatement psUpd = this.c.prepareStatement(upd);
                psUpd.setString(1, "1");
                psUpd.setString(2, cod);
                psUpd.executeUpdate();
                psUpd.close();
            }
            ps.close();
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public ArrayList<String[]> nc_kind() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT codice,descrizione,et1,et2 FROM nc_kind WHERE attivo = ? order by descrizione;";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "1");
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

    public ArrayList<C_TransactionRegisterDetail_value> list_C_TransactionRegisterDetail_value(
            String data1, String data2, ArrayList<String> branch) {
        ArrayList<C_TransactionRegisterDetail_value> out = new ArrayList<>();
        try {
            ArrayList<String[]> nck = nc_kind();
            String loc[] = get_local_currency();

            branch.add("000");

            String sql = "SELECT * FROM ch_transaction WHERE del_fg='0' ";
            String filwhere = "";
            for (int i = 0; i < branch.size(); i++) {
                filwhere = filwhere + "filiale=? OR ";
            }

            if (filwhere.length() > 3) {
                sql = sql + " AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
            }

            if (data1 != null) {
                sql = sql + "AND data >= ? ";
            }

            if (data2 != null) {
                sql = sql + "AND data <= ? ";
            }

            sql = sql + " ORDER BY filiale,tipotr,data";

            try (Connection conn = DriverManager.getConnection("url", "username", "password");
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int parameterIndex = 1;
                for (String b : branch) {
                    pstmt.setString(parameterIndex++, b);
                }
                if (data1 != null) {
                    pstmt.setString(parameterIndex++, data1 + " 00:00:00");
                }
                if (data2 != null) {
                    pstmt.setString(parameterIndex, data2 + " 23:59:59");
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String sql1 = "SELECT * FROM ch_transaction_valori WHERE cod_tr= ? order by valuta,supporto";
                        try (PreparedStatement pstmt1 = conn.prepareStatement(sql1)) {
                            pstmt1.setString(1, rs.getString("cod"));
                            try (ResultSet rs1 = pstmt1.executeQuery()) {
                                while (rs1.next()) {
                                    C_TransactionRegisterDetail_value ncmfb = new C_TransactionRegisterDetail_value();

                                    String start = "";

                                    if (rs.getString("tipotr").equals("B")) {
                                        start = "-";
                                    }

                                    ncmfb.setFiliale(rs.getString("filiale"));
                                    ncmfb.setTill(rs.getString("till"));
                                    ncmfb.setUser(rs.getString("user"));
                                    ncmfb.setDate(formatStringtoStringDate(rs.getString("data"), patternsqldate,
                                            patternnormdate));
                                    ncmfb.setDt(parseStringDate(rs.getString("data"), patternsqldate));
                                    ncmfb.setCur(rs1.getString("valuta"));
                                    ncmfb.setKind(Ch_transaction.formatType(rs.getString("tipotr")));
                                    ncmfb.setAmountqty(start + rs1.getString("quantita"));
                                    ncmfb.setRate(rs1.getString("rate"));
                                    ncmfb.setTotal(start + rs1.getString("total"));
                                    ncmfb.setPerc(rs1.getString("com_perc_tot"));
                                    ncmfb.setCommfee(rs1.getString("tot_com"));
                                    ncmfb.setRefundoff(rs.getString("round"));
                                    ncmfb.setPayinout(start + rs1.getString("net"));
                                    out.add(ncmfb);
                                }
                            }
                        }
                    }
                }
            }

            sql = "SELECT * FROM nc_transaction WHERE del_fg='0' ";
            filwhere = "";
            for (int i = 0; i < branch.size(); i++) {
                filwhere = filwhere + "filiale=? OR ";
            }

            if (filwhere.length() > 3) {
                sql = sql + " AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
            }

            if (data1 != null) {
                sql = sql + "AND data >= ? ";
            }

            if (data2 != null) {
                sql = sql + "AND data <= ? ";
            }

            sql = sql + " ORDER BY filiale,fg_tipo_transazione_nc,gruppo_nc,data";

            try (Connection conn = DriverManager.getConnection("url", "username", "password");
                    PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int parameterIndex = 1;
                for (String b : branch) {
                    pstmt.setString(parameterIndex++, b);
                }
                if (data1 != null) {
                    pstmt.setString(parameterIndex++, data1 + " 00:00:00");
                }
                if (data2 != null) {
                    pstmt.setString(parameterIndex, data2 + " 23:59:59");
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        C_TransactionRegisterDetail_value ncmfb = new C_TransactionRegisterDetail_value();

                        if (rs.getString("fg_tipo_transazione_nc").equals("1")) {
                            ncmfb.setTotal(rs.getString("netto"));
                            ncmfb.setCommfee(rs.getString("commissione"));
                            ncmfb.setPayinout(rs.getString("total"));
                        } else {
                            ncmfb.setTotal(rs.getString("total"));
                            ncmfb.setCommfee(rs.getString("commissione"));
                            ncmfb.setPayinout(rs.getString("total"));
                        }

                        String start = "";

                        if (ncmfb.getTotal().contains("-")) {
                            start = "-";
                        }

                        ncmfb.setFiliale(rs.getString("filiale"));
                        ncmfb.setTill(rs.getString("till"));
                        ncmfb.setUser(rs.getString("user"));
                        ncmfb.setDate(formatStringtoStringDate(rs.getString("data"), patternsqldate, patternnormdate));
                        ncmfb.setDt(parseStringDate(rs.getString("data"), patternsqldate));
                        ncmfb.setCur(loc[0]);
                        ncmfb.setKind(Utility.formatAL(rs.getString("fg_tipo_transazione_nc"), nck, 1));

                        String q = rs.getString("quantita");
                        if (q.equals("0") || q.equals("0.00")) {
                            q = "1.00";
                        }

                        ncmfb.setAmountqty(start + q);

                        ncmfb.setRate("-");
                        ncmfb.setPerc("-");
                        ncmfb.setRefundoff("-");
                        out.add(ncmfb);
                    }
                }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        Collections.sort(out);

        return out;
    }

    public ArrayList<Openclose> query_oc(String d1, String d2) {
        ArrayList<Openclose> out = new ArrayList<>();
        try {
            d1 = d1.trim() + " 00:00:00";
            d2 = d2.trim() + " 23:59:59";
            String sql = "SELECT * FROM oc_lista WHERE data >= '" + d1 + "' AND data <= '" + d2
                    + "' ORDER BY data,id DESC";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            System.out.println(sql);
            while (rs.next()) {
                out.add(new Openclose(rs.getString(1), rs.getString(2), StringUtils.leftPad(rs.getString(3), 15, "0"),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8)));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<Till> list_till() {
        ArrayList<Till> out = new ArrayList<>();
        try {
            String sql = "SELECT cod,de_till,filiale FROM till";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(new Till(rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3)));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

}
