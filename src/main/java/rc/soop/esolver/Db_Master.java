/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import static rc.soop.esolver.Util.formatStringtoStringDate;
import static rc.soop.esolver.Util.log;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.patternsqldate;
import static rc.soop.esolver.Util.test;
import static rc.soop.esolver.Util.visualizzaStringaMySQL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
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

    public Connection getC() {
        return c;
    }

    public void setC(Connection c) {
        this.c = c;
    }

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
            String host = test ? rb.getString("db.ip") + "/maccorp" : rb.getString("db.ip") + "/maccorpita";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
    }

    public void closeDB() {
        try {
            if (this.c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<Ch_transaction> list_esolver_ch(String data, ArrayList<Branch> branch, String branching) {
        ArrayList<Ch_transaction> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM ch_transaction WHERE del_fg ='0' AND cod NOT IN (SELECT transaction FROM inv_list) ";
            sql += "AND data >= ? ";
            sql += "AND data <= ? ";

            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere += "(filiale=?) OR ";
                }

                if (filwhere.length() > 3) {
                    sql += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }
            } else {
                sql += "AND filiale = ?";
            }

            sql += " ORDER BY data";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, data + " 00:00:00");
                ps.setString(2, data + " 23:59:59");
                int paramIndex = 3;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
                }

                try (ResultSet rs = ps.executeQuery()) {
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
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<Ch_transaction_refund> list_esolver_refund(String data, ArrayList<Branch> branch,
            String branching) {
        ArrayList<Ch_transaction_refund> out = new ArrayList<>();

        try {
            String sql = "SELECT * FROM ch_transaction_refund WHERE status=? AND method='BR' ";
            sql += "AND dt_refund >= ? ";
            sql += "AND dt_refund <= ? ";
            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere = filwhere + "(branch_cod=?) OR ";
                }

                if (filwhere.length() > 3) {
                    sql += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }
            } else {
                sql += "AND branch_cod=?";
            }
            sql += " ORDER BY dt_refund";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, "1");
                ps.setString(2, data + " 00:00:00");
                ps.setString(3, data + " 23:59:59");
                int paramIndex = 4;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Ch_transaction_refund ref = new Ch_transaction_refund();
                        ref.setCod(rs.getString(1));
                        ref.setCod_tr(rs.getString(2));
                        ref.setFrom(rs.getString(3));
                        ref.setMethod(rs.getString(4));
                        ref.setBranch_cod(rs.getString(5));
                        ref.setType(rs.getString(6));
                        ref.setValue(rs.getString(7));
                        ref.setCod_usaegetta(rs.getString(8));
                        ref.setStatus(rs.getString(9));
                        ref.setUser_refund(rs.getString(10));
                        ref.setDt_refund(rs.getString(11));
                        ref.setIdopentill_refund(rs.getString(12));
                        ref.setTimestamp(rs.getString(13));
                        out.add(ref);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<NC_transaction> list_esolver_nc(String data, ArrayList<Branch> branch, String branching) {
        ArrayList<NC_transaction> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_transaction WHERE del_fg =? AND gruppo_nc <> 'ATLANTE' ";
            sql += "AND data >= ? ";
            sql += "AND data <= ? ";

            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere += "(filiale=?) OR ";
                }

                if (filwhere.length() > 3) {
                    sql += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }
            } else {
                sql += "AND filiale=?";
            }

            sql += " ORDER BY data";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, "0");
                ps.setString(2, data + " 00:00:00");
                ps.setString(3, data + " 23:59:59");
                int paramIndex = 4;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
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
                                rs.getString(19),
                                rs.getString(20),
                                rs.getString(21), rs.getString(22), rs.getString(23), rs.getString(24),
                                rs.getString(25),
                                rs.getString(26),
                                rs.getString(27), rs.getString(28), rs.getString(29), rs.getString(30),
                                rs.getString(31),
                                rs.getString(32),
                                rs.getString(33), rs.getString(34), rs.getString(35), rs.getString(36),
                                rs.getString(37),
                                rs.getString(38),
                                rs.getString(39), rs.getString(40), rs.getString(41), rs.getString(42),
                                rs.getString(43),
                                rs.getString(44),
                                rs.getString(45), rs.getString(46));
                        out.add(nc);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<ET_change> list_esolver_et(String data, ArrayList<Branch> branch, String branching) {
        ArrayList<ET_change> out = new ArrayList<>();
        try {
            String sql3 = "SELECT * FROM et_change WHERE fg_annullato=? ";
            sql3 += "AND dt_it >= ? ";
            sql3 += "AND dt_it <= ? ";

            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere += "(filiale=?) OR ";
                }
                if (filwhere.length() > 3) {
                    sql3 += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }

            } else {
                sql3 += "AND filiale =?";
            }

            sql3 += " AND cod_dest NOT IN (SELECT cod FROM bank WHERE fg_annullato = ? AND (bank_account = ? OR cod IN (SELECT DISTINCT(carta_credito) FROM carte_credito))) ";
            sql3 += " ORDER BY dt_it";

            try (PreparedStatement ps = this.c.prepareStatement(sql3)) {
                ps.setString(1, "0");
                ps.setString(2, data + " 00:00:00");
                ps.setString(3, data + " 23:59:59");
                int paramIndex = 4;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
                }
                ps.setString(paramIndex + 1, "0");
                ps.setString(paramIndex + 2, "Y");

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ET_change et = new ET_change();
                        et.setCod(rs.getString(1));
                        et.setId(StringUtils.leftPad(rs.getString(2), 10, "0"));
                        et.setFiliale(rs.getString(3));
                        et.setUser(rs.getString(4));
                        et.setTill_from(rs.getString(5));
                        et.setFg_tofrom(rs.getString(6));
                        et.setFg_brba(rs.getString(7));
                        et.setCod_dest(rs.getString(8));
                        et.setIdopen_from(rs.getString(9));
                        et.setDt_it(rs.getString(10));
                        et.setFg_annullato(rs.getString(11));
                        et.setDel_dt(rs.getString(12));
                        et.setDel_user(rs.getString(13));
                        et.setDel_motiv(rs.getString(14));
                        et.setNote(rs.getString(15));
                        et.setIp_oneri(rs.getString(16));
                        et.setFiliale_in(rs.getString(17));
                        et.setId_in(rs.getString(18));
                        et.setCod_in(rs.getString(19));
                        out.add(et);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<Openclose> list_esolver_ocerr(String data, ArrayList<Branch> branch, String branching) {
        ArrayList<Openclose> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM oc_lista WHERE errors = ? ";
            sql += "AND data >= ? ";
            sql += "AND data <= ? ";

            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere += "(filiale=?) OR ";
                }
                if (filwhere.length() > 3) {
                    sql += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }
            } else {
                sql += "AND filiale =?";
            }
            sql += " ORDER BY data";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, "Y");
                ps.setString(2, data + " 00:00:00");
                ps.setString(3, data + " 23:59:59");
                int paramIndex = 4;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
                }

                try (ResultSet rs = ps.executeQuery()) {
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
                }
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<NC_causal> list_nc_causal_enabled() {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_causali WHERE annullato = ? AND filiale = ? order by causale_nc";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, getCodLocal(true)[0]);
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public String[] getCodLocal(boolean onlycod) {
        try {
            String sql = "SELECT cod FROM local LIMIT 1";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String cod = rs.getString(1);
                if (onlycod) {
                    String[] out = { cod, cod };
                    return out;
                }

                String sql2 = "SELECT de_branch FROM branch WHERE cod = ?";
                PreparedStatement ps2 = this.c.prepareStatement(sql2, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ps2.setString(1, cod);
                ResultSet rs2 = ps2.executeQuery();
                if (rs2.next()) {
                    String[] out = { cod, rs2.getString(1) };
                    return out;
                }
                ps2.close();
                ps.close();
            }

        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public ArrayList<NC_category> list_nc_category_enabled() {
        ArrayList<NC_category> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_tipologia WHERE annullato = ? AND filiale = ? order by gruppo_nc";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, getCodLocal(true)[0]);
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
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
        }
        return new String[] { "EUR", "242" };
    }

    public ArrayList<String[]> get_fatt_note(String data, ArrayList<Branch> branch, String branching) {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM inv_list i, ch_transaction c WHERE i.stato='1' AND i.transaction = c.cod ";
            sql += "AND i.dt >= ? AND i.dt <= ? ";

            if (branching == null) {
                String filwhere = "";
                for (int i = 0; i < branch.size(); i++) {
                    filwhere += "c.filiale=? OR ";
                }
                if (filwhere.length() > 3) {
                    sql += "AND (" + filwhere.substring(0, filwhere.length() - 3).trim() + ") ";
                }
            } else {
                sql += "AND c.filiale = ?";
            }

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, data + " 00:00:00");
                ps.setString(2, data + " 23:59:59");
                int paramIndex = 3;
                if (branching == null) {
                    for (Branch b : branch) {
                        ps.setString(paramIndex++, b.getCod());
                    }
                } else {
                    ps.setString(paramIndex, branching);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String[] valori = {
                                rs.getString("c.filiale"), rs.getString("c.cod"), rs.getString("c.cl_cod"),
                                rs.getString("c.pay"),
                                rs.getString("c.total"), rs.getString("c.commission"), rs.getString("i.tipo"),
                                rs.getString("c.tipotr"),
                                rs.getString("c.round"), rs.getString("i.numero"), rs.getString("c.localfigures"),
                                rs.getString("c.pos"),
                                rs.getString("c.del_fg"), rs.getString("c.spread_total")
                        };
                        out.add(valori);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public ArrayList<String[]> contabilita() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM contabilita ORDER BY descrizione";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), rs.getString(2), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
                        formatStringtoStringDate(rs.getString(4), "yyyy-MM-dd", "dd/MM/yyyy"), rs.getString(5),
                        rs.getString(6) };
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public String contabilita(String cod) {
        String sql = "SELECT conto FROM contabilita WHERE id = ?";
        try (PreparedStatement stmt = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            stmt.setString(1, cod);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return "-";
    }

    public Ch_transaction query_transaction_ch(String cod) {
        try {
            String sql = "SELECT * FROM ch_transaction WHERE cod = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
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
                ch.setCredccard_number(rs.getString("credccard_number"));
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
                ch.setBb(rs.getString("bb"));
                return ch;
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public CustomerKind get_customerKind(String cod) {
        try {
            String sql = "SELECT * FROM tipologiaclienti WHERE tipologia_clienti = ? ORDER BY de_tipologia_clienti DESC";

            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
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
                ck.setTaxfree(rs.getString("taxfree"));

                ck.setTipofat(rs.getString("tipofat"));
                ck.setVatcode(rs.getString("vatcode"));
                ck.setIp_soglia_bollo(rs.getString("ip_soglia_bollo"));
                ck.setIp_value_bollo(rs.getString("ip_value_bollo"));
                ck.setDescr_bollo(rs.getString("descr_bollo"));
                ck.setResident(rs.getString("resid"));
                ck.setTaxfree(rs.getString("taxfree"));

                return ck;
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public ArrayList<Ch_transaction_value> query_transaction_value(String cod_tr) {
        ArrayList<Ch_transaction_value> li = new ArrayList<>();
        try {
            Ch_transaction ch1 = query_transaction_ch(cod_tr);
            if (ch1 == null) {
                ch1 = query_transaction_ch_temp(cod_tr);
                if (ch1 == null) {
                    return li;
                }
            }

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
                chv.setTrorig(ch1);
                chv.setRoundvalue(rs.getString(27));
                li.add(chv);
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return li;
    }

    public Client query_Client_transaction(String codtr, String codcl) {
        try {
            String sql = "SELECT * FROM ch_transaction_client WHERE codtr = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
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
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public Ch_transaction query_transaction_ch_temp(String cod) {
        try {
            String sql = "SELECT * FROM ch_transaction_temp WHERE cod = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
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
                ch.setCredccard_number(rs.getString("credccard_number"));
                ch.setCl_cf(rs.getString("cl_cf"));
                ch.setCl_cod(rs.getString("cl_cod"));
                ch.setDel_fg(rs.getString("del_fg"));
                ch.setDel_dt(rs.getString("del_dt"));
                ch.setDel_user(rs.getString("del_user"));
                ch.setDel_motiv(rs.getString("del_motiv"));
                ch.setRefund(rs.getString("refund"));
                ch.setFa_number(rs.getString("fa_number"));
                ch.setCn_number(rs.getString("cn_number"));
                ch.setCredccard_number(rs.getString("credccard_number"));
                ch.setBb(rs.getString("bb"));
                return ch;
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public String codnaz_esolv(String cod) {
        String sql = "SELECT cod FROM nazionies where idnaz = ?";
        try (PreparedStatement stmt = this.c.prepareStatement(sql)) {
            stmt.setString(1, cod);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }

        return "US";
    }

    public String[] getCity_apm(String cod) {
        String sql = "SELECT * FROM comuni_apm WHERE codice_avv_bancario = ?";
        try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, cod);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new String[]{ 
                        rs.getString(1), 
                        visualizzaStringaMySQL(rs.getString(2)), 
                        rs.getString(3) 
                    };
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return null;
    }

    public VATcode get_vat(String cod) {
        String sql = "SELECT * FROM vatcode WHERE id = ?";
        try (PreparedStatement stmt = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            stmt.setString(1, cod);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new VATcode(
                        StringUtils.leftPad(rs.getString(1), 2, "0"),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6)
                    );
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return null;
    }


    public ArrayList<String[]> list_oc_errors(String cod_oc) {
        ArrayList<String[]> out = new ArrayList<>();
        String sql = "SELECT cod, tipo, valuta, kind, gruppo_nc, carta_credito, note, total_diff, rate, data, quantity_user, total_user, quantity_system, total_system FROM oc_errors WHERE cod = ? ORDER BY tipo, valuta, kind, gruppo_nc, carta_credito";
        
        try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, cod_oc);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { 
                    rs.getString(1), 
                    rs.getString(2), 
                    rs.getString(3), 
                    rs.getString(4), 
                    rs.getString(5),
                    rs.getString(6), 
                    rs.getString(7), 
                    rs.getString(8), 
                    rs.getString(9), 
                    rs.getString(10),
                    rs.getString(11),
                    rs.getString(12), 
                    rs.getString(13), 
                    rs.getString(14) 
                };
                out.add(o1);
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<ET_change> get_ET_change_value(String cod) {
        try {
            ArrayList<ET_change> list = new ArrayList<>();
            String sql = "SELECT * FROM et_change_valori Where cod = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, cod);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ET_change et = new ET_change();
                et.setFiliale(rs.getString(1));
                et.setCod(rs.getString(2));
                et.setValuta(rs.getString(3));
                et.setSupporto(rs.getString(4));
                et.setIp_stock(rs.getString(5));
                et.setIp_quantity(rs.getString(6));
                et.setIp_rate(rs.getString(7));
                et.setIp_total(rs.getString(8));
                et.setIp_buyvalue(rs.getString(9));
                et.setIp_spread(rs.getString(10));
                et.setDt_it(rs.getString(11));
                list.add(et);
            }
            ps.close();
            return list;
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public DateTime getNowDT() {
        try {
            String sql = "SELECT now()";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsqldate);
                return formatter.parseDateTime(rs.getString(1).substring(0, 19));
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return new DateTime();
    }

    public ArrayList<VATcode> li_vat(String st) {
        ArrayList<VATcode> li = new ArrayList<>();
        String sql = "SELECT * FROM vatcode";
        if (st != null) {
            sql += " WHERE fg_annullato = ?";
        }
        sql += " ORDER BY cast(codice AS decimal (10,0))";

        try (PreparedStatement stmt = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            if (st != null) {
                stmt.setString(1, st);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    li.add(new VATcode(
                        StringUtils.leftPad(rs.getString(1), 2, "0"),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6)
                    ));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return li;
    }


    public ArrayList<String[]> get_list_codice_ATL() {
        ArrayList<String[]> out1 = new ArrayList<>();
        try {
            String sql = "SELECT cod_mac,cod_atl FROM branch_atl";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] v = { rs.getString(1), rs.getString(2) };
                out1.add(v);
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out1;
    }

    public NC_vatcode get_NC_vatcode(String nc_code) {
        try {
            String sql = "SELECT * FROM nc_vatcode WHERE nc_gruppo = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, nc_code);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new NC_vatcode(nc_code, rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10));
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new NC_vatcode(nc_code);
    }

    public String get_department_NC(String nochange) {
        try {
            String sql = "SELECT codice FROM department_nc WHERE gruppo_nc = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, nochange);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            ps.close();
        } catch (SQLException ex) {
            // insertTR("E", "System",
            // Thread.currentThread().getStackTrace()[1].getMethodName() + ": " +
            // ex.getMessage());
        }
        return "01";
    }
}
