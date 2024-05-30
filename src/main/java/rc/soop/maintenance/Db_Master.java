/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import com.google.common.util.concurrent.AtomicDouble;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.stream.Collectors;
import static rc.soop.maintenance.Monitor.log;
import static rc.soop.maintenance.ProceduraDaily.getNC_causal;
import static rc.soop.maintenance.ProceduraDaily.parseDoubleR;
import static rc.soop.maintenance.ProceduraDaily.patternnormdate_filter;
import static rc.soop.maintenance.ProceduraDaily.patternsql;
import static rc.soop.maintenance.ProceduraDaily.subDays;

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
    public PreparedStatement ps = null;

    private static String sanitizeInput(String input) {
        return input.replaceAll("[^a-zA-Z0-9]", "");
    }

    public Db_Master(boolean filiale, String ip) {
        try {
            String sanitizedIp = sanitizeInput(ip); // Sanifica l'input dell'indirizzo IP
            if (filiale) {
                Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
                Properties p = new Properties();
                p.put("user", "root");
                p.put("password", "root");
                p.put("useUnicode", "true");
                p.put("characterEncoding", "UTF-8");
                p.put("useSSL", "false");
                p.put("connectTimeout", "100");
                p.put("useJDBCCompliantTimezoneShift", "true");
                p.put("useLegacyDatetimeCode", "false");
                p.put("serverTimezone", "UTC");
                this.c = DriverManager.getConnection("jdbc:mysql://" + sanitizedIp + ":3306/maccorp", p);
                // Utilizza un PreparedStatement per eseguire le query SQL
                this.ps = this.c.prepareStatement("SELECT * FROM some_table WHERE some_column = ?");
            } else {
                this.c = null;
            }
            ps.close();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            // log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
            this.c = null;
        }
    }

    public Db_Master() {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
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
            String host = rb.getString("db.ip") + "/maccorpita";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
            this.c = null;
        }
    }

    public Db_Master(String hostcentrale) {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
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
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + hostcentrale, p);
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
            this.c = null;
        }
    }

    public Db_Master(boolean cz, boolean uk) {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
            String pwd = rb.getString("db.pwd");
            String host = "";
            if (cz) {
                host = rb.getString("db.ip") + "/maccorpczprod";
            } else if (uk) {
                host = rb.getString("db.ip") + "/maccorpukprod";
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
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
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
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
            }ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
            }ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public ArrayList<String[]> getValuteForMonitor(String filiale) {
        ArrayList<String[]> al = new ArrayList<>();
        String query;
        try {
            if (filiale.equals("---")) {
                query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                        + "FROM valute where fg_valuta_corrente='0' AND filiale <>'000' ";
            } else {
                query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                        + "FROM valute where fg_valuta_corrente='0' AND filiale = ? ";
            }
            query += " ORDER BY filiale, valuta";

            try (PreparedStatement ps = this.c.prepareStatement(query)) {
                if (!filiale.equals("---")) {
                    ps.setString(1, filiale);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String[] var = {
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
                        al.add(var);
                    }
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return al;
    }

    public ArrayList<Branch> list_all_branch() {
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
            }ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
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
            }ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
        // ing = StringUtils.replace(ing, "€", "&#0128;");
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
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return dat;
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
                ps.close();

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

            }

        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public ArrayList<NC_causal> query_nc_causal_filial(String filiale, String status) {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {

            String sql = "SELECT * FROM nc_causali WHERE filiale = '" + filiale + "' ";

            if (status == null || status.equals("...")) {

            } else {
                sql += " AND annullato = '" + status + "' ";
            }
            sql += "ORDER BY gruppo_nc";

            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    public ArrayList<String[]> credit_card_enabled() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT carta_credito,de_carta_credito,fg_annullato FROM carte_credito WHERE fg_annullato = ? AND filiale = ? order by carta_credito";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "0");
            ps.setString(2, getCodLocal(true)[0]);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] o1 = { rs.getString(1), visualizzaStringaMySQL(rs.getString(2)), rs.getString(3) };
                out.add(o1);
            }
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    // public static void main(String[] args) {
    // String[] fil = {"102", "102"};
    // Db_Master db1 = new Db_Master();
    //
    // db1.list_Daily_value(fil, "2019-07-26 00:00", "2019-07-26 23:59", false,
    // false);
    //
    // db1.closeDB();
    //
    // }
    public Daily_value list_Daily_value(String[] fil, String datad1, String datad2, boolean now, boolean uk) {
        if (datad1 != null && datad2 != null) {

            try {

                String valutalocale = get_local_currency()[0];

                Daily_value d = new Daily_value();
                ArrayList<NC_causal> nc_caus = query_nc_causal_filial(fil[0], null);

                double setPurchTotal = 0.0;
                double setPurchComm = 0.0;
                double setPurchGrossTot = 0.0;
                double setPurchSpread = 0.0;
                double setPurchProfit = 0.0;
                double setSalesTotal = 0.0;
                double setSalesComm = 0.0;
                double setSalesGrossTot = 0.0;
                double setSalesSpread = 0.0;
                double setSalesProfit = 0.0;
                double setCashAdNetTot = 0.0;
                double setCashAdComm = 0.0;
                double setCashAdGrossTot = 0.0;
                double setCashAdSpread = 0.0;
                double setCashAdProfit = 0.0;

                double refund = 0.0;
                double refundshow = 0.0;

                // refund
                String sql0 = "SELECT value FROM ch_transaction_refund where status = '1' and method = 'BR' and branch_cod = '"
                        + fil[0] + "'";

                sql0 = sql0 + "AND dt_refund >= '" + datad1 + ":00' ";

                sql0 = sql0 + "AND dt_refund <= '" + datad2 + ":59' ";

                sql0 = sql0 + " ORDER BY dt_refund";

                PreparedStatement ps0 = this.c.prepareStatement(sql0,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs0 = ps0.executeQuery();
                while (rs0.next()) {
                    refund = refund + fd(rs0.getString("value"));
                    refundshow = refundshow + parseDoubleR(rs0.getString("value"));
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
                String sql = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = '" + fil[0]
                        + "' ";

                sql = sql + "AND tr1.data >= '" + datad1 + ":00' ";

                sql = sql + "AND tr1.data <= '" + datad2 + ":59' ";

                sql = sql + " ORDER BY tr1.data";

                PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = ps.executeQuery();

                int setNoTransPurch = 0;
                int setNoTransCC = 0;
                int setNoTransSales = 0;
                @SuppressWarnings("unused")
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
                @SuppressWarnings("unused")
                int ij = 1;
                while (rs.next()) {
                    setTotal++;
                    if (rs.getString("tr1.tipotr").equals("B")) {
                        setNoTransPurch++;

                        String sqlval = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psval = this.c.prepareStatement(sqlval, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        psval.setString(1, rs.getString("cod"));
                        ResultSet rsval = psval.executeQuery();

                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) {// CASH ADVANCE
                                setNoTransCC++;
                                setCashAdNetTot = setCashAdNetTot + fd(rsval.getString("net"));
                                setCashAdComm = setCashAdComm + fd(rsval.getString("tot_com"))
                                        + fd(rsval.getString("roundvalue"));
                                setNoTransPurch--;
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCashAdNtrans());

                                    start++;
                                    dc.setCashAdNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCashAdAmount());
                                    d1 = d1 + fd(rsval.getString("total"));
                                    dc.setCashAdAmount(roundDoubleandFormat(d1, 2));
                                }
                            } else if (rsval.getString("supporto").equals("06")) {// CREDIT CARD

                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getCcNtrans());
                                    start++;
                                    dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getCcAmount());
                                    d1 = d1 + fd(rsval.getString("net"));
                                    dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                    // poamount = poamount + fd(rsval.getString("net"));
                                }

                            } else if (rsval.getString("supporto").equals("07")) {// bancomat

                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getBankNtrans());
                                    start++;
                                    dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getBankAmount());
                                    d1 = d1 + fd(rsval.getString("net"));
                                    dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                    // poamount = poamount + fd(rsval.getString("net"));
                                }

                            } else if (rsval.getString("supporto").equals("08")) {
                                DailyBank dc = DailyBank.get_obj(listdb, rsval.getString("pos"));
                                if (dc != null) {
                                    double start = fd(dc.getNtrans());
                                    start++;
                                    dc.setNtrans(roundDoubleandFormat(start, 0));
                                    double d1 = fd(dc.getAmount());
                                    d1 = d1 + fd(rsval.getString("net"));
                                    dc.setAmount(roundDoubleandFormat(d1, 2));
                                    // poamount = poamount + fd(rsval.getString("net"));
                                }
                            } else {
                                setPurchTotal = setPurchTotal + fd(rsval.getString("net"));
                                setPurchComm = setPurchComm + fd(rsval.getString("tot_com"))
                                        + fd(rsval.getString("roundvalue"));
                            }
                        }
                        psval.close();

                    } else {
                        setNoTransSales++;
                        setSalesTotal = setSalesTotal + fd(rs.getString("tr1.pay"));
                        setSalesGrossTot = setSalesGrossTot + fd(rs.getString("tr1.total"));
                        // 26012018
                        // setSalesTotal = setSalesTotal + fd(rs.getString("tr1.pay"));

                        setSalesComm = setSalesComm + fd(rs.getString("tr1.commission"))
                                + fd(rs.getString("tr1.round"));
                        ij++;
                        setSalesSpread = setSalesSpread + fd(rs.getString("tr1.spread_total"));

                        if (rs.getString("tr1.localfigures").equals("06")) {// CREDIT CARD
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getCcNtrans());
                                start++;
                                dc.setCcNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getCcAmount());
                                d1 = d1 + fd(rs.getString("tr1.pay"));
                                dc.setCcAmount(roundDoubleandFormat(d1, 2));
                                poamount = poamount + fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("tr1.localfigures").equals("07")) {// bancomat

                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getBankNtrans());
                                start++;
                                dc.setBankNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getBankAmount());
                                d1 = d1 + fd(rs.getString("tr1.pay"));
                                dc.setBankAmount(roundDoubleandFormat(d1, 2));
                                poamount = poamount + fd(rs.getString("tr1.pay"));
                            }
                            setTotPos++;
                        } else if (rs.getString("localfigures").equals("08")) {

                            DailyBank dc = DailyBank.get_obj(listdb, rs.getString("tr1.pos"));
                            if (dc != null) {
                                double start = fd(dc.getNtrans());
                                start++;
                                dc.setNtrans(roundDoubleandFormat(start, 0));
                                double d1 = fd(dc.getAmount());
                                d1 = d1 + fd(rs.getString("tr1.pay"));
                                dc.setAmount(roundDoubleandFormat(d1, 2));
                                poamount = poamount + fd(rs.getString("tr1.pay"));
                            }

                            setTotAcc++;
                        }

                        String sqlval = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psval = this.c.prepareStatement(sqlval, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        psval.setString(1, rs.getString("cod"));
                        ResultSet rsval = psval.executeQuery();

                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) { // CASH ADVANCE
                                setCashAdNetTot += fd(rsval.getString("total"));
                                setCashAdComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));

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
                        psval.close();

                    }

                }ps.close();

                setPurchGrossTot = setPurchTotal + setPurchComm;
                // setSalesGrossTot = setSalesTotal - setSalesComm;
                setCashAdGrossTot = setCashAdNetTot + setCashAdComm;

                setPurchProfit = setPurchComm;
                setSalesProfit = setSalesComm + setSalesSpread;
                setCashAdProfit = setCashAdComm;

                // NO CHANGE
                String sql1 = "SELECT causale_nc,supporto,total,pos,fg_inout FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                        + fil[0] + "' ";

                // String sql1 = "SELECT * FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                // + fil[0] + "' ";
                sql1 = sql1 + "AND data >= '" + datad1 + ":00' ";

                sql1 = sql1 + "AND data <= '" + datad2 + ":59' ";

                if (uk) {
                    sql1 = sql1 + "AND gruppo_nc <> 'DEPOS' ";
                }

                sql1 = sql1 + " ORDER BY data";

                PreparedStatement ps1 = this.c.prepareStatement(sql1,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs1 = ps1.executeQuery();
                double totalnotesnochange = 0.00;

                ArrayList<String[]> listkind = nc_kind_order();
                // ArrayList<String[]> list_nc_descr = list_nc_descr();
                ArrayList<DailyKind> dklist = new ArrayList<>();
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
                                    // System.out.println("macmonitor.Db_Master.list_Daily_value(----) " +
                                    // nc2.getNc_de() + fd(rs1.getString("total")));
                                    if (!nc2.getNc_de().equals("14")) {
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
                                        // System.out.println("macmonitor.Db_Master.list_Daily_value(----) " + supporto
                                        // + nc2.getNc_de() + fd(rs1.getString("total")));
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
                    } else {
                        dklist.add(dk);
                        totalnotesnochange = totalnotesnochange + setToLocalCurr + setFromLocalCurr;
                    }
                }

                d.setDati(dklist);

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

                if (uk) {
                    sql2 = sql2 + "AND cod_dest <> '000' ";
                }

                sql2 = sql2 + " ORDER BY dt_it";
                // System.out.println(sql2);
                PreparedStatement ps2 = this.c.prepareStatement(sql2,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs2 = ps2.executeQuery();
                while (rs2.next()) {

                    PreparedStatement pstmt = this.c.prepareStatement(
                            "SELECT * FROM et_change_valori WHERE cod = ?",
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    pstmt.setString(1, rs2.getString("cod"));
                    ResultSet rs2val = pstmt.executeQuery();

                    if (rs2.getString("fg_tofrom").equals("T")) { // sales

                        if (rs2.getString("fg_brba").equals("BA")) { // BANK
                            while (rs2val.next()) {
                                setBaSalesSpread = setBaSalesSpread + fd(rs2val.getString("ip_spread"));
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        setBaSalesTransfNotes = setBaSalesTransfNotes
                                                + fd(rs2val.getString("ip_total"));
                                    } else {
                                        setBaSalesTotal = setBaSalesTotal + fd(rs2val.getString("ip_total"));
                                    }
                                } else if (rs2val.getString("kind").equals("02")
                                        || rs2val.getString("kind").equals("03")) {
                                    setBaSalesTotal = setBaSalesTotal + fd(rs2val.getString("ip_total"));
                                } else if (rs2val.getString("kind").equals("04")) {
                                    setBaSalesCreditCard = setBaSalesCreditCard + fd(rs2val.getString("ip_total"));
                                } else {
                                    setBaSalesTransfOther = setBaSalesTransfOther + fd(rs2val.getString("ip_total"));
                                }
                            }

                        } else {// BRANCH
                            while (rs2val.next()) {
                                setBraSalesSpread = setBraSalesSpread + fd(rs2val.getString("ip_spread"));

                                if (rs2val.getString("currency").equals(valutalocale)
                                        && rs2val.getString("kind").equals("01")) {
                                    // System.out.println("macmonitor.Db_Master.list_Daily_value()
                                    // "+rs2val.getString("ip_total"));
                                    setBraSalesLocalCurr = setBraSalesLocalCurr + fd(rs2val.getString("ip_total"));
                                } else {
                                    setBraSalesTotal = setBraSalesTotal + fd(rs2val.getString("ip_total"));
                                }
                            }

                        }
                    } else if (rs2.getString("fg_brba").equals("BA")) { // BANK
                        while (rs2val.next()) {

                            setBaPurchSpread = setBaPurchSpread + fd(rs2val.getString("ip_spread"));

                            if (rs2val.getString("kind").equals("01")) {
                                if (rs2val.getString("currency").equals(valutalocale)) {
                                    setBaPurchTransfNotes = setBaPurchTransfNotes + fd(rs2val.getString("ip_total"));
                                } else {
                                    setBaPurchTotal = setBaPurchTotal + fd(rs2val.getString("ip_total"));
                                }
                            } else if (rs2val.getString("kind").equals("02") || rs2val.getString("kind").equals("03")) {
                                setBaPurchTotal = setBaPurchTotal + fd(rs2val.getString("ip_total"));
                            } else if (rs2val.getString("kind").equals("04")) {
                                setBaPurchCreditCard = setBaPurchCreditCard + fd(rs2val.getString("ip_total"));
                            } else {
                                setBaPurchTransfOther = setBaPurchTransfOther + fd(rs2val.getString("ip_total"));
                            }

                        }
                    } else {// BRANCH
                        while (rs2val.next()) {

                            setBraPurchSpread = setBraPurchSpread + fd(rs2val.getString("ip_spread"));
                            if (rs2val.getString("currency").equals(valutalocale)
                                    && rs2val.getString("kind").equals("01")) {
                                setBraPurchLocalCurr = setBraPurchLocalCurr + fd(rs2val.getString("ip_total"));
                            } else {
                                setBraPurchTotal = setBraPurchTotal + fd(rs2val.getString("ip_total"));
                            }
                        }
                        
                        rs2.close();
                        pstmt.close();
                        rs2.close();

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
                double setGrossProfit = setPurchComm + setSalesComm + setCashAdComm + setSalesSpread + setBaSalesSpread
                        + setBraSalesSpread;
                double setLastCashOnPrem = 0.0;
                double setCashOnPrem = 0.0;
                double setFx = 0.0;

                //// //cambiare
                //// ArrayList<Till> array_till = list_till_daily_report("O",
                //// datad1.substring(0, 10), fil[0], "MIN");
                ////
                //// for (int i = 0; i < array_till.size(); i++) {
                //// ResultSet rsvalue = this.c.createStatement().executeQuery("SELECT value_op
                //// FROM oc_change "
                //// + "WHERE cod_oc = '" + array_till.get(i).getId_opcl() + "' AND valuta =
                //// 'EUR' AND kind = '01'");
                //// while (rsvalue.next()) {
                //// setLastCashOnPrem = setLastCashOnPrem + fd(rsvalue.getString("value_op"));
                //// }
                //// }
                // MODIFICA LASTCASH
                ArrayList<Office_sp> li = list_query_officesp2(fil[0], subDays(datad1.substring(0, 10), patternsql, 1));
                if (!li.isEmpty()) {

                    Office_sp o = li.get(0);
                    d.setOfficesp(o.getCodice());

                    setLastCashOnPrem = fd(o.getTotal_cod());
                    // ArrayList<OfficeStockPrice_value> dati =
                    // list_OfficeStockPrice_value(o.getCodice(), fil[0]);
                    // for (int x = 0; x < dati.size(); x++) {
                    // OfficeStockPrice_value t = dati.get(x);
                    // if (t.getCurrency().equals(valutalocale) && t.getSupporto().contains("01")) {
                    // setLastCashOnPrem = setLastCashOnPrem + fd(t.getQtaSenzaFormattazione());
                    //// setLastCashOnPrem = setLastCashOnPrem +
                    // fd(t.getControvaloreSenzaFormattazione());
                    // }
                    // }
                } else {
                    d.setOfficesp(null);
                }

                Office_sp o = list_query_last_officesp(fil[0], datad2);
                if (o != null) {
                    double[] d1 = list_dettagliotransazioni(fil, o.getData(), datad2, valutalocale);
                    setCashOnPrem = fd(o.getTotal_cod()) + d1[0];
                    setFx = fd(o.getTotal_fx()) + d1[1];
                    // System.out.println("macmonitor.Db_Master.list_Daily_value()
                    // "+fd(o.getTotal_fx()));
                    // System.out.println("macmonitor.Db_Master.list_Daily_value() "+d1[1]);
                }

                // ArrayList<OfficeStockPrice_value> dati =
                // list_OfficeStockPrice_value(o.getCodice(), fil[0]);
                //
                // for (int x = 0; x < dati.size(); x++) {
                // OfficeStockPrice_value t = dati.get(x);
                // if (t.getCurrency().equals(valutalocale) && t.getSupporto().contains("01")) {
                // setCashOnPrem = setCashOnPrem + fd(t.getControvaloreSenzaFormattazione());
                // }else{
                // setFx = setFx + fd(t.getControvaloreSenzaFormattazione());
                // }
                // }
                boolean dividi = true;

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

                // System.out.println("macmonitor.Db_Master.list_Daily_value(+) " +
                // setSalesTotal);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " +
                // setPurchTotal);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(+) " +
                // setBaPurchTransfNotes);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " +
                // setBaSalesTransfNotes);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(+) " +
                // setBraPurchLocalCurr);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " +
                // setBraSalesLocalCurr);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(+) " +
                // totalnotesnochange);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(+) " +
                // setLastCashOnPrem);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " + refund);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " +
                // setCashAdNetTot);
                // System.out.println("macmonitor.Db_Master.list_Daily_value(-) " + poamount);
                // System.out.println("1a " + setSalesTotal);
                // System.out.println("1b " + setPurchTotal);
                // System.out.println("2a " + setBaPurchTransfNotes);
                // System.out.println("2b " + setBaSalesTransfNotes);
                // System.out.println("3a " + setBraPurchLocalCurr);
                // System.out.println("3b " + setBraSalesLocalCurr);
                // System.out.println("4 " + totalnotesnochange);
                // System.out.println("5 " + setLastCashOnPrem);
                // System.out.println("6 " + refund);
                // System.out.println("7 " + setCashAdNetTot);
                // System.out.println("8 " + poamount);
                //// - refund - setCashAdNetTot - poamount;
                // double setCashOnPremError = setCashOnPrem - setCashOnPremFromTrans;
                double setFxClosureErrorDeclared = 0.0;
                double setCashOnPremError = 0.0;
                String sql10  ="SELECT * FROM oc_errors where filiale = '" + fil[0] + "' AND cod IN "
                + "(SELECT cod FROM oc_lista WHERE data >= '" + datad1 + ":00' AND data <= '" + datad2
                + ":59') "
                + "AND tipo='CH' AND (kind='01' OR kind='02' OR kind='03')";
                PreparedStatement ps10 = this.c.prepareStatement(sql10,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs10 = ps10.executeQuery();
                while (rs10.next()) {
                    if (rs10.getString("valuta").equals(valutalocale) && rs10.getString("kind").equals("01")) {
                        // calcolare
                        double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                        setCashOnPremError = setCashOnPremError + eurerr;
                        // System.out.println("EUR " + eurerr);
                    } else {
                        double fxerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                        // System.out.println("FX " + getControvalore(fxerr, fd(rs10.getString("rate")),
                        // dividi));
                        setFxClosureErrorDeclared = setFxClosureErrorDeclared
                                + getControvalore(fxerr, fd(rs10.getString("rate")), dividi);
                        // setFxClosureErrorDeclared = setFxClosureErrorDeclared +
                        // getControvalore(fd(rs10.getString("total_diff")), fd(rs10.getString("rate")),
                        // dividi);
                    }
                }
                ps10.close();

                setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;
                // System.out.println("macmonitor.Db_Master.list_Daily_value() " +
                // setCashOnPrem);
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
            } catch (SQLException |

                    NumberFormatException ex) {
                log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
            }
        }
        return null;
    }

    public static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    public static double fd(String si_t_old) {
        double d1 = 0.0D;
        si_t_old = si_t_old.replace(",", "").trim();
        try {
            d1 = Double.parseDouble(si_t_old);
        } catch (Exception ex) {
            d1 = 0.0D;
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return d1;
    }

    public Office_sp query_officesp(String codice) {
        String sql = "SELECT * FROM office_sp WHERE codice = ?";
        try (PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, codice);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Office_sp(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getString(6),
                            rs.getString(7),
                            rs.getString(8),
                            rs.getString(9),
                            rs.getString(10));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public boolean updateCOP(String codice, String cop) {
        try {
            String upd1 = "UPDATE office_sp SET total_cod = ? WHERE codice = ?";
            String upd2 = "UPDATE office_sp_valori SET quantity = ?, controv = ? WHERE cod = ? AND currency = ? AND kind = '01'";

            try (PreparedStatement ps1 = this.c.prepareStatement(upd1);
                    PreparedStatement ps2 = this.c.prepareStatement(upd2)) {

                // Imposta i parametri per la prima query
                ps1.setString(1, cop);
                ps1.setString(2, codice);
                // Esegui l'aggiornamento della tabella office_sp
                ps1.executeUpdate();

                // Imposta i parametri per la seconda query
                ps2.setString(1, cop);
                ps2.setString(2, cop);
                ps2.setString(3, codice);
                ps2.setString(4, get_local_currency()[0]);
                // Esegui l'aggiornamento della tabella office_sp_valori
                ps2.executeUpdate();

                return true;
            }
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return false;
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
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return new String[] { "EUR", "242" };
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
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    public Office_sp list_query_last_officesp(String filiale, String data) {
        try {
            String sql = "SELECT * FROM office_sp WHERE filiale = ? AND data < ? ORDER BY data DESC LIMIT 1";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                // Imposta i parametri per la query preparata
                ps.setString(1, filiale);
                ps.setString(2, data);

                // Esegui la query
                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    return new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                            rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                            rs.getString(10));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public double[] list_dettagliotransazioni(String[] fil, String datad1, String datad2, String valutalocale) {
        double[] d = new double[2];
        double lo = 0.00;
        double fx = 0.00;
        try {
            // Query per la selezione dei rimborsi
            String sql0 = "SELECT * FROM ch_transaction_refund WHERE status = '1' AND method = 'BR' AND branch_cod = ? AND dt_refund >= ? AND dt_refund <= ?";
            try (PreparedStatement ps0 = this.c.prepareStatement(sql0)) {
                ps0.setString(1, fil[0]);
                ps0.setString(2, datad1);
                ps0.setString(3, datad2);
                ResultSet rs0 = ps0.executeQuery();
                while (rs0.next()) {
                    lo -= fd(rs0.getString("value"));
                }
            }

            // Query per la selezione delle transazioni
            String sql = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg = '0' AND tr1.filiale = ? AND tr1.data >= ? AND tr1.data <= ?";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, fil[0]);
                ps.setString(2, datad1);
                ps.setString(3, datad2);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    if (rs.getString("tr1.tipotr").equals("B")) {
                        String queryValori = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                        try (PreparedStatement psValori = this.c.prepareStatement(queryValori)) {
                            psValori.setString(1, rs.getString("cod"));
                            ResultSet rsval = psValori.executeQuery();
                            while (rsval.next()) {
                                if (rsval.getString("supporto").equals("01") || rsval.getString("supporto").equals("02")
                                        || rsval.getString("supporto").equals("03")) {
                                    if (rsval.getString("supporto").equals("01")
                                            && rsval.getString("valuta").equals(valutalocale)) {
                                        lo += fd(rsval.getString("total"));
                                    } else {
                                        fx += fd(rsval.getString("total"));
                                    }
                                }
                                lo -= fd(rsval.getString("net"));
                            }
                        }
                    } else {
                        String queryValori = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                        try (PreparedStatement psValori = this.c.prepareStatement(queryValori)) {
                            psValori.setString(1, rs.getString("cod"));
                            ResultSet rsval = psValori.executeQuery();
                            while (rsval.next()) {
                                if (rsval.getString("valuta").equals(valutalocale)) {
                                    lo -= fd(rsval.getString("total"));
                                } else {
                                    fx -= fd(rsval.getString("total"));
                                }
                            }
                        }
                        if (rs.getString("tr1.localfigures").equals("01")) {
                            lo += fd(rs.getString("pay"));
                        }
                    }
                }
            }

            // Query per le transazioni NO CHANGE
            String sql1 = "SELECT supporto,fg_inout,total FROM nc_transaction WHERE del_fg='0' AND filiale = ? AND data >= ? AND data <= ?";
            try (PreparedStatement ps1 = this.c.prepareStatement(sql1)) {
                ps1.setString(1, fil[0]);
                ps1.setString(2, datad1);
                ps1.setString(3, datad2);
                ResultSet rs1 = ps1.executeQuery();
                while (rs1.next()) {
                    String supporto = rs1.getString("supporto");
                    if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {
                        if (supporto.equals("01")) {
                            lo += fd(rs1.getString("total"));
                        }
                    } else if (supporto.equals("01")) {
                        lo -= fd(rs1.getString("total"));
                    }
                }
            }

            // Query per le transazioni EXTERNAL TRANSFER
            String sql2 = "SELECT * FROM et_change WHERE fg_annullato = '0' AND filiale = ? AND dt_it >= ? AND dt_it <= ?";
            try (PreparedStatement ps2 = this.c.prepareStatement(sql2)) {
                ps2.setString(1, fil[0]);
                ps2.setString(2, datad1);
                ps2.setString(3, datad2);
                ResultSet rs2 = ps2.executeQuery();
                while (rs2.next()) {
                    String queryValori = "SELECT * FROM et_change_valori WHERE cod = ?";
                    try (PreparedStatement psValori = this.c.prepareStatement(queryValori)) {
                        psValori.setString(1, rs2.getString("cod"));
                        ResultSet rs2val = psValori.executeQuery();
                        if (rs2.getString("fg_tofrom").equals("T")) { // sales
                            while (rs2val.next()) {
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        lo -= fd(rs2val.getString("ip_total"));
                                    } else {
                                        fx -= fd(rs2val.getString("ip_total"));
                                    }
                                } else {
                                    fx -= fd(rs2val.getString("ip_total"));
                                }
                            }
                        } else { // buy
                            while (rs2val.next()) {
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        lo += fd(rs2val.getString("ip_total"));
                                    } else {
                                        fx += fd(rs2val.getString("ip_total"));
                                    }
                                } else {
                                    fx += fd(rs2val.getString("ip_total"));
                                }
                            }
                        }
                    }
                }
            }

        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        d[0] = lo;
        d[1] = fx;
        return d;
    }

    public ArrayList<Office_sp> list_query_officesp2(String filiale, String data) {
        ArrayList<Office_sp> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM office_sp WHERE filiale = ? AND data <= ? ORDER BY data DESC";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, filiale);
                ps.setString(2, data + " 23:59:59");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    out.add(new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                            rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                            rs.getString(10)));
                }
            }
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    public ArrayList<String> list_cod_branch_enabled() {
        ArrayList<String> out = new ArrayList<>();
        try {
            String sql = "SELECT cod FROM branch WHERE fg_annullato = ? AND filiale = ? ORDER BY cod";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, "0");
                ps.setString(2, getCodLocal(true)[0]);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    out.add(sanitizeInput(rs.getString("cod")));
                }
            }
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    public List<IpFiliale> getIpFiliale() {
        List<IpFiliale> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale, ip FROM dbfiliali";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    out.add(new IpFiliale(rs.getString(1), rs.getString(2)));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public Daily_value_proc list_Daily_value(String[] fil, String datad1, String datad2, String valutalocale)
            throws SQLException {

        if (datad1 != null && datad2 != null) {
            try {
                Daily_value_proc d = new Daily_value_proc();
                double setPurchTotal = 0.0;
                double setSalesTotal = 0.0;
                double setCashAdNetTot = 0.0;
                ArrayList<NC_causal> nc_caus = query_nc_causal_filial(fil[0], null);
                double refund = 0.0;

                // refund
                String sql0 = "SELECT value FROM ch_transaction_refund WHERE status = '1' AND method = 'BR' AND branch_cod = ? AND dt_refund >= ? AND dt_refund <= ?";
                try (PreparedStatement ps0 = this.c.prepareStatement(sql0)) {
                    ps0.setString(1, fil[0]);
                    ps0.setString(2, datad1 + ":00");
                    ps0.setString(3, datad2 + ":59");
                    ResultSet rs0 = ps0.executeQuery();
                    while (rs0.next()) {
                        refund += fd(rs0.getString("value"));
                    }
                }

                // TRANSACTION
                String sql = "SELECT cod, pay, tipotr, localfigures FROM ch_transaction WHERE del_fg='0' AND filiale = ? AND data >= ? AND data <= ?";
                try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                    ps.setString(1, fil[0]);
                    ps.setString(2, datad1 + ":00");
                    ps.setString(3, datad2 + ":59");
                    ResultSet rs = ps.executeQuery();
                    double poamount = 0.00;

                    while (rs.next()) {
                        if (rs.getString("tipotr").equals("B")) {
                            String sqlVal = "SELECT supporto, net FROM ch_transaction_valori WHERE cod_tr = ?";
                            try (PreparedStatement psVal = this.c.prepareStatement(sqlVal,
                                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                                psVal.setString(1, rs.getString("cod"));
                                ResultSet rsval = psVal.executeQuery();

                                while (rsval.next()) {
                                    String supporto = rsval.getString("supporto");
                                    double netValue = fd(rsval.getString("net"));

                                    switch (supporto) {
                                        case "04": // CASH ADVANCE
                                            setCashAdNetTot += netValue;
                                            break;
                                        case "06": // CREDIT CARD
                                        case "07": // bancomat
                                        case "08":
                                            poamount += netValue;
                                            break;
                                        default:
                                            setPurchTotal += netValue;
                                            break;
                                    }
                                }
                            }

                        } else {
                            double payValue = fd(rs.getString("pay"));
                            setSalesTotal += payValue;

                            String localfigures = rs.getString("localfigures");
                            switch (localfigures) {
                                case "06": // CREDIT CARD
                                case "07": // bancomat
                                case "08":
                                    poamount += payValue;
                                    break;
                                default:
                                    break;
                            }
                        }
                    }

                    // NO CHANGE
                    String sql1 = "SELECT causale_nc,supporto,total,pos,fg_inout FROM nc_transaction WHERE del_fg='0' AND filiale = '"
                            + fil[0] + "' ";
                    sql1 = sql1 + "AND data >= '" + datad1 + ":00' ";
                    sql1 = sql1 + "AND data <= '" + datad2 + ":59' ";
                    sql1 = sql1 + " ORDER BY data";
                    PreparedStatement ps1 = this.c.prepareStatement(sql1,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs1 = ps1.executeQuery();
                    double totalnotesnochange = 0.00;
                    double setToLocalCurr = 0.00;
                    double setFromLocalCurr = 0.00;
                    while (rs1.next()) {
                        NC_causal nc2 = getNC_causal(nc_caus, rs1.getString("causale_nc"));
                        if (nc2 != null) {
                            if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {
                                if (rs1.getString("supporto").equals("01")) {
                                    setFromLocalCurr = setFromLocalCurr + fd(rs1.getString("total"));
                                }
                            } else {
                                if (!nc2.getNc_de().equals("14")) {
                                    if (rs1.getString("supporto").equals("01")
                                            || rs1.getString("supporto").equals("...")) {
                                        setToLocalCurr = setToLocalCurr + fd(rs1.getString("total"));
                                    }
                                }
                            }
                        }
                    }
                    ps1.close();
                    totalnotesnochange = setToLocalCurr + setFromLocalCurr;
                    double setBaPurchTransfNotes = 0.00;
                    double setBaSalesTransfNotes = 0.00;
                    double setBraPurchLocalCurr = 0.00;
                    double setBraSalesLocalCurr = 0.00;
                    // EXTERNAL TRANSFER
                    String sql2 = "SELECT cod, fg_tofrom, fg_brba FROM et_change WHERE fg_annullato = '0' "
                            + "AND filiale = ? AND dt_it >= ? AND dt_it <= ? ORDER BY dt_it";
                    PreparedStatement ps2 = this.c.prepareStatement(sql2);
                    ps2.setString(1, fil[0]);
                    ps2.setString(2, datad1 + ":00");
                    ps2.setString(3, datad2 + ":59");
                    ResultSet rs2 = ps2.executeQuery();

                    while (rs2.next()) {
                        String sql2val = "SELECT kind, currency, ip_total FROM et_change_valori WHERE cod = ?";
                        PreparedStatement ps2val = this.c.prepareStatement(sql2val);
                        ps2val.setString(1, rs2.getString("cod"));
                        ResultSet rs2val = ps2val.executeQuery();

                        if (rs2.getString("fg_tofrom").equals("T")) { // sales
                            if (rs2.getString("fg_brba").equals("BA")) { // BANK
                                while (rs2val.next()) {
                                    if (rs2val.getString("kind").equals("01")) {
                                        if (rs2val.getString("currency").equals(valutalocale)) {
                                            setBaSalesTransfNotes += fd(rs2val.getString("ip_total"));
                                        }
                                    }
                                }
                                ps2.close();
                            } else { // BRANCH
                                while (rs2val.next()) {
                                    if (rs2val.getString("currency").equals(valutalocale)
                                            && rs2val.getString("kind").equals("01")) {
                                        setBraSalesLocalCurr += fd(rs2val.getString("ip_total"));
                                    }
                                }
                                ps2val.close();
                            }
                        } else if (rs2.getString("fg_brba").equals("BA")) { // BANK
                            while (rs2val.next()) {
                                if (rs2val.getString("kind").equals("01")) {
                                    if (rs2val.getString("currency").equals(valutalocale)) {
                                        setBaPurchTransfNotes += fd(rs2val.getString("ip_total"));
                                    }
                                }
                            }ps2val.close();
                        } else { // BRANCH
                            while (rs2val.next()) {
                                if (rs2val.getString("currency").equals(valutalocale)
                                        && rs2val.getString("kind").equals("01")) {
                                    setBraPurchLocalCurr += fd(rs2val.getString("ip_total"));
                                }
                            }
                        }
                    }
                    ps2.close();
                    

                    double setLastCashOnPrem = 0.0;
                    ArrayList<Office_sp> li = list_query_officesp2(fil[0],
                            subDays(datad1.substring(0, 10), patternsql, 1));
                    if (!li.isEmpty()) {
                        Office_sp o = li.get(0);
                        d.setOfficesp(o.getCodice());
                        setLastCashOnPrem = fd(o.getTotal_cod());
                    }
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

                    double setCashOnPremError = 0.0;
                    String sql10 = "SELECT * FROM oc_errors where filiale = '" + fil[0] + "' AND cod IN "
                    + "(SELECT cod FROM oc_lista WHERE data >= '" + datad1 + ":00' AND data <= '"
                    + datad2
                    + ":59') "
                    + "AND tipo='CH' AND (kind='01' OR kind='02' OR kind='03')";

                    PreparedStatement ps10 = this.c.prepareStatement(sql10,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs10 = ps10.executeQuery();
                            
                    while (rs10.next()) {
                        if (rs10.getString("valuta").equals(valutalocale) && rs10.getString("kind").equals("01")) {
                            double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                            setCashOnPremError = setCashOnPremError + eurerr;
                        }
                        ps10.close();
                    }
                    double setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;
                    d.setCashOnPrem(roundDoubleandFormat(setCashOnPrem, 2));
                    d.setLastCashOnPrem(roundDoubleandFormat(setLastCashOnPrem, 2));
                    return d;
                } catch (SQLException | NumberFormatException ex) {
                    log.log(Level.SEVERE, "{0}: {1}",
                            new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
                }
                return null;
            } catch (SQLException | NumberFormatException ex) {
                log.log(Level.SEVERE, "{0}: {1}",
                        new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
            }
        }

        return null;

    }

    public Daily_value_proc list_Daily_value_NEW(String[] fil, String datad1, String datad2, String valutalocale) {
        if (datad1 != null && datad2 != null) {
            try {
                ArrayList<NC_causal> nc_caus = query_nc_causal_filial(fil[0], null);

                Daily_value_proc d = new Daily_value_proc();
                double setPurchTotal = 0.0;
                double setSalesTotal = 0.0;
                double setCashAdNetTot = 0.0;
                double refund = 0.0;
                @SuppressWarnings("unused")
                double setToLocalCurr = 0.0;
                @SuppressWarnings("unused")
                double setFromLocalCurr = 0.0;
                double setBaPurchTransfNotes = 0.0;
                double setBaSalesTransfNotes = 0.0;
                double setBraPurchLocalCurr = 0.0;
                double setBraSalesLocalCurr = 0.0;

                // Prepare the SQL statements with parameter placeholders
                String sql0 = "SELECT value FROM ch_transaction_refund WHERE status = '1' AND method = 'BR' AND branch_cod = ? AND dt_refund BETWEEN ? AND ?";
                String sql = "SELECT tr1.tipotr, cod, tr1.pay, tr1.localfigures, tr1.pos FROM ch_transaction tr1 WHERE tr1.del_fg = '0' AND tr1.filiale = ? AND tr1.data BETWEEN ? AND ?";
                String sql1 = "SELECT causale_nc, supporto, total, pos, fg_inout, quantita FROM nc_transaction WHERE del_fg = '0' AND filiale = ? AND data BETWEEN ? AND ? AND (supporto = '01' OR supporto = '02')";
                String sql2 = "SELECT cod, fg_tofrom, fg_brba FROM et_change WHERE fg_annullato = '0' AND filiale = ? AND dt_it BETWEEN ? AND ?";
                String sql10 = "SELECT total_user, total_system FROM oc_errors WHERE filiale = ? AND cod IN (SELECT cod FROM oc_lista WHERE data BETWEEN ? AND ? AND errors = 'Y') AND tipo = 'CH' AND kind = '01' AND valuta = ?";

                // Execute the refund query
                try (PreparedStatement ps0 = this.c.prepareStatement(sql0)) {
                    ps0.setString(1, fil[0]);
                    ps0.setString(2, datad1 + ":00");
                    ps0.setString(3, datad2 + ":59");
                    try (ResultSet rs0 = ps0.executeQuery()) {
                        while (rs0.next()) {
                            refund += fd(rs0.getString("value"));
                        }
                    }
                }

                // Execute the transaction query
                try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                    ps.setString(1, fil[0]);
                    ps.setString(2, datad1 + ":00");
                    ps.setString(3, datad2 + ":59");
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            refund += fd(rs.getString("value"));
                        }
                    }
                }

                // Execute the transaction query
                try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                    ps.setString(1, fil[0]);
                    ps.setString(2, datad1 + ":00");
                    ps.setString(3, datad2 + ":59");
                    try (ResultSet rs = ps.executeQuery()) {
                        @SuppressWarnings("unused")
                        double poamount = 0.00;
                        ArrayList<String[]> cc = credit_card_enabled();
                        ArrayList<String[]> bc = list_bankAccount();
                        ArrayList<DailyCOP> dclist = new ArrayList<>();
                        for (String[] c : cc) {
                            DailyCOP dc = new DailyCOP(c[1], c[0]);
                            dclist.add(dc);
                        }
                        ArrayList<DailyBank> listdb = new ArrayList<>();
                        for (String[] b : bc) {
                            DailyBank dc = new DailyBank(b[1], b[0]);
                            listdb.add(dc);
                        }
                        while (rs.next()) {
                            if ("B".equals(rs.getString("tr1.tipotr"))) {
                                try (PreparedStatement psVal = this.c.prepareStatement(
                                        "SELECT supporto, net, pos FROM ch_transaction_valori WHERE cod_tr = ?")) {
                                    psVal.setString(1, rs.getString("cod"));
                                    try (ResultSet rsVal = psVal.executeQuery()) {
                                        while (rsVal.next()) {
                                            switch (rsVal.getString("supporto")) {
                                                case "04": // CASH ADVANCE
                                                    setCashAdNetTot += fd(rsVal.getString("net"));
                                                    break;
                                                case "06": // CREDIT CARD
                                                case "07": // BANCOMAT
                                                    DailyCOP dc = DailyCOP.get_obj(dclist, rsVal.getString("pos"));
                                                    if (dc != null) {
                                                        poamount += fd(rsVal.getString("net"));
                                                    }
                                                    break;
                                                case "08": // BANK ACCOUNT
                                                    DailyBank db = DailyBank.get_obj(listdb, rsVal.getString("pos"));
                                                    if (db != null) {
                                                        poamount += fd(rsVal.getString("net"));
                                                    }
                                                    break;
                                                default:
                                                    setPurchTotal += fd(rsVal.getString("net"));
                                                    break;
                                            }
                                        }
                                    }
                                }
                            } else {
                                setSalesTotal += fd(rs.getString("tr1.pay"));
                                if ("06".equals(rs.getString("tr1.localfigures"))
                                        || "07".equals(rs.getString("tr1.localfigures"))) {
                                    DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                                    if (dc != null) {
                                        poamount += fd(rs.getString("tr1.pay"));
                                    }
                                } else if ("08".equals(rs.getString("localfigures"))) {
                                    DailyBank db = DailyBank.get_obj(listdb, rs.getString("tr1.pos"));
                                    if (db != null) {
                                        poamount += fd(rs.getString("tr1.pay"));
                                    }
                                }
                            }
                        }ps.close();
                    }
                }
                // Execute the no change query
                try (PreparedStatement ps1 = this.c.prepareStatement(sql1)) {
                    ps1.setString(1, fil[0]);
                    ps1.setString(2, datad1 + ":00");
                    ps1.setString(3, datad2 + ":59");
                    try (ResultSet rs1 = ps1.executeQuery()) {
                        while (rs1.next()) {
                            NC_causal nc2 = getNC_causal(nc_caus, rs1.getString("causale_nc"));
                            if (nc2 == null) {
                                continue;
                            }
                            if ("1".equals(rs1.getString("fg_inout")) || "3".equals(rs1.getString("fg_inout"))) {
                                setFromLocalCurr += fd(rs1.getString("total"));
                            } else {
                                if (!"14".equals(nc2.getNc_de())) { // Only purchases are not considered
                                    setToLocalCurr += fd(rs1.getString("total"));
                                }
                            }
                        }
                    }ps1.close();
                }

                // Execute the external transfer query
                try (PreparedStatement ps2 = this.c.prepareStatement(sql2)) {
                    ps2.setString(1, fil[0]);
                    ps2.setString(2, datad1 + ":00");
                    ps2.setString(3, datad2 + ":59");
                    try (ResultSet rs2 = ps2.executeQuery()) {
                        while (rs2.next()) {
                            try (PreparedStatement ps2Val = this.c.prepareStatement(
                                    "SELECT ip_total FROM et_change_valori WHERE cod = ? AND kind = '01' AND currency = ?")) {
                                ps2Val.setString(1, rs2.getString("cod"));
                                ps2Val.setString(2, valutalocale);
                                try (ResultSet rs2Val = ps2Val.executeQuery()) {
                                    if ("T".equals(rs2.getString("fg_tofrom"))) { // Sales
                                        if ("BA".equals(rs2.getString("fg_brba"))) { // Bank
                                            while (rs2Val.next()) {
                                                setBaSalesTransfNotes += fd(rs2Val.getString("ip_total"));
                                            }
                                        } else { // Branch
                                            while (rs2Val.next()) {
                                                setBraSalesLocalCurr += fd(rs2Val.getString("ip_total"));
                                            }
                                        }
                                    } else if ("BA".equals(rs2.getString("fg_brba"))) { // Bank
                                        while (rs2Val.next()) {
                                            setBaPurchTransfNotes += fd(rs2Val.getString("ip_total"));
                                        }
                                    } else { // Branch
                                        while (rs2Val.next()) {
                                            setBraPurchLocalCurr += fd(rs2Val.getString("ip_total"));
                                        }
                                    }
                                }
                            }
                        }
                    }ps2.close();
                }

                double setLastCashOnPrem = 0.0;
                ArrayList<Office_sp> li = list_query_officesp2(fil[0], subDays(datad1.substring(0, 10), patternsql, 1));

                if (!li.isEmpty()) {
                    Office_sp o = li.get(0);
                    d.setOfficesp(o.getCodice());
                    setLastCashOnPrem = fd(o.getTotal_cod());
                } else {
                    d.setOfficesp(null);
                }

                double setCashOnPremFromTrans = setSalesTotal // Sell
                        - setPurchTotal // Buy
                        + setBaPurchTransfNotes // Bank
                        - setBaSalesTransfNotes // Bank
                        + setBraPurchLocalCurr // Branch
                        - setBraSalesLocalCurr // Branch
                        + setLastCashOnPrem
                        - refund
                        - setCashAdNetTot;
                double setCashOnPremError = 0.0;
                try (PreparedStatement ps10 = this.c.prepareStatement(sql10)) {
                    ps10.setString(1, fil[0]);
                    ps10.setString(2, datad1 + ":00");
                    ps10.setString(3, datad2 + ":59");
                    ps10.setString(4, valutalocale);
                    try (ResultSet rs10 = ps10.executeQuery()) {
                        while (rs10.next()) {
                            double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                            setCashOnPremError += eurerr;
                        }
                    }
                }
                double setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;
                d.setLastCashOnPrem(roundDoubleandFormat(setLastCashOnPrem, 2));
                d.setCashOnPrem(roundDoubleandFormat(setCashOnPrem, 2));
                return d;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public ArrayList<BranchStockInquiry_value> list_BranchStockInquiry_value(String[] filiale, String datad1,
            String tipo) {
        ArrayList<BranchStockInquiry_value> out = new ArrayList<>();
        try {
            String sql = "SELECT f.cod,f.data,f.id,f.user,f.fg_tipo,f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<? AND filiale = ? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data<? ORDER BY f.till";

            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, datad1);
            ps.setString(2, filiale[0]);
            ps.setString(3, filiale[0]);
            ps.setString(4, datad1);
            ResultSet rs = ps.executeQuery();

            ArrayList<String> listval = new ArrayList<>();
            ArrayList<String[]> listdati = new ArrayList<>();

            while (rs.next()) {
                String sql2 = "SELECT * FROM stock_report WHERE filiale=? AND data<? AND tipo=? AND (codiceopenclose=? OR codtr=?) AND till=? ORDER BY cod_value";

                PreparedStatement ps2 = this.c.prepareStatement(sql2);
                ps2.setString(1, filiale[0]);
                ps2.setString(2, datad1);
                ps2.setString(3, tipo);
                ps2.setString(4, rs.getString("f.cod"));
                ps2.setString(5, rs.getString("f.cod"));
                ps2.setString(6, rs.getString("f.till"));
                ResultSet rs2 = ps2.executeQuery();

                while (rs2.next()) {
                    listval.add(rs2.getString("cod_value"));
                    String[] dat = { rs2.getString("cod_value"), rs2.getString("kind"), rs2.getString("total") };
                    listdati.add(dat);
                }
                ps2.close();
            }
            ps.close();

            removeDuplicatesAL(listval);

            for (int i = 0; i < listval.size(); i++) {
                ArrayList data = new ArrayList();
                BranchStockInquiry_value bsi1 = new BranchStockInquiry_value();
                String valuta = listval.get(i);

                bsi1.setCurrency(valuta);
                double v1 = 0.00;
                double v2 = 0.00;
                double v3 = 0.00;

                for (int j = 0; j < listdati.size(); j++) {
                    String[] va = listdati.get(j);
                    if (valuta.equals(va[0])) {
                        if (va[1].equals("01")) {
                            v1 = v1 + fd(va[2]);
                        } else if (va[1].equals("02")) {
                            v2 = v2 + fd(va[2]);
                        } else if (va[1].equals("03")) {
                            v3 = v3 + fd(va[2]);
                        }
                    }
                }

                data.add(roundDoubleandFormat(v1, 2));
                data.add(roundDoubleandFormat(v2, 2));
                data.add(roundDoubleandFormat(v3, 2));
                bsi1.setDati(data);

                if (v1 > 0 || v2 > 0 || v3 > 0) {
                    out.add(bsi1);
                }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public static boolean removeDuplicatesAL(ArrayList l) {
        int sizeInit = l.size();

        Iterator p = l.iterator();
        while (p.hasNext()) {
            Object op = p.next();
            Iterator q = l.iterator();
            Object oq = q.next();
            while (op != oq) {
                oq = q.next();
            }
            boolean b = q.hasNext();
            while (b) {
                oq = q.next();
                if (op.equals(oq)) {
                    p.remove();
                    b = false;
                } else {
                    b = q.hasNext();
                }
            }
        }

        Collections.sort(l);

        return sizeInit != l.size();
    }

    public ArrayList<OfficeStockPrice_value> list_OfficeStockPrice_value(String spres, String filiale) {
        ArrayList<OfficeStockPrice_value> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM office_sp_valori WHERE cod = ? ORDER BY currency, kind";

            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, spres);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                OfficeStockPrice_value osp01 = new OfficeStockPrice_value();
                osp01.setData(rs.getString(7));
                osp01.setCurrency(rs.getString(2));
                osp01.setDecurrency(rs.getString(2));
                osp01.setSupporto(rs.getString(3));
                osp01.setQta(rs.getString(4));
                osp01.setMedioacq(rs.getString(5));
                osp01.setControvalore(rs.getString(6));
                out.add(osp01);
            }
            ps.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<OfficeStockPrice_value> list_OfficeStockPrice_value(String filiale) {
        ArrayList<OfficeStockPrice_value> out = new ArrayList<>();
        String valutalocale = get_local_currency()[0];
        try {
            boolean dividi = get_national_office_changetype().equals("/");
            String sql = "SELECT cod_value,kind,total,controval FROM stock WHERE filiale = '" + filiale
                    + "' AND tipostock = 'CH' "
                    + "ORDER BY cod_value,kind,date";

            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            List<String> listval = new ArrayList<>();
            List<Stock> content = new ArrayList<>();

            // String
            // cod_value,kind,total,controval
            while (rs.next()) {
                if (!rs.getString("kind").equals("04")) {
                    content.add(new Stock(rs.getString("cod_value"), rs.getString("kind"), rs.getString("total"),
                            rs.getString("controval")));
                    listval.add(rs.getString("cod_value"));
                }
            }
            ps.close();

            // rs.beforeFirst();
            // removeDuplicatesAL(listval);
            listval = listval.stream().distinct().collect(Collectors.toList());

            for (int i = 0; i < listval.size(); i++) {
                String val = listval.get(i);

                OfficeStockPrice_value osp01 = new OfficeStockPrice_value();
                osp01.setCurrency(val);
                osp01.setDecurrency(val);
                osp01.setSupporto("01");
                OfficeStockPrice_value osp02 = new OfficeStockPrice_value();
                osp02.setCurrency(val);
                osp02.setDecurrency(val);
                osp02.setSupporto("02");

                OfficeStockPrice_value osp03 = new OfficeStockPrice_value();
                osp03.setCurrency(val);
                osp03.setDecurrency(val);
                osp03.setSupporto("03");
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

                double setQtaosp01 = roundDouble(ad_setQtaosp01.get(), 2);
                double controv01 = roundDouble(ad_controv01.get(), 2);

                double setQtaosp02 = roundDouble(ad_setQtaosp02.get(), 2);
                double controv02 = roundDouble(ad_controv02.get(), 2);

                double setQtaosp03 = roundDouble(ad_setQtaosp03.get(), 2);
                double controv03 = roundDouble(ad_controv03.get(), 2);

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

    public static double roundDouble(double d, int scale) {
        d = new BigDecimal(d).setScale(scale, RoundingMode.HALF_UP).doubleValue();
        return d;
    }

    public static double getControvalore(double primo, double secondo, boolean dividi) {
        if (dividi) {
            if (secondo != 0) {
                return primo / secondo;
            } else {
                return 0;
            }
        } else {
            return primo * secondo;
        }
    }
    

    public static double getControvaloreOFFSET(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return secondo / primo;
        }
    }

    public String get_national_office_changetype() {
        try {
            String sql = "SELECT changetype FROM office ";
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
        return "/";
    }

    public ResultSet getDatiPerFiliale(String tabella) {
        try {
            String sql = "SELECT * FROM " + tabella + " WHERE filiale = '000'";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs;
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ResultSet getDatiPerFiliale(String tabella, String filiale) {
        try {
            String sql = "SELECT * FROM " + tabella + " WHERE filiale = '" + filiale + "'";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs;
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
