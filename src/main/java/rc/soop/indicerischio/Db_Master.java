/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.indicerischio;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.removeDuplicatesAL;
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
            String host = rb.getString("db.ip") + "/maccorpita";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
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
            ex.printStackTrace();
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
            ex.printStackTrace();
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
            ex.printStackTrace();
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
            rs.close();
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
            ex.printStackTrace();
        }
        return dat;
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
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public List<Value> list_CustomerTransactionList_value(
            ArrayList<String> branch, String datad1, String datad2, ArrayList<Branch> allbr,
            String numtrans, String threshold) {
        List<Value> out = new ArrayList<>();
        try {
            String sql = "SELECT cl_cod, cod, tipotr, pay, total FROM ch_transaction tr1 WHERE tr1.del_fg='0' ";
            StringBuilder filwhere = new StringBuilder();
            for (int i = 0; i < branch.size(); i++) {
                filwhere.append("tr1.filiale=? OR ");
            }
            if (filwhere.length() > 3) {
                sql += "AND (" + filwhere.substring(0, filwhere.length() - 3) + ") ";
            }
            if (datad1 != null) {
                sql += "AND tr1.data >= ? ";
            }
            if (datad2 != null) {
                sql += "AND tr1.data <= ? ";
            }
            sql += " ORDER BY tr1.cl_cod, tr1.filiale, tr1.data";

            PreparedStatement ps = this.c.prepareStatement(sql);

            // Imposta i parametri delle filiali
            int paramIndex = 1;
            for (String b : branch) {
                ps.setString(paramIndex++, b);
            }

            // Imposta i parametri delle date
            if (datad1 != null) {
                ps.setString(paramIndex++, datad1 + " 00:00:00");
            }
            if (datad2 != null) {
                ps.setString(paramIndex, datad2 + " 23:59:59");
            }

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(new Value(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)));
            }
            ps.close();
            rs.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<CustomerTransactionList_value> list_CustomerTransactionList_value2(
            ArrayList<String> branch, String datad1, String datad2, ArrayList<Branch> allbr,
            String numtrans, String threshold) {
        ArrayList<CustomerTransactionList_value> out = new ArrayList<>();
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            StringBuilder sql = new StringBuilder("SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg='0' ");

            if (!branch.isEmpty()) {
                sql.append("AND (");
                for (int i = 0; i < branch.size(); i++) {
                    sql.append("tr1.filiale=? OR ");
                }
                sql.setLength(sql.length() - 4); // Remove the last " OR "
                sql.append(") ");
            }

            if (datad1 != null) {
                sql.append("AND tr1.data >= ? ");
            }
            if (datad2 != null) {
                sql.append("AND tr1.data <= ? ");
            }
            sql.append("ORDER BY tr1.cl_cod, tr1.filiale, tr1.data");

            stmt = this.c.prepareStatement(sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            int paramIndex = 1;
            for (String br : branch) {
                stmt.setString(paramIndex++, br);
            }
            if (datad1 != null) {
                stmt.setString(paramIndex++, datad1 + " 00:00:00");
            }
            if (datad2 != null) {
                stmt.setString(paramIndex++, datad2 + " 23:59:59");
            }

            rs = stmt.executeQuery();

            ArrayList<String> cl = new ArrayList<>();
            while (rs.next()) {
                cl.add(rs.getString("tr1.cl_cod"));
            }
            removeDuplicatesAL(cl);

            for (String client : cl) {
                Client cli = query_Client(client);
                if (cli != null) {
                    CustomerTransactionList_value c1 = new CustomerTransactionList_value();
                    ArrayList<CustomerTransactionList_value> dati = new ArrayList<>();
                    c1.setCustomer(cli.getCognome() + " " + cli.getNome());
                    c1.setTreshold(threshold != null ? threshold : "");

                    ArrayList<String> cl_numtr = new ArrayList<>();
                    double tot = 0.00;

                    rs.beforeFirst();
                    while (rs.next()) {
                        if (rs.getString("tr1.cl_cod").equals(client)) {
                            cl_numtr.add(rs.getString("tr1.cod"));
                            CustomerTransactionList_value rm = new CustomerTransactionList_value();
                            rm.setBranch(rs.getString("tr1.filiale"));
                            rm.setTill(rs.getString("tr1.till"));
                            rm.setUser(rs.getString("tr1.user"));
                            rm.setNotr(rs.getString("tr1.id"));
                            rm.setTime(rs.getString("tr1.data"));
                            rm.setCur(rs.getString("tr2.valuta"));
                            rm.setAmount(rs.getString("tr2.total"));
                            rm.setRate(rs.getString("tr2.rate"));
                            rm.setTotal(rs.getString("tr2.net"));
                            rm.setPerc(rs.getString("tr2.com_perc_tot"));
                            rm.setComfree(rs.getString("tr2.fx_com"));
                            rm.setPayinpayout(rs.getString("tr2.total"));
                            rm.setCustomer(cli.getCognome() + " " + cli.getNome());
                            rm.setSpread(rs.getString("tr2.spread"));
                            rm.setPos(rs.getString("tr1.pos"));
                            rm.setRound(rs.getString("tr1.round"));
                            rm.setInternetbooking(rs.getString("tr1.intbook"));
                            tot += fd(rm.getTotal());
                            dati.add(rm);
                        }
                    }

                    c1.setDati(dati);

                    removeDuplicatesAL(cl_numtr);
                    if (numtrans != null) {
                        if (cl_numtr.size() >= Integer.parseInt(numtrans)) {
                            out.add(c1);
                        }
                    } else if (threshold != null) {
                        if (tot >= fd(threshold)) {
                            out.add(c1);
                        }
                    }
                    stmt.close();
                    rs.close();
                }
            }
        } catch (Exception ex) {
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
                ps.close();
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    public Client query_Client(String cod) {
        try {

            String sql = "SELECT * FROM ch_transaction_client WHERE codcl = ?";
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
                ps.close();
                ps1.close();
                rs1.close();
                rs.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public boolean deleteAll_Indicerischio() {
        try {
            String sql = "DELETE FROM indice_rischio";
            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.executeUpdate();
                ps.close();
                return true;
            }
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
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new DateTime().toString("yyyy-MM-dd HH:mm:ss");
    }

    public boolean insert_Indicerischio(IndiceRischio ir) {
        try {
            String ins = "INSERT INTO indice_rischio VALUES (?,?,?,?)";
            PreparedStatement ps = this.c.prepareStatement(ins, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, ir.getId());
            ps.setString(2, ir.getMessage());
            ps.setString(3, ir.getStato());
            ps.setString(4, ir.getDt());
            ps.execute();
            ps.close();
            return true;
        } catch (SQLException ex) {
            if (ex.getMessage().toLowerCase().contains("duplicate")) {
                System.out.println(ir.getId() + " duplicate");
                return true;
            }
            ex.printStackTrace();
        }
        return false;
    }

}

class Value {

    String cl_cod, cod, tipotr, pay, total;

    public Value(String cl_cod, String cod, String tipotr, String pay, String total) {
        this.cl_cod = cl_cod;
        this.cod = cod;
        this.tipotr = tipotr;
        this.pay = pay;
        this.total = total;
    }

    public String getCl_cod() {
        return cl_cod;
    }

    public void setCl_cod(String cl_cod) {
        this.cl_cod = cl_cod;
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getTipotr() {
        return tipotr;
    }

    public void setTipotr(String tipotr) {
        this.tipotr = tipotr;
    }

    public String getPay() {
        return pay;
    }

    public void setPay(String pay) {
        this.pay = pay;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

}