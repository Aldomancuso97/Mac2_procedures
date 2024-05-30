/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.sftp;

import rc.soop.esolver.Branch;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.formatStringtoStringDate;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.patternsqldate;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import static rc.soop.esolver.Util.visualizzaStringaMySQL;
import rc.soop.qlik.LoggerNew;
import static rc.soop.rilasciofile.Action.formatTypeTransaction_stockprice;
import static rc.soop.rilasciofile.Utility.formatALCurrency;
import static rc.soop.rilasciofile.Utility.formatDoubleforMysql;
import static rc.soop.rilasciofile.Utility.formatMysqltoDisplay;
import static rc.soop.rilasciofile.Utility.getControvalore;
import static rc.soop.rilasciofile.Utility.get_figures;
import static rc.soop.rilasciofile.Utility.path_log;
import static rc.soop.rilasciofile.Utility.patternnormdate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.rilasciofile.BCE;
import rc.soop.rilasciofile.Currency;
import rc.soop.rilasciofile.Figures;
import rc.soop.rilasciofile.Fileinfo;
import rc.soop.rilasciofile.GeneraFile;
import rc.soop.rilasciofile.StockPrice_value;
import static rc.soop.start.Utility.rb;
import java.util.Optional;


/**
 *
 * @author rcosco
 */
public class Db {

    private Connection c = null;
    private LoggerNew logger = new LoggerNew("SFTP_MAC", path_log);

    public Db(boolean mac) {
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
            String host = mac ? rb.getString("db.ip") + "/maccorpita" : rb.getString("db.ip") + "/macsftp";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            logger.log.log(Level.SEVERE, "Errore CONNESSIONE DB : {0}", ex.getMessage());
            this.c = null;
        }
    }

    public void closeDB() {
        try {
            if (c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore CONNESSIONE DB : {0}", ex.getMessage());
        }
    }

    public Connection getConnectionDB() {
        return c;
    }

    public Connection getConnection() {
        return c;
    }

    public ArrayList<Fileinfo> getFile(String table) {
        ArrayList<Fileinfo> out = new ArrayList();
        try {
            String sql = "SELECT nomefile, hash_code, size, stato, b_64 FROM " + table;
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Fileinfo tmp = new Fileinfo(rs.getString("nomefile"), rs.getString("hash_code"), rs.getLong("size"),
                        rs.getString("b_64"));
                out.add(tmp);
            }
            ps.close();
        } catch (SQLException e) {
            logger.log.log(Level.SEVERE, "Errore DB getFile: {0}", e.getMessage());
        }
        return out;
    }

    public boolean insertFile(Fileinfo fileinfo, String table) {
        try {
            String sql = "INSERT INTO " + table + " SET nomefile=?, hash_code=?, size=?, b_64=?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, fileinfo.getName());
            ps.setString(2, fileinfo.getHash());
            ps.setLong(3, fileinfo.getSize());
            ps.setString(4, fileinfo.getB_64());
            ps.close();
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.log.log(Level.SEVERE, "Errore DB insertFile: {0}", e.getMessage());
        }
        return false;
    }

    public String getPath(String path, String field) {
        try {
            String sql = (field.equals("descr")) ? "SELECT descr FROM path WHERE cod = ? "
                    : "SELECT url FROM path WHERE id = ? ";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, path);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            ps.close();
        } catch (SQLException e) {
            logger.log.log(Level.SEVERE, "Errore DB getPath: {0}", e.getMessage());
        }
        return "";
    }

    public ArrayList<String[]> list_files_elaborare() {
        ArrayList<String[]> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM bassilichi WHERE stato = ?";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, "S");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String[] v1 = { "bassilichi", rs.getString(1), rs.getString(2) };
                out.add(v1);
            }
            ps.close();
            String sql2 = "SELECT * FROM mercury WHERE stato = ?";
            PreparedStatement ps2 = this.c.prepareStatement(sql2, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps2.setString(1, "S");
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                String[] v1 = { "mercury", rs2.getString(1), rs2.getString(2) };
                out.add(v1);
            }
            ps2.close();
        } catch (SQLException e) {
            logger.log.log(Level.SEVERE, "Errore DB list_files_elaborare: {0}", e.getMessage());
        }
        return out;
    }

    // HISTORICAL STOCK PRICE 23-08-21
    public ArrayList<Figures> list_figures(String filiale) {
        ArrayList<Figures> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM supporti where filiale = ? ORDER BY fg_sys_trans";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ps.setString(1, filiale);
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
            logger.log.log(Level.SEVERE, "Errore DB list_figures: {0}", ex.getMessage());
        }
        return out;
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
                cu.setDescrizione(rs.getString("de_valuta"));
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
            logger.log.log(Level.SEVERE, "Errore DB list_figures_query_edit: {0}", ex.getMessage());
        }
        return out;
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
            logger.log.log(Level.SEVERE, "Errore DB getNowDT: {0}", ex.getMessage());
        }
        return new DateTime();
    }

    public String get_BCE(DateTime giorno, String valuta, GeneraFile gf) {
        try {
            String sql1 = "SELECT rif_bce FROM rate where filiale = '000' AND valuta = ? AND data = ?";
            PreparedStatement ps1 = this.c.prepareStatement(sql1);
            ps1.setString(1, valuta);
            ps1.setString(2, giorno.toString(patternsql));
            ResultSet rs0 = ps1.executeQuery();
            if (rs0.next()) {
                return rs0.getString(1);
            }
            ps1.close();

            String sql0 = "SELECT rif_bce FROM rate where filiale = '000' AND valuta = ? AND data < ? ORDER BY data DESC LIMIT 1";
            PreparedStatement ps0 = this.c.prepareStatement(sql0);
            ps0.setString(1, valuta);
            ps0.setString(2, giorno.toString(patternsql));
            ResultSet rs1 = ps0.executeQuery();
            if (rs1.next()) {
                return rs1.getString(1);
            }
            ps0.close();
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore DB get_BCE: {0}", ex.getMessage());
        }
        return get_BCE("000", giorno, valuta, gf);
    }

    public String get_BCE(
            String filiale,
            DateTime giorno,
            String valuta,
            GeneraFile gf) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql1 = "SELECT modify FROM rate_history where filiale = ? "
                    + "AND valuta = ? "
                    + "AND modify LIKE ? "
                    + "ORDER BY dt_mod DESC LIMIT 1";
            PreparedStatement ps1 = this.c.prepareStatement(sql1);
            ps1.setString(1, filiale);
            ps1.setString(2, valuta);
            ps1.setString(3, "%bce value%%" + giorno.toString(patternnormdate_filter) + "%%");
            ResultSet rs0 = ps1.executeQuery();
            if (rs0.next()) {
                String modify = rs0.getString(1);
                String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                if (bce.contains(",")) {
                    bce = formatDoubleforMysql(gf, bce);
                }
                return bce;
            }
            ps1.close();

            String gg2 = giorno.minusDays(1).toString(patternsql);

            String sql2 = "SELECT modify FROM rate_history WHERE filiale = ? "
                    + "AND valuta = ? AND modify like ? "
                    + "AND dt_mod < ? ORDER BY dt_mod DESC";
            PreparedStatement ps2 = this.c.prepareStatement(sql2);
            ps2.setString(1, filiale);
            ps2.setString(2, valuta);
            ps2.setString(3, "%bce value%");
            ps2.setString(4, gg2 + " 23:59:59");
            ResultSet rs = ps2.executeQuery();

            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (gg2.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(gf, bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(gg2).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        gg2 = sqldate.parseDateTime(gg2).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
            }
            ps2.close();
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore DB get_BCE: {0}", ex.getMessage());
        }
        return "0";
    }

    public String get_BCE_central(DateTime giorno, String valuta, GeneraFile gf) {
        try {
            DateTimeFormatter sqldate = DateTimeFormat.forPattern(patternsql);
            String sql1 = "SELECT modify FROM rate_history WHERE filiale = '000' "
                    + "AND valuta = ? AND modify LIKE '%bce value%' "
                    + "AND modify LIKE ? ORDER BY dt_mod DESC LIMIT 1";

            PreparedStatement ps1 = this.c.prepareStatement(sql1);
            ps1.setString(1, valuta);
            ps1.setString(2, "%" + giorno.toString(patternnormdate_filter) + "%");
            ResultSet rs0 = ps1.executeQuery();

            if (rs0.next()) {
                String modify = rs0.getString(1);
                String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                if (bce.contains(",")) {
                    bce = formatDoubleforMysql(gf, bce);
                }
                return bce;
            }
            ps1.close();

            String gg2 = giorno.minusDays(1).toString(patternsql);

            String sql2 = "SELECT modify FROM rate_history WHERE filiale = '000' "
                    + "AND valuta = ? AND modify LIKE '%bce value%' "
                    + "AND dt_mod < ? ORDER BY dt_mod DESC";

            PreparedStatement ps2 = this.c.prepareStatement(sql2);
            ps2.setString(1, valuta);
            ps2.setString(2, gg2 + " 23:59:59");
            ResultSet rs = ps2.executeQuery();

            while (rs.next()) {
                String modify = rs.getString(1);
                String datainizio = modify.split("Date validity: ")[1].trim().split(" ")[0];
                String datasql = formatStringtoStringDate(datainizio, patternnormdate_filter, patternsql);
                if (gg2.equals(datasql)) {
                    String bce = modify.split("BCE value ")[1].trim().split("<")[0];
                    if (bce.contains(",")) {
                        bce = formatDoubleforMysql(gf, bce);
                    }
                    return bce;
                } else {
                    if (sqldate.parseDateTime(gg2).isBefore(sqldate.parseDateTime(datasql))) {
                    } else {
                        gg2 = sqldate.parseDateTime(gg2).minusDays(1).toString(patternsql);
                        rs.previous();
                    }
                }
                ps2.close();
            }
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore DB get_BCE_central: {0}", ex.getMessage());
        }
        return "0";
    }

    public boolean is_ET_FROMBRANCH(String cod) {
        try (PreparedStatement ps = this.c
                .prepareStatement("SELECT cod FROM et_change WHERE cod=? AND fg_tofrom='F' AND fg_brba='BR'")) {
            ps.setString(1, cod);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ex) {
            logger.log.log(Level.SEVERE, "Errore DB is_ET_FROMBRANCH: {0}", ex.getMessage());
        }
        return false;
    }

    public ArrayList<StockPrice_value> list_StockPrice_value(String[] filiale, boolean dividi, String loccur,
            GeneraFile gf) {
        ArrayList<StockPrice_value> out = new ArrayList<>();
        ArrayList<Currency> licur = list_figures_query_edit(filiale[0]);
        ArrayList<Figures> lifg = list_figures(filiale[0]);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsqldate);
        DateTime inizioanno = new DateTime().withDayOfYear(1).withMillisOfDay(0);
        DateTime today = new DateTime().withMillisOfDay(0);
        List<BCE> present = new ArrayList<>();

        String sql = "SELECT * FROM stock WHERE filiale = ? AND total <> '0.00' "
                + "AND tipostock = 'CH' AND (kind = '01' OR kind = '02' OR kind = '03') "
                + "AND codice NOT IN (SELECT codice FROM stock WHERE cod_value = ? AND kind = '01') "
                + "ORDER BY cod_value, kind, date";

        try {
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, filiale[0]);
            ps.setString(2, loccur);

            try (ResultSet rs = ps.executeQuery()) {
                String dateactual = getNowDT().toString(patternnormdate_filter);

                while (rs.next()) {
                    StockPrice_value sp1 = new StockPrice_value();
                    sp1.setDateactual(dateactual + " (Actual)");

                    sp1.setCurrency(rs.getString("cod_value"));
                    sp1.setDe_currency(formatALCurrency(rs.getString("cod_value"), licur));
                    sp1.setSupportocod(rs.getString("kind"));
                    sp1.setSupportodesc(get_figures(lifg, rs.getString("kind")).getDe_supporto());
                    sp1.setBcanconote1(formatTypeTransaction_stockprice(rs.getString("tipo")));
                    sp1.setSupportovalue(rs.getString("id_op"));
                    sp1.setBanconote2(formatStringtoStringDate(rs.getString("date"), patternsqldate, patternnormdate));
                    sp1.setQuantita(rs.getString("total"));
                    sp1.setControvalore(rs.getString("controval"));
                    sp1.setCambio(roundDoubleandFormat(fd(rs.getString("rate")), 8));

                    Currency currency = licur.stream().filter(c1 -> c1.getCode().equals(sp1.getCurrency())).findAny()
                            .orElse(null);
                    if (currency != null) {
                        double rateattuale = fd(currency.getCambio_bce());
                        String valuta = rs.getString("cod_value");
                        String datarif = StringUtils.substring(rs.getString("date"), 0, 10);
                        DateTime dt = formatter.parseDateTime(StringUtils.substring(rs.getString("date"), 0, 19));
                        boolean annoprecedente = dt.isBefore(inizioanno);
                        double oldrate = 1.00;


                        if (!valuta.equals(loccur)) {
                            if (annoprecedente) {
                                DateTime data_riferimento = inizioanno.minusDays(1);
                                oldrate = fd(get_BCE(data_riferimento, valuta, gf));
                            } else {
                                boolean checkET = rs.getString("tipo").equals("ET");
                                if (checkET) {
                                    checkET = is_ET_FROMBRANCH(rs.getString("idoperation"));
                                }
                        
                                if (checkET) {
                                    oldrate = fd(roundDoubleandFormat(fd(rs.getString("rate")), 8));
                                    sp1.setBcanconote1("Ext. Transfert BR");
                                } else {
                                    boolean ratefromlist = false;
                        
                                    if (dt.withMillisOfDay(0).equals(today)) {
                                        oldrate = rateattuale;
                                        present.add(new BCE(datarif, valuta, rateattuale));
                                    } else {
                                        if (!present.isEmpty()) {
                                            ratefromlist = present.stream()
                                                    .anyMatch(c1 -> c1.getData().equalsIgnoreCase(datarif)
                                                            && c1.getValuta().equalsIgnoreCase(valuta));
                                        }
                        
                                        if (ratefromlist) {
                                            Optional<BCE> optionalBCE = present.stream()
                                                    .filter(c1 -> c1.getData().equalsIgnoreCase(datarif)
                                                            && c1.getValuta().equalsIgnoreCase(valuta))
                                                    .findFirst();
                                            if (optionalBCE.isPresent()) {
                                                oldrate = optionalBCE.get().getRif_bce();
                                            } else {
                                                // Handle the case where no BCE is found
                                                oldrate = fd(get_BCE(dt, valuta, gf));
                                                if (oldrate == 0) {
                                                    oldrate = fd(get_BCE(dt, valuta, gf));
                                                }
                                                if (oldrate == 0) {
                                                    oldrate = fd(get_BCE_central(dt, valuta, gf));
                                                }
                                                if (oldrate == 0) {
                                                    oldrate = fd(rs.getString("rate"));
                                                }
                                            }
                                        } else {
                                            oldrate = fd(get_BCE(dt, valuta, gf));
                                            if (oldrate == 0) {
                                                oldrate = fd(get_BCE(dt, valuta, gf));
                                            }
                                            if (oldrate == 0) {
                                                oldrate = fd(get_BCE_central(dt, valuta, gf));
                                            }
                                            if (oldrate == 0) {
                                                oldrate = fd(rs.getString("rate"));
                                            }
                        
                                            present.add(new BCE(datarif, valuta, oldrate));
                                        }
                                    }
                                }
                            }
                        }
                        

                        double oldcv = getControvalore(fd(rs.getString("total")), oldrate, dividi);
                        double actualcv = getControvalore(fd(rs.getString("total")), rateattuale, dividi);
                        sp1.setCambio(roundDoubleandFormat(oldrate, 8));

                        sp1.setControvalore(roundDoubleandFormat(oldcv, 2));
                        sp1.setHistoricalBCE(roundDoubleandFormat(rateattuale, 8));
                        sp1.setDeltaEquivalent(roundDoubleandFormat(actualcv - oldcv, 2));
                    }

                    if (fd(rs.getString("total")) > 0
                            && !(rs.getString("cod_value").equals(loccur) && rs.getString("kind").equals("01"))) {
                        out.add(sp1);
                    }
                }
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return out;
    }

    public ArrayList<Branch> list_branch_completeAFTER311217() {
        ArrayList<Branch> out = new ArrayList<>();
        String sql = "SELECT * FROM branch WHERE fg_annullato='0' OR (fg_annullato='1' AND STR_TO_DATE(da_annull, '%Y-%m-%d')>'2017-12-31') ORDER BY de_branch";

        try (PreparedStatement ps = this.c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

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
            ex.printStackTrace();
        }
        return out;
    }

    public ArrayList<BCE> getRate3112() {
        ArrayList<BCE> out = new ArrayList<>();
        try {
            String sql = "SELECT valuta,rif_bce FROM rate r WHERE r.data = '2022-01-01' AND r.filiale='000'";
            try (Statement st = this.c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    out.add(new BCE("", rs.getString(1), rs.getDouble(2)));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }
}
