/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import com.google.common.util.concurrent.AtomicDouble;
import rc.soop.start.Utility;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.getControvaloreOFFSET;
import static rc.soop.riallinea.Util.log;
import static rc.soop.riallinea.Util.patternsql;
import static rc.soop.riallinea.Util.roundDouble;
import static rc.soop.riallinea.Util.roundDoubleandFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Db_Master {

    public Connection c = null;

    public Db_Master(boolean filiale, String ip) {
        try {
            if (filiale) {
                Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
                Properties p = new Properties();
                p.put("user", "root");
                p.put("password", "root");
                p.put("useUnicode", "true");
                p.put("characterEncoding", "UTF-8");
                p.put("useSSL", "false");
                // p.put("connectTimeout", "1000");
                // p.put("useJDBCCompliantTimezoneShift", "true");
                // p.put("useLegacyDatetimeCode", "false");
                // p.put("serverTimezone", "UTC");
                this.c = DriverManager.getConnection("jdbc:mysql://" + ip + ":3306/maccorp", p);
            } else {
                this.c = null;
            }
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
            String host = Utility.rb.getString("host_h");
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
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
            this.c = null;
        }
    }

    public Db_Master(boolean cz, boolean uk, boolean test) {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
            String pwd = rb.getString("db.pwd");
            String host;
            if (test) {
                if (cz) {
                    host = rb.getString("db.ip") + "/maccorpcz";
                } else if (uk) {
                    host = rb.getString("db.ip") + "/maccorpuk";
                } else {
                    host = rb.getString("db.ip") + "/maccorp";
                }
            } else {

                if (cz) {
                    host = rb.getString("db.ip") + "/maccorpczprod";
                } else if (uk) {
                    host = rb.getString("db.ip") + "/maccorpukprod";
                } else {
                    host = rb.getString("db.ip") + "/maccorpita";
                }
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

    public Db_Master(boolean cz, boolean uk) {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
            String pwd = rb.getString("db.pwd");
            String host;
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
            String sql = "SELECT des FROM conf WHERE id = ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1).trim();
            }
            ps.close();
        } catch (SQLException ex) {
            ex.printStackTrace(); // Log dell'intero stack trace dell'eccezione
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
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public ArrayList<String[]> getValuteForMonitor(String filiale) {
        ArrayList<String[]> al = new ArrayList<>();
        try {

            String query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                    + "FROM valute where fg_valuta_corrente='0' AND filiale ='" + filiale + "' ";

            if (filiale.equals("---")) {
                query = "SELECT valuta, de_valuta, cambio_bce, buy_std_type, buy_std_value, buy_std, sell_std_value, sell_std_type, sell_std, filiale, enable_buy, enable_sell "
                        + "FROM valute where fg_valuta_corrente='0' AND filiale <>'000' ";
            }

            query = query + " ORDER BY filiale,valuta";

            PreparedStatement ps = this.c.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String var[] = { rs.getString("valuta"), rs.getString("de_valuta"), rs.getString("cambio_bce"),
                        rs.getString("buy_std_value"),
                        rs.getString("buy_std_type"), rs.getString("buy_std"), rs.getString("sell_std_value"),
                        rs.getString("sell_std_type"),
                        rs.getString("sell_std"), rs.getString("filiale"), rs.getString("enable_buy"),
                        rs.getString("enable_sell")
                };
                al.add(var);
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return al;
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

    public Office_sp query_officesp(String codice) {
        try {
            String sql = "SELECT * FROM office_sp where codice = '" + codice + "'";
            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10));
            }
            ps.close();
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }

    public boolean updateCOP(String codice, String cop) {
        try {
            String upd1 = "UPDATE office_sp SET total_cod = '" + cop + "' WHERE codice='" + codice + "';";
            String upd2 = "UPDATE office_sp_valori SET quantity = '" + cop + "' , controv = '" + cop
                    + "' WHERE cod='" + codice + "' AND currency='EUR' and kind='01'";
            PreparedStatement ps1 = this.c.prepareStatement(upd1,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            PreparedStatement ps2 = this.c.prepareStatement(upd2,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs1 = ps1.executeQuery();
            ResultSet rs2 = ps2.executeQuery();
            ps1.close();
            ps2.close();
            return true;
        } catch (Exception ex) {
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

    public Office_sp list_query_last_officesp(String filiale, String data) {
        try {
            String sql = "SELECT * FROM office_sp WHERE filiale = ? AND data < ? ORDER BY data DESC LIMIT 1";
            
            PreparedStatement pstmt = this.c.prepareStatement(sql);
            pstmt.setString(1, filiale);
            pstmt.setString(2, data);
    
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                return new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10));
            }
            
            pstmt.close(); // Chiudi il PreparedStatement dopo l'uso
    
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return null;
    }
    
    

    public ArrayList<Office_sp> list_query_officesp2(String filiale, String data) {
        ArrayList<Office_sp> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM office_sp WHERE filiale = ? AND data <= ? ORDER BY data DESC";
            PreparedStatement pstmt = this.c.prepareStatement(sql);
            pstmt.setString(1, filiale);
            pstmt.setString(2, data + " 23:59:59");
    
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                out.add(new Office_sp(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
                        rs.getString(10)));
            }
    
            pstmt.close(); // Chiudi il PreparedStatement dopo l'uso
    
        } catch (Exception ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
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
            ps.setString(2, getCodLocal(true)[0]);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(rs.getString("cod"));
            }
            ps.close();
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }

    public List<IpFiliale> getIpFiliale() {
        List<IpFiliale> out = new ArrayList<>();
        try {
            String sql = "SELECT filiale,ip FROM dbfiliali";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                out.add(new IpFiliale(rs.getString(1), rs.getString(2)));
            }
            ps.close();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return out;
    }

    public ArrayList<BranchStockInquiry_value> list_BranchStockInquiry_value(String[] filiale, String datad1,
            String tipo) {
        ArrayList<BranchStockInquiry_value> out = new ArrayList<>();
        try {
            String sql = "SELECT f.cod, f.data, f.id, f.user, f.fg_tipo, f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data < ? AND filiale = ? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data < ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, datad1);
            ps.setString(2, filiale[0]);
            ps.setString(3, filiale[0]);
            ps.setString(4, datad1);
            ResultSet rs = ps.executeQuery();

            ArrayList<String> listval = new ArrayList<>();
            ArrayList<String[]> listdati = new ArrayList<>();
            while (rs.next()) {
                String sql2 = "SELECT * FROM stock_report where filiale = ? AND data < ? AND tipo = ? "
                        + "AND (codiceopenclose = ? OR codtr = ?) AND till = ?";
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

        } catch (Exception ex) {
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
            String sql = "SELECT * FROM office_sp_valori WHERE cod = ?";
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
            String sql = "SELECT cod_value,kind,total,controval FROM stock WHERE filiale = '"
                    + filiale + "' AND tipostock = 'CH' AND total <>'0.00' "
                    + "ORDER BY cod_value,kind,date";

            PreparedStatement ps = this.c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            List<String> listval = new ArrayList<>();
            List<Stock> content = new ArrayList<>();
            while (rs.next()) {
                if (!rs.getString("kind").equals("04")) {
                    content.add(new Stock(rs.getString("cod_value"), rs.getString("kind"), rs.getString("total"),
                            rs.getString("controval")));
                    listval.add(rs.getString("cod_value"));
                }
                ps.close();
            }
            
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
                    double mediarate = getControvaloreOFFSET(setQtaosp01,
                            controv01, dividi);
                    osp01.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp01.setControvalore(roundDoubleandFormat(controv01, 2));
                    out.add(osp01);
                }
                if (setQtaosp02 > 0) {
                    osp02.setQta(roundDoubleandFormat(setQtaosp02, 2));
                    double mediarate = getControvaloreOFFSET(setQtaosp02,
                            controv02, dividi);
                    osp02.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp02.setControvalore(roundDoubleandFormat(controv02, 2));
                    out.add(osp02);
                }
                if (setQtaosp03 > 0) {
                    osp03.setQta(roundDoubleandFormat(setQtaosp03, 2));
                    double mediarate = getControvaloreOFFSET(setQtaosp03,
                            controv03, dividi);
                    osp03.setMedioacq(roundDoubleandFormat(mediarate, 8));
                    osp03.setControvalore(roundDoubleandFormat(controv03, 2));
                    out.add(osp03);
                }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
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

    public List<Office_sp> getofficeSP_valorinegativi() {
        List<Office_sp> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM office_sp WHERE total_cod < 0 AND DATA > '2020-03-01'";
            PreparedStatement ps = this.c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {

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
                String[] o1 = { rs.getString(1), (rs.getString(2)), rs.getString(3) };
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

    public ArrayList<NC_causal> query_nc_causal_filial(String filiale, String status) {
        ArrayList<NC_causal> out = new ArrayList<>();
        try {
            String sql = "SELECT * FROM nc_causali WHERE filiale = ?";
            if (status != null && !status.equals("...")) {
                sql += " AND annullato = ?";
            }
            sql += " ORDER BY gruppo_nc";
    
            PreparedStatement pstmt = this.c.prepareStatement(sql);
            pstmt.setString(1, filiale);
            if (status != null && !status.equals("...")) {
                pstmt.setString(2, status);
            }
    
            ResultSet rs = pstmt.executeQuery();
    
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
    
            pstmt.close(); // Chiudi il PreparedStatement dopo l'uso
    
        } catch (SQLException ex) {
            log.severe(ex.getStackTrace()[0].getMethodName() + ": " + ex.getMessage());
        }
        return out;
    }
    

    public double[] list_dettagliotransazioni(String[] fil, String datad1, String datad2, String valutalocale) {
        double[] d = new double[2];
        double lo = 0.00;
        double fx = 0.00;
        try {
            String sql0 = "SELECT * FROM ch_transaction_refund where status = '1' and method = 'BR' and branch_cod = ? "
                    + "AND dt_refund >= ? AND dt_refund <= ?";
            PreparedStatement ps0 = this.c.prepareStatement(sql0);
            ps0.setString(1, fil[0]);
            ps0.setString(2, datad1);
            ps0.setString(3, datad2);
            ResultSet rs0 = ps0.executeQuery();
            while (rs0.next()) {
                lo = lo - fd(rs0.getString("value"));
            }
            ps0.close();

            String sql = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = ? "
                    + "AND tr1.data >= ? AND tr1.data <= ?";
            PreparedStatement ps = this.c.prepareStatement(sql);
            ps.setString(1, fil[0]);
            ps.setString(2, datad1);
            ps.setString(3, datad2);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.getString("tr1.tipotr").equals("B")) {
                    String sql2 = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                    PreparedStatement ps2 = this.c.prepareStatement(sql2);
                    ps2.setString(1, rs.getString("cod"));
                    ResultSet rsval = ps2.executeQuery();
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
                        ps2.close();
                        lo = lo - fd(rsval.getString("net"));
                    }
                } else {
                    String sql3 = "SELECT * FROM ch_transaction_valori WHERE cod_tr = ?";
                    PreparedStatement ps3 = this.c.prepareStatement(sql3);
                    ps3.setString(1, rs.getString("cod"));
                    ResultSet rsval = ps3.executeQuery();
                    while (rsval.next()) {
                        if (rsval.getString("valuta").equals(valutalocale)) {
                            lo = lo - fd(rsval.getString("total"));
                        } else {
                            fx = fx - fd(rsval.getString("total"));
                        }
                    }
                    ps3.close();
                    if (rs.getString("tr1.localfigures").equals("01")) {
                        lo = lo + fd(rs.getString("pay"));
                    }
                }
            }
            ps.close();

            // NO CHANGE
            String sql1 = "SELECT supporto, fg_inout, total FROM nc_transaction WHERE del_fg='0' AND filiale = ? "
                    + "AND data >= ? AND data <= ?";
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
            String sql2 = "SELECT * FROM et_change WHERE fg_annullato = '0' AND filiale = ? "
                    + "AND dt_it >= ? AND dt_it <= ?";
            PreparedStatement ps2 = this.c.prepareStatement(sql2);
            ps2.setString(1, fil[0]);
            ps2.setString(2, datad1);
            ps2.setString(3, datad2);
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                String sql2val = "SELECT * FROM et_change_valori WHERE cod = ?";
                PreparedStatement ps2val = this.c.prepareStatement(sql2val);
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
                    }
                    ps2val.close();
                }
            }
            ps2.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        d[0] = lo;
        d[1] = fx;
        return d;
    }

    public String get_BCE(DateTime giorno, String valuta) {
        try {
            String sql1 = "SELECT rif_bce FROM rate where filiale = '000' AND valuta = '" + valuta + "' AND data = '"
                    + giorno.toString(patternsql) + "'";
            // System.out.println("Q1) " + sql1);
            PreparedStatement ps0 = this.c.prepareStatement(sql1,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs0 = ps0.executeQuery();
            if (rs0.next()) {
                return rs0.getString(1);
            }
            ps0.close();
            String sql0 = "SELECT rif_bce FROM rate where filiale = '000' AND valuta = '" + valuta + "' AND data < '"
                    + giorno.toString(patternsql) + "' ORDER BY giorno DESC LIMIT 1";
            System.out.println("Q2) " + sql0);
            PreparedStatement ps1 = this.c.prepareStatement(sql0,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs1 = ps1.executeQuery();
            if (rs1.next()) {
                return rs1.getString(1);
            }
            ps1.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return null;
    }

}
