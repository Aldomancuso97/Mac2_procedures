/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.getControvalore;
import static rc.soop.riallinea.Util.patternsqlcomplete;
import static rc.soop.riallinea.Util.roundDoubleandFormat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class AllineaRealOsp {

    public static boolean allineaReport(Db_Master db, String[] fil) {
        String st = db.getCodLocal(true)[0];
        Allineamento_Response resp = new Allineamento_Response();
        resp.setFil(fil);
        DateTime start = new DateTime().plusDays(1).withMillisOfDay(0);
        boolean dividi = db.get_national_office_changetype().equals("/");
        ArrayList<BranchStockInquiry_value> bsi = db.list_BranchStockInquiry_value(fil,
                start.toString(patternsqlcomplete), "CH");
        ArrayList<OfficeStockPrice_value> osp = db.list_OfficeStockPrice_value(fil[0]);

        List<String> elenco_osp = osp.stream().filter(f1 -> f1.getSupporto().equals("01")).map(f1 -> f1.getCurrency())
                .collect(Collectors.toList());
        List<String> elenco_bsi = bsi.stream().map(f1 -> f1.getCurrency()).collect(Collectors.toList());

        System.out.println("com.mac_riallinea.AllineaRealOsp.allineaReport(bsi 1)" + bsi.size());
        System.out.println("com.mac_riallinea.AllineaRealOsp.allineaReport(osp 2)" + osp.size());
        System.out.println("com.mac_riallinea.AllineaRealOsp.allineaReport(bsi 1)" + elenco_bsi.size());
        System.out.println("com.mac_riallinea.AllineaRealOsp.allineaReport(osp 2)" + elenco_osp.size());

        if (elenco_osp.size() > elenco_bsi.size()) {
            for (int x = 0; x < elenco_osp.size(); x++) {
                if (!elenco_bsi.contains(elenco_osp.get(x))) {
                    String diff = roundDoubleandFormat(-fd(osp.get(x).getQta()), 2);
                    String sql = "SELECT * FROM stock WHERE filiale=? AND kind='01' AND cod_value=? AND total <>'0.00' ORDER BY date";
                    try {
                        double remove = 0.0;
                        PreparedStatement pstmt = db.getC().prepareStatement(sql);
                        pstmt.setString(1, fil[0]);
                        pstmt.setString(2, sanitizeInput(elenco_osp.get(x)));
                        ResultSet rs = pstmt.executeQuery();
                        while (rs.next() && remove > 0) {
                            if (remove >= fd(rs.getString("total"))) {
                                String upd = "UPDATE stock SET filiale = '---' WHERE codice = ?";
                                PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                                updateStmt.setString(1, rs.getString("codice"));
                                boolean ex = updateStmt.executeUpdate() > 0;
                                System.out.println(st + " (REM) " + upd + " : " + ex);
                                remove = remove - fd(rs.getString("total"));
                                updateStmt.close();
                                System.out.println(st + " (REM) " + remove);
                            } else {
                                String newtotal = roundDoubleandFormat(fd(rs.getString("total")) - remove, 2);
                                String controval = roundDoubleandFormat(
                                        getControvalore(fd(newtotal), fd(rs.getString("rate")), dividi), 2);
                                String upd = "UPDATE stock SET total = ?, controval = ? WHERE codice = ?";
                                PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                                updateStmt.setString(1, newtotal);
                                updateStmt.setString(2, controval);
                                updateStmt.setString(3, rs.getString("codice"));
                                boolean ex = updateStmt.executeUpdate() > 0;
                                updateStmt.close();
                                System.out.println(st + " (REM) " + upd + " : " + ex);
                                remove = remove - (fd(rs.getString("total")) - fd(newtotal));
                                System.out.println(st + " (REM) " + remove);
                            }
                        }
                        rs.close();
                        pstmt.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                        return false;
                    }
                }
            }
        } else {
            for (int x = 0; x < elenco_bsi.size(); x++) {
                if (!elenco_osp.contains(elenco_bsi.get(x))) {
                    String diff = roundDoubleandFormat(fd(bsi.get(x).getDati().get(0).toString()), 2);
                    String sql = "SELECT codice, rate FROM stock WHERE filiale=? AND kind='01' AND cod_value=? AND idoperation LIKE 'ET%' AND total = '0.00' ORDER by date DESC LIMIT 1";
                    try {
                        double add = fd(diff);
                        PreparedStatement pstmt = db.getC().prepareStatement(sql);
                        pstmt.setString(1, fil[0]);
                        pstmt.setString(2, sanitizeInput(bsi.get(x).getCurrency()));
                        ResultSet rs = pstmt.executeQuery();
                        if (rs.next()) {
                            String controval = roundDoubleandFormat(
                                    getControvalore(add, fd(rs.getString("rate")), dividi), 2);
                            String upd = "UPDATE stock SET total = ?, controval = ? WHERE codice = ?";
                            PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                            updateStmt.setString(1, diff);
                            updateStmt.setString(2, controval);
                            updateStmt.setString(3, rs.getString("codice"));
                            boolean ex = updateStmt.executeUpdate() > 0;
                            updateStmt.close();
                            System.out.println(st + " (ADD NEW) " + upd + " : " + ex);
                        }
                        pstmt.close();
                        rs.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                        return false;
                    }
                }
            }
            for (int i = 0; i < bsi.size(); i++) {
                for (int x = 0; x < osp.size(); x++) {
                    if (bsi.get(i).getCurrency().equals(osp.get(x).getCurrency())
                            && osp.get(x).getSupporto().equals("01")) {
                        if (!bsi.get(i).getDati().get(0).toString().equals(osp.get(x).getQta())) {
                            System.out.println(st + " (" + i + ") " + bsi.get(i).getCurrency());
                            System.out.println(st + " (CORRETTO) " + bsi.get(i).getDati().get(0).toString());
                            System.out.println(st + " (ERRATO) " + osp.get(x).getQta());
                            String diff = roundDoubleandFormat(
                                    fd(bsi.get(i).getDati().get(0).toString()) - fd(osp.get(x).getQta()), 2);
                            System.out.println(st + " (DIFF) " + diff);
                            if (fd(diff) > 0) {
                                String sql = "SELECT codice, rate FROM stock WHERE filiale=? AND kind='01' AND cod_value=? AND idoperation LIKE 'ET%' AND total = '0.00' ORDER by date DESC LIMIT 1";
                                try {
                                    PreparedStatement pstmt = db.getC().prepareStatement(sql);
                                    pstmt.setString(1, fil[0]);
                                    pstmt.setString(2, sanitizeInput(bsi.get(i).getCurrency()));
                                    ResultSet rs = pstmt.executeQuery();
                                    while (rs.next()) {
                                        String controval = roundDoubleandFormat(
                                                getControvalore(fd(diff), fd(rs.getString("rate")), dividi), 2);
                                        String upd = "UPDATE stock SET total = ?, controval = ? WHERE codice = ?";
                                        PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                                        updateStmt.setString(1, diff);
                                        updateStmt.setString(2, controval);
                                        updateStmt.setString(3, rs.getString("codice"));
                                        boolean ex = updateStmt.executeUpdate() > 0;
                                        updateStmt.close();
                                        System.out.println(st + " (ADD) " + upd + " : " + ex);
                                    }
                                    pstmt.close();
                                    rs.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                    return false;
                                }
                            } else {
                                String sql = "SELECT * FROM stock WHERE filiale=? AND kind='01' AND cod_value=? AND total <>'0.00' ORDER by date";
                                try {
                                    PreparedStatement pstmt = db.getC().prepareStatement(sql);
                                    pstmt.setString(1, fil[0]);
                                    pstmt.setString(2, sanitizeInput(bsi.get(i).getCurrency()));
                                    double remove = -fd(diff);
                                    System.out.println(st + " (REM) " + remove);
                                    ResultSet rs = pstmt.executeQuery();
                                    while (rs.next() && remove > 0) {
                                        if (remove >= fd(rs.getString("total"))) {
                                            String upd = "UPDATE stock SET filiale = '---' WHERE codice = ?";
                                            PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                                            updateStmt.setString(1, rs.getString("codice"));
                                            boolean ex = updateStmt.executeUpdate() > 0;
                                            System.out.println(st + " (REM) " + upd + " : " + ex);
                                            remove = remove - fd(rs.getString("total"));
                                            updateStmt.close();
                                            System.out.println(st + " (REM) " + remove);
                                        } else {
                                            String newtotal = roundDoubleandFormat(fd(rs.getString("total")) - remove,
                                                    2);
                                            String controval = roundDoubleandFormat(
                                                    getControvalore(fd(newtotal), fd(rs.getString("rate")), dividi), 2);
                                            String upd = "UPDATE stock SET total = ?, controval = ? WHERE codice = ?";
                                            PreparedStatement updateStmt = db.getC().prepareStatement(upd);
                                            updateStmt.setString(1, newtotal);
                                            updateStmt.setString(2, controval);
                                            updateStmt.setString(3, rs.getString("codice"));
                                            boolean ex = updateStmt.executeUpdate() > 0;
                                            updateStmt.close();
                                            System.out.println(st + " (REM) " + upd + " : " + ex);
                                            remove = remove - (fd(rs.getString("total")) - fd(newtotal));
                                            System.out.println(st + " (REM) " + remove);
                                        }
                                    }
                                    rs.close();
                                    pstmt.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    private static String sanitizeInput(String input) {
        return input.replaceAll("-", "");
    }

    public static boolean allineaOSP(Db_Master db, String fil[]) {

        return false;
    }

}

class Allineamento_Response {

    String fil[];

    boolean central, filiale;

    public String[] getFil() {
        return fil;
    }

    public void setFil(String[] fil) {
        this.fil = fil;
    }

    public boolean isCentral() {
        return central;
    }

    public void setCentral(boolean central) {
        this.central = central;
    }

    public boolean isFiliale() {
        return filiale;
    }

    public void setFiliale(boolean filiale) {
        this.filiale = filiale;
    }

}

class IpFiliale {

    String filiale, ip;

    public IpFiliale(String filiale, String ip) {
        this.filiale = filiale;
        this.ip = ip;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}

class Processed {

    String d1, cop, osp, lcop;

    public Processed(String d1, String cop, String osp, String lcop) {
        this.d1 = d1;
        this.cop = cop;
        this.osp = osp;
        this.lcop = lcop;
    }

    public String getD1() {
        return d1;
    }

    public void setD1(String d1) {
        this.d1 = d1;
    }

    public String getCop() {
        return cop;
    }

    public void setCop(String cop) {
        this.cop = cop;
    }

    public String getOsp() {
        return osp;
    }

    public void setOsp(String osp) {
        this.osp = osp;
    }

    public String getLcop() {
        return lcop;
    }

    public void setLcop(String lcop) {
        this.lcop = lcop;
    }

}
