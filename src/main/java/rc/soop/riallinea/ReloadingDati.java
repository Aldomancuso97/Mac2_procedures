/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import static rc.soop.riallinea.Util.createLog;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.formatStringtoStringDate;
import static rc.soop.riallinea.Util.patternita;
import static rc.soop.riallinea.Util.patternsql;
import static rc.soop.riallinea.Util.roundDoubleandFormat;
import static rc.soop.rilasciofile.Utility.sanitizeInput;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.maintenance.Monitor;
import static rc.soop.riallinea.AllineaRealOsp.allineaReport;
import static rc.soop.riallinea.Util.estraiEccezione;

/**
 *
 * @author rcosco
 */
public class ReloadingDati {

    public static final Logger log = createLog("Mac2.0_ProceduraRecuperoErrori_", Monitor.rb.getString("path.log"),
            "yyyyMMdd");

    public static int sanitizeInt(int value) {
        // Verifica se il valore è negativo e lo rende positivo se lo è
        return Math.max(value, 0);
    }

    private static void fase3(DateTime start) {

        Db_Master db0 = new Db_Master();
        List<IpFiliale> filial_list_ip = db0.getIpFiliale();
        db0.closeDB();

        List<String> lista = new ArrayList<>();
        try {
            Db_Master db1 = new Db_Master();
            String sql = "SELECT cod FROM maccorpita.branch WHERE cod <> '000' AND (fg_annullato = '0' OR (fg_annullato = '1' AND da_annull > '2020-01-01'))";
            try (PreparedStatement ps1 = db1.getC().prepareStatement(sql)) {
                try (ResultSet rs1 = ps1.executeQuery()) {
                    while (rs1.next()) {
                        lista.add(rs1.getString(1).replaceAll("'", "''"));
                    }
                }
            }
            db1.closeDB();
        } catch (SQLException e) {
            log.severe(ExceptionUtils.getStackTrace(e));
        }

        for (String fil : lista) {
            log.log(Level.WARNING, "INIZIO: {0}", fil);
            try {
                fase1(fil, start);
            } catch (Exception e1) {
                log.severe(estraiEccezione(e1));
            }

            try {
                Optional<IpFiliale> optionalFiliale = filial_list_ip.stream()
                        .filter(f1 -> f1.getFiliale().equals(fil))
                        .findAny();

                if (optionalFiliale.isPresent()) {
                    String ip = optionalFiliale.get().getIp();
                    fase1_dbfiliale(fil, start, ip);
                } else {
                    log.warning("Nessuna filiale trovata per il nome: " + fil);
                }
            } catch (Exception e1) {
                log.severe("Errore durante l'esecuzione di fase1_dbfiliale: " + e1.getMessage());
            }

            try {
                String[] fil2 = { fil, fil };
                Db_Master db = new Db_Master();
                boolean central_resp = allineaReport(db, fil2);
                db.closeDB();

                if (!central_resp) {
                    log.severe("ALLINEAMENTO CENTRALE FALLITO. RIPROVARE. FILIALE: " + fil);
                } else {
                    Optional<IpFiliale> optionalFiliale = filial_list_ip.stream()
                            .filter(f1 -> f1.getFiliale().equals(fil))
                            .findAny();

                    if (optionalFiliale.isPresent()) {
                        Db_Master filialdb = new Db_Master(true, sanitizeInput(optionalFiliale.get().getIp()));
                        boolean filial_resp = allineaReport(filialdb, fil2);
                        filialdb.closeDB();
                        if (!filial_resp) {
                            log.severe("ALLINEAMENTO FILIALE FALLITO. RIPROVARE. FILIALE: " + fil);
                        }
                    } else {
                        log.severe("FILIALE NON TROVATA NELLA LISTA. FILIALE: " + fil);
                    }
                }
            } catch (Exception e1) {
                log.severe(estraiEccezione(e1));
            }

            try {
                Optional<IpFiliale> optionalFiliale = filial_list_ip.stream()
                        .filter(f1 -> f1.getFiliale().equals(fil))
                        .findAny();

                if (optionalFiliale.isPresent()) {
                    String ip = optionalFiliale.get().getIp();
                    allineaOSP_Prec(fil, ip);
                } else {
                    log.warning("Nessuna filiale trovata per il nome: " + fil);
                }
            } catch (Exception e1) {
                log.severe("Errore durante l'allineamento OSP: " + e1.getMessage());
            }
            log.log(Level.WARNING, "FINE: {0}", fil);
        }
    }

    private static void allineaOSP_Prec(String fil_cod, String ip) {
        Db_Master db = new Db_Master();
        DateTime start = new DateTime().minusDays(10).withMillisOfDay(0);
        DateTime end = new DateTime().withMillisOfDay(0);

        try {
            while (start.isBefore(end)) {
                String stdate = start.toString(patternsql) + " 23:59:59";
                String[] fil = { fil_cod, fil_cod };
                ArrayList<BranchStockInquiry_value> dati = db.list_BranchStockInquiry_value(fil, stdate, "CH");

                if (!dati.isEmpty()) {
                    ArrayList<Office_sp> offices = db.list_query_officesp2(fil[0], stdate.substring(0, 10));
                    if (!offices.isEmpty()) {
                        Office_sp sp = offices.get(0);
                        ArrayList<OfficeStockPrice_value> last = db.list_OfficeStockPrice_value(sp.getCodice(), fil[0]);

                        for (int x = 0; x < last.size(); x++) {
                            OfficeStockPrice_value od = last.get(x);

                            for (int i = 0; i < dati.size(); i++) {
                                if (dati.get(i).getCurrency().equalsIgnoreCase(od.getCurrency())
                                        && !od.getQta().equals(dati.get(i).getDati().get(0).toString())) {
                                    double nc = fd(dati.get(i).getDati().get(0).toString()) * fd(od.getMedioacq());
                                    String upd = "UPDATE office_sp_valori SET quantity = ?, controv = ? "
                                            + "WHERE cod = ? AND currency = ? AND kind = '01'";

                                    try (PreparedStatement pstmt = db.getC().prepareStatement(upd)) {
                                        pstmt.setString(1, dati.get(i).getDati().get(0).toString());
                                        pstmt.setString(2, roundDoubleandFormat(nc, 2));
                                        pstmt.setString(3, sp.getCodice());
                                        pstmt.setString(4, od.getCurrency());

                                        int upd_FILIAL = pstmt.executeUpdate();

                                        if (upd_FILIAL > 0) {
                                            log.info("Aggiornamento effettuato con successo");
                                        } else {
                                            log.warning("Nessun record aggiornato");
                                        }
                                    } catch (SQLException ex1) {
                                        log.severe("ALLINEAMENTO FILIALE " + fil_cod + " FALLITO. RIPROVARE,");
                                        log.severe(estraiEccezione(ex1));
                                    }
                                }
                            }
                        }
                    } else {
                        log.warning("Nessun ufficio trovato per il codice filiale: " + fil[0]);
                    }
                }
                start = start.plusDays(1);
            }
        } finally {
            db.closeDB();
        }
    }

    public static void riallinea() {
        try {
            Db_Master db = new Db_Master();
            String sql = "SELECT v.cod FROM et_change e, et_change_valori v WHERE e.cod = v.cod AND v.ip_spread <> '0.00' "
                    + "AND e.fg_brba = 'BR' AND e.dt_it > '2022-06-01' AND e.fg_annullato = '0'";

            try (Statement st = db.getC().createStatement(); ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    String upd = "UPDATE et_change_valori SET ip_spread = '0.00' WHERE ip_spread <> '0.00' AND cod = ?";

                    try (PreparedStatement pstmt = db.getC().prepareStatement(upd)) {
                        pstmt.setString(1, rs.getString(1));
                        pstmt.executeUpdate();
                    }
                }
            }
            db.closeDB();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        fase3(null);
    }

    public static void main(String[] args) throws SQLException {
        // CHECK EXTERNAL
        // try {
        // Db_Master db = new Db_Master();
        // String sql = "SELECT v.cod FROM et_change e, et_change_valori v WHERE
        // e.cod=v.cod AND v.ip_spread<>'0.00' "
        // + "AND e.fg_brba='BR' AND e.dt_it > '2022-06-01' AND e.fg_annullato='0'";
        // try ( Statement st = db.getC().createStatement(); ResultSet rs =
        // st.executeQuery(sql)) {
        // while (rs.next()) {
        // String upd = "UPDATE et_change_valori SET ip_spread = '0.00' WHERE
        // ip_spread<>'0.00' AND cod='" + rs.getString(1) + "'";
        // try ( Statement st2 = db.getC().createStatement()) {
        // st2.executeUpdate(upd);
        // }
        // }
        // }
        // db.closeDB();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        //
        // fase3(null);

        DateTime start = new DateTime(2024, 4, 24, 0, 0).withMillisOfDay(0);
        // fase3(start);
        //
        List<String> lista = new ArrayList<>();
        // lista.add("306");
        // lista.add("307");
        // lista.add("312");
        // lista.add("321");
        ////// lista.add("172");
        //// lista.add("190");
        //// lista.add("195");
        //// lista.add("048");
        //// lista.add("063");
        //// lista.add("090");
        // lista.add("019");
        lista.add("111");
        // lista.add("188");
        //// lista.add("172");
        //// lista.add("188");
        //// lista.add("195");
        ////
        for (int i = 0; i < lista.size(); i++) {
            String fil = lista.get(i);
            log.log(Level.WARNING, "INIZIO: {0}", fil);
            fase1(fil, start);
            log.log(Level.WARNING, "FINE: {0}", fil);
            // fase1_dbfiliale(fil, start, "192.168.115.106");
        }
        // DateTime start = new DateTime().withMillisOfDay(0);
        //
        // LinkedList<Dati> result = main("090", start);
        //
        // result.forEach(d1 -> {
        // System.out.println(d1.toString());
        // });
        // fase1("090", new DateTime(2021, 1, 20, 0, 0).withMillisOfDay(0));
        // fase3();
        // List<String> listaok = new ArrayList<>();
        // listaok.add("010");//07/12
        // List<String> lista = new ArrayList<>();
        // try {
        //
        // Db_Master db1 = new Db_Master();
        // Statement st1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        // ResultSet.CONCUR_UPDATABLE);
        // ResultSet rs1 = st1.executeQuery("SELECT cod FROM maccorpita.branch WHERE
        // cod<>'000' AND (fg_annullato = '0' OR (fg_annullato ='1' AND da_annull >
        // '2020-01-01')) AND cod NOT IN (SELECT DISTINCT(filiale) FROM
        // macreport.dailyerror)");
        // while (rs1.next()) {
        // if (!listaok.contains(rs1.getString(1))) {
        // lista.add(rs1.getString(1));
        // }
        // }
        // rs1.close();
        // st1.close();
        // db1.closeDB();
        // } catch (Exception e) {
        // log.severe(ExceptionUtils.getStackTrace(e));
        // }
        //
        // for (int i = 0; i < lista.size(); i++) {
        // String fil = lista.get(i);
        // log.warning("INIZIO: " + fil);
        // fase1(fil);
        // fase2(fil);
        // log.warning("FINE: " + fil);
        // }
        // fase2("010");
    }

    public static void fase1_dbfiliale(String filiale, DateTime start, String ip) {

        boolean update = true;

        DateTime end = new DateTime().minusDays(1).withMillisOfDay(0);

        while (start.isBefore(end)) {

            LinkedList<Dati> result = main_dbfiliale(filiale, start, ip);

            Dati dato = result.get(1);

            boolean ERgiornoincorso = false;
            if ((dato.getDAY_COP() == dato.getBR_ST_IN()) && (dato.getDAY_COP() == dato.getOF_ST_PR())
                    && (dato.getBR_ST_IN() == dato.getOF_ST_PR())) {
            } else {
                ERgiornoincorso = true;
            }

            if (ERgiornoincorso) {

                if (dato.getBR_ST_IN() != dato.getDAY_COP()) {
                    log.log(Level.WARNING, "{0} -> ERRORE BSI DIVERSO DA DAILY COP: {1} -- {2}",
                            new Object[] { dato.getDATA(), dato.getBR_ST_IN(), dato.getDAY_COP() });
                    String d = roundDoubleandFormat(dato.getBR_ST_IN() - dato.getDAY_COP(), 2);
                    double diff = fd(d);
                    String dU = "0.00";
                    String dS = "0.00";

                    if (diff > 0) {
                        dU = d;
                    } else {
                        dS = StringUtils.replace(d, "-", "");
                    }
                    try {
                        Db_Master db1 = new Db_Master();
                        PreparedStatement ps1 = db1.getC().prepareStatement(
                                "INSERT INTO macreport.dailyerror_branch VALUES(?, ?, ?, ?, ?, '', '')");
                        ps1.setString(1, filiale);
                        ps1.setString(2,
                                formatStringtoStringDate(dato.getDATA().split(" ")[0], patternsql, patternita));
                        ps1.setString(3, String.valueOf(dato.getBR_ST_IN()));
                        ps1.setString(4, String.valueOf(dato.getDAY_COP()));
                        ps1.setString(5, d);
                        ps1.executeUpdate();
                        ps1.close();
                        db1.closeDB();
                    } catch (Exception e) {
                        log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
                        log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
                        break;
                    }

                    String newcod = "ERR" + generaId(22);

                    String insert1 = "INSERT INTO oc_lista VALUES (?, ?, '000000000000000', '000', '0000', 'C', ?, 'Y', 'N', 'N', 'N', '-', '-')";
                    String insert2 = "INSERT INTO oc_errors VALUES (?, ?, 'CH', 'EUR', '01', '-', '-', 'Riallineamento valore EURO a seguito di errore di sistema.', ?, ?, ?, '1.00000000', '0.00', ?, '0.00', ?)";
                    try {
                        Db_Master db1 = new Db_Master(true, ip);
                        PreparedStatement ps1 = db1.getC().prepareStatement(insert1);
                        ps1.setString(1, dato.getFILIALE());
                        ps1.setString(2, newcod);
                        ps1.setString(3, dato.getDATA() + " 01:00:00");
                        ps1.executeUpdate();
                        ps1.close();

                        PreparedStatement ps2 = db1.getC().prepareStatement(insert2);
                        ps2.setString(1, dato.getFILIALE());
                        ps2.setString(2, newcod);
                        ps2.setString(3, d);
                        ps2.setString(4, dato.getDATA() + " 01:00:00");
                        ps2.setString(5, dU);
                        ps2.setString(6, dS);
                        ps2.executeUpdate();
                        ps2.close();

                        db1.closeDB();
                        log.info("OK: " + insert1);
                        log.info("OK: " + insert2);
                    } catch (Exception e) {
                        log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
                        log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
                        break;
                    }

                } else if ((dato.getBR_ST_IN() != dato.getOF_ST_PR()) || (dato.getDAY_COP() != dato.getOF_ST_PR())) {
                    log.warning(dato.getDATA() + " -> ERRORE BSI (E DAILY COP) DIVERSO DA OSP: " + dato.getBR_ST_IN()
                            + " -- " + dato.getOF_ST_PR());
                    try {
                        String query1 = "SELECT codice FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1;";

                        Db_Master db0 = new Db_Master(true, ip);
                        PreparedStatement ps0 = db0.getC().prepareStatement(query1);
                        ps0.setString(1, filiale);
                        ps0.setString(2, dato.getDATA() + " 23:59:59");

                        ResultSet rs0 = ps0.executeQuery();

                        if (rs0.next()) {
                            String upd1 = "UPDATE office_sp SET total_cod = ? WHERE codice=?";
                            String upd2 = "UPDATE office_sp_valori SET controv = ? WHERE cod=? AND currency = 'EUR' AND kind = '01'";

                            boolean es1 = false;
                            boolean es2 = false;

                            PreparedStatement ps1 = db0.getC().prepareStatement(upd1);
                            ps1.setString(1, String.valueOf(dato.getBR_ST_IN()));
                            ps1.setString(2, rs0.getString(1));
                            if (update) {
                                es1 = ps1.executeUpdate() > 0;
                            }
                            ps1.close();
                            log.info(es1 + ": " + upd1);

                            PreparedStatement ps2 = db0.getC().prepareStatement(upd2);
                            ps2.setString(1, String.valueOf(dato.getBR_ST_IN()));
                            ps2.setString(2, rs0.getString(1));
                            if (update) {
                                es2 = ps2.executeUpdate() > 0;
                            }
                            ps2.close();
                            log.info(es2 + ": " + upd2);
                            start = start.plusDays(1);
                        }
                        ps0.close();
                        rs0.close();
                        db0.closeDB();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            } else {
                log.info("OK " + dato.toString());
                start = start.plusDays(1);
            }
        }
    }

    private static void fase2_dbfiliale(String filiale, String ip) {
        try {

            Db_Master db0 = new Db_Master(true, ip);

            Statement st0 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            Statement st00 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

            String del1 = "DELETE FROM oc_errors where cod IN (SELECT cod FROM oc_lista where cod LIKE 'ER%'"
                    + " AND errors='Y' AND filiale='" + filiale
                    + "' AND id='000000000000000' AND DATA < '2020-01-01 00:00:00')";
            String del2 = "DELETE FROM oc_lista where cod LIKE 'ER%' AND errors='Y' AND filiale='" + filiale
                    + "' AND id='000000000000000' AND DATA < '2020-01-01 00:00:00'";

            st0.executeUpdate(del1);
            st00.executeUpdate(del2);

            st0.close();
            st00.close();
            db0.closeDB();

            String sql1 = "SELECT SUM(diff) FROM macreport.dailyerror_branch"
                    + " WHERE filiale='" + filiale + "' AND STR_TO_DATE(DATA, '%d/%c/%Y') <'2020-01-01'";

            db0 = new Db_Master();
            Statement st01 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs1 = st01.executeQuery(sql1);
            if (rs1.next()) {

                if (rs1.getString(1) == null) {
                    log.log(Level.INFO, "{0} TUTTO OK", filiale);
                } else {
                    String d = roundDoubleandFormat(fd(rs1.getString(1)), 2);
                    double diff = fd(d);

                    String dU = "0.00";
                    String dS = "0.00";
                    if (diff > 0) {
                        dU = d;
                    } else {
                        dS = StringUtils.replace(d, "-", "");
                    }

                    String newcod = "ERR" + generaId(22);

                    String insert1 = "INSERT INTO oc_lista VALUES ('" + filiale + "','"
                            + newcod + "','000000000000000','000','0000','C','"
                            + "2020-01-01 01:00:00','Y','N','N','N','-','-')";

                    String insert2 = "INSERT INTO oc_errors VALUES ('" + filiale + "','"
                            + newcod
                            + "','CH','EUR','01','-','-','Riallineamento valore EURO a seguito di errore di sistema.','"
                            + d + "','" + "2020-01-01 01:00:00','0.00','0.00','" + dU + "','0.00','" + dS + "')";
                    Db_Master db1 = new Db_Master(true, ip);
                    Statement st1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    Statement st2 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    st1.execute(insert1);
                    st1.execute(insert2);
                    st1.close();
                    st2.close();

                    String sql3 = "SELECT codice,total_cod FROM office_sp WHERE filiale='" + filiale
                            + "' AND data <= '2019-12-31 23:59:59' ORDER BY data DESC LIMIT 1";
                    Statement st3 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs3 = st3.executeQuery(sql3);
                    if (rs3.next()) {
                        String cod = rs3.getString(1);
                        double t_start = fd(roundDoubleandFormat(fd(rs3.getString(2)), 2));
                        log.info(cod + " START) " + t_start);
                        log.info(cod + " DIFF) " + d);
                        log.info(cod + " OUTPUT) " + roundDoubleandFormat(t_start - diff, 2));

                        String upd1 = "UPDATE office_sp SET total_cod = '" + roundDoubleandFormat(t_start - diff, 2)
                                + "' WHERE codice='" + cod + "'";
                        String upd2 = "UPDATE office_sp_valori SET controv = '"
                                + roundDoubleandFormat(t_start - diff, 2) + "' WHERE cod='" + cod
                                + "' AND currency = 'EUR' AND kind = '01'";

                        Statement st001 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        Statement st02 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        st001.executeUpdate(upd1);
                        st02.executeUpdate(upd2);
                        st001.close();
                        st02.close();
                        db1.closeDB();
                    }
                    rs3.close();
                    st3.close();

                }
            }
            rs1.close();
            st01.close();
            db0.closeDB();
        } catch (Exception e) {
            log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
            log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
        }

    }

    private static void fase1(String filiale, DateTime start) throws SQLException {

        boolean update = true;

        // DateTime end = new DateTime(2020, 12, 31, 0, 0);
        DateTime end = new DateTime().withMillisOfDay(0);

        while (start.isBefore(end)) {
            LinkedList<Dati> result = main(filiale, start);

            Dati dato = result.get(1);

            boolean ERgiornoincorso = false;
            if ((dato.getDAY_COP() == dato.getBR_ST_IN()) && (dato.getDAY_COP() == dato.getOF_ST_PR())
                    && (dato.getBR_ST_IN() == dato.getOF_ST_PR())) {
            } else {
                ERgiornoincorso = true;
            }

            if (ERgiornoincorso) {
                if (dato.getBR_ST_IN() != dato.getDAY_COP()) {
                    log.warning(dato.getDATA()
                            + " -> ERRORE BSI DIVERSO DA DAILY COP: "
                            + dato.getBR_ST_IN() + " -- "
                            + dato.getDAY_COP());
                    String d = roundDoubleandFormat(dato.getBR_ST_IN() - dato.getDAY_COP(), 2);
                    double diff = fd(d);
                    String dU = "0.00";
                    String dS = "0.00";

                    if (diff > 0) {
                        dU = d;
                    } else {
                        dS = StringUtils.replace(d, "-", "");
                    }
                    try {
                        Db_Master db1 = new Db_Master();
                        String insertQuery = "INSERT INTO macreport.dailyerror VALUES(?, ?, ?, ?, ?, '', '')";
                        try (PreparedStatement st1 = db1.getC().prepareStatement(insertQuery)) {
                            st1.setString(1, filiale);
                            st1.setString(2,
                                    formatStringtoStringDate(dato.getDATA().split(" ")[0], patternsql, patternita));
                            st1.setDouble(3, dato.getBR_ST_IN());
                            st1.setDouble(4, dato.getDAY_COP());
                            st1.setString(5, d);
                            st1.executeUpdate();
                        }
                    } catch (Exception e) {

                        if (e.getMessage().toLowerCase().contains("duplicate entry")) {
                            start = start.plusDays(1);
                            continue;
                        }
                        log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
                        log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
                    }

                    String newcod = "ERR" + generaId(22);

                    String insert1 = "INSERT INTO oc_lista VALUES (?, ?, '000000000000000', '000', '0000', 'C', ?, 'Y', 'N', 'N', 'N', '-', '-')";

                    String insert2 = "INSERT INTO oc_errors VALUES (?, ?, 'CH', 'EUR', '01', '-', '-', 'Riallineamento valore EURO a seguito di errore di sistema.', ?, ?, '1.00000000', '0.00', ?, '0.00', ?)";
                    try {

                        Db_Master db1 = new Db_Master();
                        try (PreparedStatement st1 = db1.getC().prepareStatement(insert1);
                                PreparedStatement st2 = db1.getC().prepareStatement(insert2)) {
                            st1.setString(1, dato.getFILIALE());
                            st1.setString(2, newcod);
                            st1.setString(3, dato.getDATA() + " 01:00:00");
                            st1.executeUpdate();

                            st2.setString(1, dato.getFILIALE());
                            st2.setString(2, newcod);
                            st2.setString(3, d);
                            st2.setString(4, dato.getDATA() + " 01:00:00");
                            st2.setString(5, dU);
                            st2.setString(6, dS);
                            st2.executeUpdate();
                        }
                        log.info("OK: " + insert1);
                        log.info("OK: " + insert2);
                        // break;
                    } catch (Exception e) {
                        log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
                        log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
                        break;
                    }

                } else if ((dato.getBR_ST_IN() != dato.getOF_ST_PR()) || (dato.getDAY_COP() != dato.getOF_ST_PR())) {
                    log.warning(dato.getDATA() + " -> ERRORE BSI (E DAILY COP) DIVERSO DA OSP: " + dato.getBR_ST_IN()
                            + " -- " + dato.getOF_ST_PR());
                    try {
                        String query1 = "SELECT codice FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1;";

                        Db_Master db0 = new Db_Master();
                        try (PreparedStatement st0 = db0.getC().prepareStatement(query1)) {
                            st0.setString(1, filiale);
                            st0.setString(2, dato.getDATA() + " 23:59:59");

                            try (ResultSet rs0 = st0.executeQuery()) {
                                if (rs0.next()) {

                                    String upd1 = "UPDATE office_sp SET total_cod = ? WHERE codice=?";
                                    String upd2 = "UPDATE office_sp_valori SET controv = ? WHERE cod=? AND currency = 'EUR' AND kind = '01'";

                                    boolean es1 = false;
                                    boolean es2 = false;

                                    try (PreparedStatement st1 = db0.getC().prepareStatement(upd1);
                                            PreparedStatement st2 = db0.getC().prepareStatement(upd2)) {
                                        st1.setDouble(1, dato.getBR_ST_IN());
                                        st1.setString(2, rs0.getString(1));
                                        es1 = st1.executeUpdate() > 0;

                                        st2.setDouble(1, dato.getBR_ST_IN());
                                        st2.setString(2, rs0.getString(1));
                                        es2 = st2.executeUpdate() > 0;
                                    }
                                    log.info(es1 + ": " + upd1);
                                    log.info(es2 + ": " + upd2);
                                    start = start.plusDays(1);
                                }
                            }
                        }
                        db0.closeDB();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            } else {
                log.info("OK " + dato.toString());
                start = start.plusDays(1);
            }
        }
    }

    private static void fase2(String filiale) {
        try {

            Db_Master db0 = new Db_Master();

            Statement st0 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            Statement st00 = db0.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

            String del1 = "DELETE FROM oc_errors where cod IN (SELECT cod FROM oc_lista where cod LIKE 'ER%'"
                    + " AND errors='Y' AND filiale='" + filiale
                    + "' AND id='000000000000000' AND DATA < '2020-01-01 00:00:00')";
            String del2 = "DELETE FROM oc_lista where cod LIKE 'ER%' AND errors='Y' AND filiale='" + filiale
                    + "' AND id='000000000000000' AND DATA < '2020-01-01 00:00:00'";

            st0.executeUpdate(del1);
            st00.executeUpdate(del2);

            st0.close();
            st00.close();
            db0.closeDB();

            String sql1 = "SELECT SUM(diff) FROM macreport.dailyerror WHERE filiale='" + filiale
                    + "' AND STR_TO_DATE(DATA, '%d/%c/%Y') <'2020-01-01'";

            Db_Master db1 = new Db_Master();
            Statement st01 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs1 = st01.executeQuery(sql1);
            if (rs1.next()) {

                if (rs1.getString(1) == null) {
                    log.log(Level.INFO, "{0} TUTTO OK", filiale);
                } else {
                    String d = roundDoubleandFormat(fd(rs1.getString(1)), 2);
                    double diff = fd(d);

                    String dU = "0.00";
                    String dS = "0.00";
                    if (diff > 0) {
                        dU = d;
                    } else {
                        dS = StringUtils.replace(d, "-", "");
                    }

                    String newcod = "ERR" + generaId(22);

                    String insert1 = "INSERT INTO oc_lista VALUES ('" + filiale + "','"
                            + newcod + "','000000000000000','000','0000','C','"
                            + "2020-01-01 01:00:00','Y','N','N','N','-','-')";

                    String insert2 = "INSERT INTO oc_errors VALUES ('" + filiale + "','"
                            + newcod
                            + "','CH','EUR','01','-','-','Riallineamento valore EURO a seguito di errore di sistema.','"
                            + d + "','" + "2020-01-01 01:00:00','1.00000000','0.00','" + dU + "','0.00','" + dS + "')";

                    Statement st1 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    Statement st2 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    st1.execute(insert1);
                    st1.execute(insert2);
                    st1.close();
                    st2.close();

                    String sql3 = "SELECT codice,total_cod FROM office_sp WHERE filiale='" + filiale
                            + "' AND data <= '2019-12-31 23:59:59' ORDER BY data DESC LIMIT 1";
                    Statement st3 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs3 = st3.executeQuery(sql3);
                    if (rs3.next()) {
                        String cod = rs3.getString(1);
                        double t_start = fd(roundDoubleandFormat(fd(rs3.getString(2)), 2));
                        log.info(cod + " START) " + t_start);
                        log.info(cod + " DIFF) " + d);
                        log.info(cod + " OUTPUT) " + roundDoubleandFormat(t_start - diff, 2));

                        String upd1 = "UPDATE office_sp SET total_cod = '" + roundDoubleandFormat(t_start - diff, 2)
                                + "' WHERE codice='" + cod + "'";
                        String upd2 = "UPDATE office_sp_valori SET controv = '"
                                + roundDoubleandFormat(t_start - diff, 2) + "' WHERE cod='" + cod
                                + "' AND currency = 'EUR' AND kind = '01'";

                        Statement st001 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        Statement st02 = db1.getC().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
                        st001.executeUpdate(upd1);
                        st02.executeUpdate(upd2);
                        st001.close();
                        st02.close();

                    }
                    rs3.close();
                    st3.close();

                }
            }
            rs1.close();
            st01.close();
            db1.closeDB();
        } catch (Exception e) {
            log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
            log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
        }

    }

    private static LinkedList<Dati> main_dbfiliale(String fil, DateTime oggi, String ip) {
        LinkedList<Dati> out = new LinkedList<>();
        String[] filiale = { fil, fil };

        DateTime ieri = oggi.minusDays(1);

        String datad1 = ieri.toString("yyyy-MM-dd") + " 23:59:59";
        Db_Master db1 = new Db_Master(true, ip);
        double br_si = 0.00;
        double osp = 0.00;
        try {
            String sql = "SELECT f.cod, f.data, f.id, f.user, f.fg_tipo, f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<? AND filiale=? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data<? "
                    + "ORDER BY f.till";
            PreparedStatement ps = db1.getC().prepareStatement(sql);
            ps.setString(1, datad1);
            ps.setString(2, filiale[0]);
            ps.setString(3, filiale[0]);
            ps.setString(4, datad1);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String sql2 = "SELECT total FROM stock_report WHERE filiale=? AND data<? AND tipo='CH' AND kind='01' AND cod_value = 'EUR' "
                        + "AND (codiceopenclose = ? OR codtr = ?) AND till=?";
                PreparedStatement ps2 = db1.getC().prepareStatement(sql2);
                ps2.setString(1, filiale[0]);
                ps2.setString(2, datad1);
                ps2.setString(3, rs.getString("f.cod"));
                ps2.setString(4, rs.getString("f.cod"));
                ps2.setString(5, rs.getString("f.till"));
                ResultSet rs2 = ps2.executeQuery();
                while (rs2.next()) {
                    br_si += fd(rs2.getString(1));
                }
                ps2.close();
                ps.close();
            }

            String sqlosp = "SELECT total_cod FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1";
            PreparedStatement psosp = db1.getC().prepareStatement(sqlosp);
            psosp.setString(1, filiale[0]);
            psosp.setString(2, datad1);
            ResultSet rsosp = psosp.executeQuery();
            if (rsosp.next()) {
                osp = fd(rsosp.getString(1));
            }
            psosp.close();

        } catch (SQLException e) {
            log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
            log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
        }

        DailyResult res = list_Daily_value_NEW(filiale, datad1.split(" ")[0] + " 00:00",
                datad1.split(" ")[0] + " 23:30", db1);
        Double[] day = res.getValori();

        db1.closeDB();

        out.add(new Dati(filiale[0], datad1.split(" ")[0], ieri, fd(roundDoubleandFormat(day[0], 2)),
                fd(roundDoubleandFormat(br_si, 2)), fd(roundDoubleandFormat(osp, 2)),
                fd(roundDoubleandFormat(day[1], 2)),
                fd(roundDoubleandFormat(day[2], 2)), res.getCODICE_OFP(), fd(roundDoubleandFormat(day[3], 2))));

        datad1 = oggi.toString("yyyy-MM-dd") + " 23:59:59";

        db1 = new Db_Master(true, ip);

        br_si = 0.00;
        osp = 0.00;

        try {
            String sql = "SELECT f.cod, f.data, f.id, f.user, f.fg_tipo, f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<? AND filiale=? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data<? "
                    + "ORDER BY f.till";
            PreparedStatement ps = db1.getC().prepareStatement(sql);
            ps.setString(1, datad1);
            ps.setString(2, filiale[0]);
            ps.setString(3, filiale[0]);
            ps.setString(4, datad1);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String sql2 = "SELECT total FROM stock_report WHERE filiale=? AND data<? AND tipo='CH' AND kind='01' AND cod_value = 'EUR' "
                        + "AND (codiceopenclose = ? OR codtr = ?) AND till=?";
                PreparedStatement ps2 = db1.getC().prepareStatement(sql2);
                ps2.setString(1, filiale[0]);
                ps2.setString(2, datad1);
                ps2.setString(3, rs.getString("f.cod"));
                ps2.setString(4, rs.getString("f.cod"));
                ps2.setString(5, rs.getString("f.till"));
                ResultSet rs2 = ps2.executeQuery();
                while (rs2.next()) {
                    br_si += fd(rs2.getString(1));
                }
                ps2.close();
                ps.close();
            }

            String sqlosp = "SELECT total_cod FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1";
            PreparedStatement psosp = db1.getC().prepareStatement(sqlosp);
            psosp.setString(1, filiale[0]);
            psosp.setString(2, datad1);
            ResultSet rsosp = psosp.executeQuery();
            if (rsosp.next()) {
                osp = fd(rsosp.getString(1));
            }
            psosp.close();

        } catch (SQLException e) {
            log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
            log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
        }
        res = list_Daily_value_NEW(filiale, datad1.split(" ")[0] + " 00:00", datad1.split(" ")[0] + " 23:30", db1);
        day = res.getValori();

        db1.closeDB();

        out.add(new Dati(filiale[0], datad1.split(" ")[0], oggi, fd(roundDoubleandFormat(day[0], 2)),
                fd(roundDoubleandFormat(br_si, 2)), fd(roundDoubleandFormat(osp, 2)),
                fd(roundDoubleandFormat(day[1], 2)),
                fd(roundDoubleandFormat(day[2], 2)), res.getCODICE_OFP(), fd(roundDoubleandFormat(day[3], 2))));

        return out;
    }

    public static LinkedList<Dati> main(String fil, DateTime oggi) throws SQLException {
        LinkedList<Dati> out = new LinkedList<>();
        try {
            String[] filiale = { fil, fil };

            DateTime ieri = oggi.minusDays(1);

            String datad1 = ieri.toString("yyyy-MM-dd") + " 23:59:59";
            Db_Master db1 = new Db_Master();
            double br_si = 0.00;
            double osp = 0.00;

            String sql = "SELECT f.cod,f.data,f.id,f.user,f.fg_tipo,f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<? AND filiale=? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data<? "
                    + "ORDER BY f.till";
            PreparedStatement ps = db1.getC().prepareStatement(sql);
            ps.setString(1, datad1);
            ps.setString(2, filiale[0]);
            ps.setString(3, filiale[0]);
            ps.setString(4, datad1);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String sql2 = "SELECT total FROM stock_report WHERE filiale=? AND data<? AND tipo='CH' AND kind='01' AND cod_value = 'EUR' "
                        + "AND (codiceopenclose = ? OR codtr = ?) AND till=?";
                PreparedStatement ps2 = db1.getC().prepareStatement(sql2);
                ps2.setString(1, filiale[0]);
                ps2.setString(2, datad1);
                ps2.setString(3, rs.getString("f.cod"));
                ps2.setString(4, rs.getString("f.cod"));
                ps2.setString(5, rs.getString("f.till"));
                ResultSet rs2 = ps2.executeQuery();
                while (rs2.next()) {
                    br_si += fd(rs2.getString(1));
                }
                ps2.close();
            }

            ps.close();

            String sqlosp = "SELECT total_cod FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1";
            PreparedStatement psosp = db1.getC().prepareStatement(sqlosp);
            psosp.setString(1, filiale[0]);
            psosp.setString(2, datad1);
            ResultSet rsosp = psosp.executeQuery();
            if (rsosp.next()) {
                osp = fd(rsosp.getString(1));
            }

            psosp.close();

            DailyResult res = list_Daily_value_NEW(filiale, datad1.split(" ")[0] + " 00:00",
                    datad1.split(" ")[0] + " 23:30", db1);
            Double[] day = res.getValori();

            out.add(new Dati(filiale[0], datad1.split(" ")[0], oggi, fd(roundDoubleandFormat(day[0], 2)),
                    fd(roundDoubleandFormat(br_si, 2)), fd(roundDoubleandFormat(osp, 2)),
                    fd(roundDoubleandFormat(day[1], 2)),
                    fd(roundDoubleandFormat(day[2], 2)), res.getCODICE_OFP(), fd(roundDoubleandFormat(day[3], 2))));

            datad1 = oggi.toString("yyyy-MM-dd") + " 23:59:59";

            br_si = 0.00;
            osp = 0.00;

            String sql1 = "SELECT f.cod,f.data,f.id,f.user,f.fg_tipo,f.till "
                    + "FROM (SELECT till, MAX(data) AS maxd FROM oc_lista WHERE data<? AND filiale = ? GROUP BY till) "
                    + "AS x INNER JOIN oc_lista AS f ON f.till = x.till AND f.data = x.maxd AND f.filiale = ? AND f.data<?"
                    + " ORDER BY f.till";

            try (PreparedStatement ps1 = db1.getC().prepareStatement(sql1)) {
                ps1.setString(1, datad1);
                ps1.setString(2, filiale[0]);
                ps1.setString(3, filiale[0]);
                ps1.setString(4, datad1);

                ResultSet rs1 = ps1.executeQuery();

                while (rs1.next()) {
                    String sql2 = "SELECT total FROM stock_report where filiale=? AND data<? AND tipo='CH' AND kind='01' AND cod_value = 'EUR' "
                            + "AND (codiceopenclose = ? OR codtr = ?) AND till=?";
                    try (PreparedStatement ps2 = db1.getC().prepareStatement(sql2)) {
                        ps2.setString(1, filiale[0]);
                        ps2.setString(2, datad1);
                        ps2.setString(3, rs1.getString("f.cod"));
                        ps2.setString(4, rs1.getString("f.cod"));
                        ps2.setString(5, rs1.getString("f.till"));

                        try (ResultSet rs2 = ps2.executeQuery()) {
                            while (rs2.next()) {
                                br_si = br_si + fd(rs2.getString(1));
                            }
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace(); // Gestione dell'eccezione
                    }
                }

                String sqlosp1 = "SELECT total_cod FROM office_sp WHERE filiale=? AND data <= ? ORDER BY data DESC LIMIT 1";
                PreparedStatement psosp1 = db1.getC().prepareStatement(sqlosp1);
                psosp1.setString(1, filiale[0]);
                psosp1.setString(2, datad1);
                ResultSet rsosp1 = psosp1.executeQuery();
                if (rsosp1.next()) {
                    osp = fd(rsosp1.getString(1));
                }
                res = list_Daily_value_NEW(filiale, datad1.split(" ")[0] + " 00:00", datad1.split(" ")[0] + " 23:30",
                        db1);
                day = res.getValori();

                db1.closeDB();

                out.add(new Dati(filiale[0], datad1.split(" ")[0], oggi, fd(roundDoubleandFormat(day[0], 2)),
                        fd(roundDoubleandFormat(br_si, 2)),
                        fd(roundDoubleandFormat(osp, 2)), fd(roundDoubleandFormat(day[1], 2)),
                        fd(roundDoubleandFormat(day[2], 2)),
                        res.getCODICE_OFP(), fd(roundDoubleandFormat(day[3], 2))));

                psosp1.close();

            }

        } catch (Exception e) {
            log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
            log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
        }
        return out;
    }

    private static DailyResult list_Daily_value_NEW(String[] fil, String datad1, String datad2, Db_Master db1) {

        if (datad1 != null && datad2 != null) {

            try {
                ArrayList<NC_causal> nc_caus = db1.query_nc_causal_filial(fil[0], null);

                double setPurchTotal = 0.0;
                double setSalesTotal = 0.0;
                double setCashAdNetTot = 0.0;
                double refund = 0.0;
                // refund
                String sql0 = "SELECT value FROM ch_transaction_refund WHERE status = '1' AND method = 'BR' AND branch_cod = ? AND dt_refund >= ? AND dt_refund <= ?";
                PreparedStatement ps0 = db1.getC().prepareStatement(sql0);
                ps0.setString(1, fil[0]);
                ps0.setString(2, datad1 + ":00");
                ps0.setString(3, datad2 + ":59");
                ResultSet rs0 = ps0.executeQuery();
                while (rs0.next()) {
                    refund += fd(rs0.getString("value"));
                }
                ps0.close();
                // TRANSACTION
                String sql = "SELECT tr1.tipotr, cod, tr1.pay, tr1.localfigures, tr1.pos FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = ? AND tr1.data >= ? AND tr1.data <= ?";
                PreparedStatement ps = db1.getC().prepareStatement(sql);
                ps.setString(1, fil[0]);
                ps.setString(2, datad1 + ":00");
                ps.setString(3, datad2 + ":59");
                ResultSet rs = ps.executeQuery();
                double poamount = 0.00;
                ArrayList<String[]> cc = db1.credit_card_enabled();
                ArrayList<String[]> bc = db1.list_bankAccount();
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

                while (rs.next()) {
                    if (rs.getString("tr1.tipotr").equals("B")) {
                        String sqlValori = "SELECT supporto, net, pos FROM ch_transaction_valori WHERE cod_tr = ?";
                        PreparedStatement psValori = db1.getC().prepareStatement(sqlValori);
                        psValori.setString(1, rs.getString("cod"));
                        ResultSet rsval = psValori.executeQuery();
                        while (rsval.next()) {
                            if (rsval.getString("supporto").equals("04")) {// CASH ADVANCE
                                setCashAdNetTot += fd(rsval.getString("net"));
                            } else if (rsval.getString("supporto").equals("06")) {// CREDIT CARD
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("07")) {// bancomat
                                DailyCOP dc = DailyCOP.get_obj(dclist, rsval.getString("pos"));
                                if (dc != null) {
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else if (rsval.getString("supporto").equals("08")) {
                                DailyBank dc = DailyBank.get_obj(listdb, rsval.getString("pos"));
                                if (dc != null) {
                                    poamount += fd(rsval.getString("net"));
                                }
                            } else {
                                setPurchTotal += fd(rsval.getString("net"));
                            }
                        }
                        psValori.close();
                    } else {
                        setSalesTotal += fd(rs.getString("tr1.pay"));
                        if (rs.getString("tr1.localfigures").equals("06")) {// CREDIT CARD
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                        } else if (rs.getString("tr1.localfigures").equals("07")) {// bancomat
                            DailyCOP dc = DailyCOP.get_obj(dclist, rs.getString("tr1.pos"));
                            if (dc != null) {
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                        } else if (rs.getString("tr1.localfigures").equals("08")) {
                            DailyBank dc = DailyBank.get_obj(listdb, rs.getString("tr1.pos"));
                            if (dc != null) {
                                poamount += fd(rs.getString("tr1.pay"));
                            }
                        }
                    }
                }
                ps.close();

                // NO CHANGE
                String sql1 = "SELECT causale_nc, supporto, total, pos, fg_inout, quantita FROM nc_transaction WHERE del_fg='0' AND filiale = ? AND data >= ? AND data <= ? AND (supporto = '01' OR supporto ='...')";
                PreparedStatement ps1 = db1.getC().prepareStatement(sql1);
                ps1.setString(1, fil[0]);
                ps1.setString(2, datad1 + ":00");
                ps1.setString(3, datad2 + ":59");
                ResultSet rs1 = ps1.executeQuery();

                double setToLocalCurr = 0.00;
                double setFromLocalCurr = 0.00;
                while (rs1.next()) {
                    NC_causal nc2 = getNC_causal(nc_caus, rs1.getString("causale_nc"));
                    if (nc2 == null) {
                        continue;
                    }
                    if (rs1.getString("fg_inout").equals("1") || rs1.getString("fg_inout").equals("3")) {
                        setFromLocalCurr += fd(rs1.getString("total"));
                    } else {
                        if (!nc2.getNc_de().equals("14")) { // solo gli acquisti non vengono considerati
                            setToLocalCurr += fd(rs1.getString("total"));
                        }
                    }
                }
                ps1.close();

                double totalnotesnochange = setToLocalCurr + setFromLocalCurr;

                double setBaPurchTransfNotes = 0.00;
                double setBaSalesTransfNotes = 0.00;
                double setBraPurchLocalCurr = 0.00;
                double setBraSalesLocalCurr = 0.00;

                // EXTERNAL TRANSFER
                String sql2 = "SELECT cod, fg_tofrom, fg_brba FROM et_change WHERE fg_annullato = '0' AND filiale = ? AND dt_it >= ? AND dt_it <= ?";
                PreparedStatement ps2 = db1.getC().prepareStatement(sql2);
                ps2.setString(1, fil[0]);
                ps2.setString(2, datad1 + ":00");
                ps2.setString(3, datad2 + ":59");
                ResultSet rs2 = ps2.executeQuery();

                while (rs2.next()) {
                    String cod = rs2.getString("cod");
                    String fg_tofrom = rs2.getString("fg_tofrom");
                    String fg_brba = rs2.getString("fg_brba");

                    String sql2val = "SELECT ip_total FROM et_change_valori WHERE cod = ? AND kind ='01' AND currency='EUR'";
                    PreparedStatement ps2val = db1.getC().prepareStatement(sql2val);
                    ps2val.setString(1, cod);
                    ResultSet rs2val = ps2val.executeQuery();

                    if (fg_tofrom.equals("T")) { // sales
                        if (fg_brba.equals("BA")) { // BANK
                            while (rs2val.next()) {
                                setBaSalesTransfNotes += fd(rs2val.getString("ip_total"));
                            }
                        } else { // BRANCH
                            while (rs2val.next()) {
                                setBraSalesLocalCurr += fd(rs2val.getString("ip_total"));
                            }
                        }
                    } else if (fg_brba.equals("BA")) { // BANK
                        while (rs2val.next()) {
                            setBaPurchTransfNotes += fd(rs2val.getString("ip_total"));
                        }
                    } else {
                        while (rs2val.next()) {
                            setBraPurchLocalCurr += fd(rs2val.getString("ip_total"));
                        }
                    }
                    ps2val.close();
                }
                ps2.close();

                double setLastCashOnPrem = 0.0;
                double OFP_FX = 0.0;
                double setFx = 0.0;

                String osp_codice = "";

                ArrayList<Office_sp> li = db1.list_query_officesp2(fil[0],
                        subDays(datad1.substring(0, 10), patternsql, 1));

                if (!li.isEmpty()) {
                    setLastCashOnPrem = fd(li.get(0).getTotal_cod());
                    // setFx = fd(li.get(0).getTotal_fx());
                    Office_sp o = db1.list_query_last_officesp(fil[0], datad2);
                    if (o != null) {
                        double[] d1 = db1.list_dettagliotransazioni(fil, o.getData(), datad2, "EUR");
                        osp_codice = o.getCodice();
                        OFP_FX = fd(o.getTotal_fx());
                        setFx = OFP_FX + d1[1];
                    }
                }

                double setCashOnPremFromTrans = setSalesTotal // sell
                        - setPurchTotal // buy
                        + setBaPurchTransfNotes // bank
                        - setBaSalesTransfNotes // bank
                        + setBraPurchLocalCurr // branch
                        - setBraSalesLocalCurr // branch
                        + totalnotesnochange // nochange
                        + setLastCashOnPrem
                        - refund
                        - setCashAdNetTot
                        - poamount;

                // System.out.println("tester.ReloadingDati.list_Daily_value_NEW()
                // "+setSalesTotal);
                // System.out.println("tester.ReloadingDati.list_Daily_value_NEW()
                // "+setPurchTotal);
                double setCashOnPremError = 0.0;
                String qe = "SELECT total_user, total_system FROM oc_errors where filiale = ? AND cod IN "
                        + "(SELECT cod FROM oc_lista WHERE data >= ? AND data <= ? AND errors='Y') "
                        + "AND tipo='CH' AND kind='01' AND valuta = 'EUR'";

                PreparedStatement ps10 = db1.getC().prepareStatement(qe);
                ps10.setString(1, fil[0]);
                ps10.setString(2, datad1 + ":00");
                ps10.setString(3, datad2 + ":59");
                ResultSet rs10 = ps10.executeQuery();

                while (rs10.next()) {
                    double eurerr = fd(rs10.getString("total_user")) - fd(rs10.getString("total_system"));
                    setCashOnPremError += eurerr;
                }
                ps10.close();

                double setCashOnPrem = setCashOnPremFromTrans + setCashOnPremError;

                Double[] d1 = { setLastCashOnPrem, setCashOnPrem, setFx, OFP_FX };
                DailyResult dr = new DailyResult(d1, osp_codice);

                return dr;
            } catch (Exception e) {
                log.severe("METHOD: " + ExceptionUtils.getRootCause(e).getStackTrace()[0].getMethodName());
                log.severe("ERROR: " + ExceptionUtils.getStackTrace(e));
            }
        }
        return null;
    }

    private static NC_causal getNC_causal(ArrayList<NC_causal> li, String nc_code) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getCausale_nc().equals(nc_code)) {
                return li.get(i);
            }
        }
        return null;
    }

    private static String subDays(String start, String pattern, int days) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
        DateTime dt = formatter.parseDateTime(start);
        return dt.minusDays(days).toString(pattern);
    }

    private static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }
}

class Dati {

    String FILIALE, DATA;
    double DAY_LCP, BR_ST_IN, OF_ST_PR, DAY_COP, DAY_FX, OFP_FX;
    String CODICE_OFP;
    DateTime now;

    public Dati(String FILIALE, String DATA, DateTime now, double DAY_LCP, double BR_ST_IN, double OF_ST_PR,
            double DAY_COP, double DAY_FX,
            String CODICE_OFP, double OFP_FX) {
        this.FILIALE = FILIALE;
        this.DATA = DATA;
        this.DAY_LCP = DAY_LCP;
        this.BR_ST_IN = BR_ST_IN;
        this.OF_ST_PR = OF_ST_PR;
        this.DAY_COP = DAY_COP;
        this.DAY_FX = DAY_FX;
        this.CODICE_OFP = CODICE_OFP;
        this.OFP_FX = OFP_FX;
        this.now = now;
    }

    public double getOFP_FX() {
        return OFP_FX;
    }

    public void setOFP_FX(double OFP_FX) {
        this.OFP_FX = OFP_FX;
    }

    public String getCODICE_OFP() {
        return CODICE_OFP;
    }

    public void setCODICE_OFP(String CODICE_OFP) {
        this.CODICE_OFP = CODICE_OFP;
    }

    public String getFILIALE() {
        return FILIALE;
    }

    public void setFILIALE(String FILIALE) {
        this.FILIALE = FILIALE;
    }

    public String getDATA() {
        return DATA;
    }

    public void setDATA(String DATA) {
        this.DATA = DATA;
    }

    public double getDAY_LCP() {
        return DAY_LCP;
    }

    public void setDAY_LCP(double DAY_LCP) {
        this.DAY_LCP = DAY_LCP;
    }

    public double getBR_ST_IN() {
        return BR_ST_IN;
    }

    public void setBR_ST_IN(double BR_ST_IN) {
        this.BR_ST_IN = BR_ST_IN;
    }

    public double getOF_ST_PR() {
        return OF_ST_PR;
    }

    public void setOF_ST_PR(double OF_ST_PR) {
        this.OF_ST_PR = OF_ST_PR;
    }

    public double getDAY_COP() {
        return DAY_COP;
    }

    public void setDAY_COP(double DAY_COP) {
        this.DAY_COP = DAY_COP;
    }

    public DateTime getNow() {
        return now;
    }

    public void setNow(DateTime now) {
        this.now = now;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
    }

    public double getDAY_FX() {
        return DAY_FX;
    }

    public void setDAY_FX(double DAY_FX) {
        this.DAY_FX = DAY_FX;
    }

}

class DailyResult {

    Double[] valori;
    String CODICE_OFP;

    public DailyResult(Double[] valori, String CODICE_OFP) {
        this.valori = valori;
        this.CODICE_OFP = CODICE_OFP;
    }

    public Double[] getValori() {
        return valori;
    }

    public void setValori(Double[] valori) {
        this.valori = valori;
    }

    public String getCODICE_OFP() {
        return CODICE_OFP;
    }

    public void setCODICE_OFP(String CODICE_OFP) {
        this.CODICE_OFP = CODICE_OFP;
    }

}
