/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.aggiornamenti;

import com.google.common.base.Splitter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static rc.soop.aggiornamenti.Mactest.agg;
import static rc.soop.aggiornamenti.Mactest.host_PROD;
import static rc.soop.aggiornamenti.Utility.patternnormdate;
import static rc.soop.aggiornamenti.Utility.sendMail;
import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import static rc.soop.aggiornamenti.Utility.formatStringtoStringDate;
import static rc.soop.aggiornamenti.Utility.patternsqldate;
import rc.soop.cora.CORA;
import static rc.soop.cora.CORA.formatStringtoStringDate_null;
import static rc.soop.cora.CORA.pattermonthnorm;
import static rc.soop.cora.CORA.patternmonthsql;
import static rc.soop.cora.CORA.patternsql;
import static rc.soop.esolver.ESolver.sanitizeFilePath;
import static rc.soop.esolver.ESolver.sanitizeInput;

import rc.soop.oam.Branch;
import rc.soop.oam.Ch_transaction;
import rc.soop.oam.Currency;
import rc.soop.oam.Db_Master;
import static rc.soop.oam.OAM.creaFile;

/**
 *
 * @author rcosco
 */
public class VerificaAggiornamenti {

    public static final int limitAGG = 5000;
    public static final int limitMINUTES = 30;

    public static void verificafileOAMCORA() {
        Db_Master db0 = new Db_Master();
        String path = db0.getPath("temp");
        db0.closeDB();

        List<String> error = new ArrayList<>();

        DateTime dt = new DateTime();

        try {
            Iterable<String> parameters = Splitter.on("/").split(dt.toString("MM/yyyy"));
            Iterator<String> it = parameters.iterator();

            if (it.hasNext()) {
                String codicetrasm = "OPMEN";
                String mese = sanitizeInput(it.next());
                String anno = sanitizeInput(it.next());
                String cfpi = "12951210157";
                String denom = "Maccorp Italiana";
                String comune = "Milano";
                String provincia = "MI";
                String controllo = "A";
                String tipologia = "1";
                int progressivo = 1;

                Db_Master db = new Db_Master();
                ArrayList<String[]> nations = db.country();
                ArrayList<String[]> tipodoc = db.identificationCard();
                ArrayList<Branch> allbr = db.list_branch_enabledB();
                ArrayList<Currency> curlist = db.list_figures();
                ArrayList<Ch_transaction> tran = db.list_transaction_oam(anno, mese);
                db.closeDB();

                String name1 = "OAM_CHECK_" + anno + mese + randomAlphanumeric(15).trim().toLowerCase() + ".txt";
                oggettoFile og1 = creaFile(progressivo, codicetrasm, anno, mese, "0", cfpi, denom,
                        comune, provincia, tipologia, controllo, tran, nations, curlist, allbr, tipodoc,
                        sanitizeFilePath(path + name1));

                File out1 = og1.getFile();

                if (out1 == null) {
                    error.add("ERRORE NELL'ELABORAZIONE OAM MENSILE. CONTROLLARE: " + og1.getErrore());
                }
            }
        } catch (Exception e) {
            error.add("ERRORE NELL'ELABORAZIONE OAM MENSILE. CONTROLLARE: " + sanitizeInput(e.getMessage()));
        }

        try {
            String from = sanitizeInput(dt.toString(pattermonthnorm));
            DateTimeFormatter formatter = DateTimeFormat.forPattern(patternsql);
            oggettoFile out = null;
            String rif = sanitizeInput(formatStringtoStringDate_null(from, pattermonthnorm, patternmonthsql));
            if (rif != null) {
                Iterable<String> parameters = Splitter.on("-").split(rif);
                Iterator<String> it = parameters.iterator();
                if (it.hasNext()) {
                    String anno = sanitizeInput(it.next());
                    String mese = sanitizeInput(it.next());
                    String f1 = null;
                    String f2 = null;
                    String primomese = "01";
                    String primogiorno = "01";
                    if (!mese.equals(primomese)) {
                        dt = formatter.parseDateTime(rif + "-01").minusMonths(1);
                        int ultimom = dt.monthOfYear().get();
                        String ultimomese;
                        if (ultimom < 10) {
                            ultimomese = "0" + ultimom;
                        } else {
                            ultimomese = "" + ultimom;
                        }
                        String ultimogiorno = "" + dt.dayOfMonth().withMaximumValue().getDayOfMonth();
                        f1 = anno + "-" + primomese + "-" + primogiorno;
                        f2 = anno + "-" + ultimomese + "-" + ultimogiorno;
                    }
                    out = CORA.mensile(path, rif, f1, f2, anno, mese);
                }
            }
            if (out == null || out.getFile() == null) {
                String msg = "NULL";
                if (out != null) {
                    msg = sanitizeInput(out.getErrore());
                }
                error.add("ERRORE NELL'ELABORAZIONE CORAM MENSILE. CONTROLLARE: " + msg);
            }
        } catch (Exception e) {
            error.add("ERRORE NELL'ELABORAZIONE CORA MENSILE. CONTROLLARE: " + sanitizeInput(e.getMessage()));
        }

        File txt;
        String text;
        if (!error.isEmpty()) {
            text = "SI SONO VERIFICATI ERRORI NELLE ANAGRAFICHE OAM/CORA. CONTROLLARE IL FILE ALLEGATO.";
            txt = new File(sanitizeFilePath(path + RandomStringUtils.randomAlphanumeric(50) + ".txt"));
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                for (String s1 : error) {
                    writer.write(sanitizeInput(s1));
                    writer.newLine();
                }
                writer.flush();
                sendMail("VERIFICA OAM-CORA", text, txt, true);
                txt.deleteOnExit();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void verificaSpreadKO() {
        List<String> errorspread = new ArrayList<>();
        Db db = new Db(host_PROD, false);

        String sql1 = "SELECT c.cod, c.filiale, c.id, c.tipotr, c.data, v.valuta " +
                "FROM ch_transaction c, ch_transaction_valori v " +
                "WHERE c.cod = v.cod_tr AND v.spread LIKE ? AND c.data > ?";

        try (PreparedStatement ps = db.getC().prepareStatement(sql1)) {
            ps.setString(1, "%KO%");
            ps.setString(2, "2024-01-01 00:00:00");

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tipotr = rs.getString("tipotr").equals("B") ? "BUY" : "SELL";
                    String add = "FILIALE: " + sanitizeInput(rs.getString("filiale")) +
                            " - ID: " + sanitizeInput(rs.getString("id")) +
                            " - TIPO: " + tipotr +
                            " - DATA: "
                            + formatStringtoStringDate(sanitizeInput(rs.getString("data")), patternsqldate,
                                    patternnormdate)
                            +
                            " - VALUTA: " + sanitizeInput(rs.getString("valuta"));
                    errorspread.add(add);
                }
                rs.close();
                ps.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        String pathtemp = db.getPath("temp");
        db.closeDB();
        File txt;
        String text;
        if (errorspread.isEmpty()) {
            text = "NESSUNA ANOMALIA CALCOLO SPREAD.";
            sendMail("VERIFICA ANOMALIE CALCOLO SPREAD", text, null, false);
        } else {
            text = "IN ALLEGATO I DETTAGLI DI UNA O PIU' TRANSAZIONI CON UN'ANOMALIA CALCOLO SPREAD.";
            txt = new File(sanitizeFilePath(pathtemp + RandomStringUtils.randomAlphanumeric(50) + ".txt"));
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                for (String s1 : errorspread) {
                    writer.write(sanitizeInput(s1));
                    writer.newLine();
                }
                writer.flush();
                sendMail("VERIFICA ANOMALIE CALCOLO SPREAD", text, txt, false);
                txt.deleteOnExit();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void ultimo_Aggiornamento22() {
        Db db = new Db(host_PROD, false);
        try {
            DateTime dt_now = new DateTime();

            File txt = new File(sanitizeFilePath(db.getPath("temp")
                    + RandomStringUtils.randomAlphanumeric(50) + ".txt"));

            AtomicInteger content = new AtomicInteger(0);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                String sql1 = "SELECT dt_start, filiale, SUBDATE(NOW(), INTERVAL 30 MINUTE) "
                        + "FROM aggiornamenti_mod_verifica WHERE filiale = '000' "
                        + "AND date_start > SUBDATE(NOW(), INTERVAL " + limitMINUTES + " MINUTE) LIMIT 1";

                try (Statement stmt1 = db.getC().createStatement();
                        ResultSet rs1 = stmt1.executeQuery(sql1)) {
                    if (rs1.next()) {
                        System.out.println("ULTIMO AGGIORNAMENTO: " + sanitizeInput(rs1.getString(2)) + " - "
                                + sanitizeInput(rs1.getString(1))
                                + " NOW: " + sanitizeInput(dt_now.toString(patternnormdate)));
                    } else {
                        content.addAndGet(1);
                        String print = "NON RISULTANO AGGIORNAMENTI IN CENTRALE DA PIU' DI " + limitMINUTES
                                + " MINUTI PROVENIENTI DALLE FILIALI. CONTROLLARE.";
                        writer.write(sanitizeInput(print));
                        writer.newLine();
                    }
                }

                String sql2 = "SELECT dt_start, filiale, SUBDATE(NOW(), INTERVAL 30 MINUTE) "
                        + "FROM aggiornamenti_mod_verifica WHERE filiale <> '000' "
                        + "AND date_start > SUBDATE(NOW(), INTERVAL " + limitMINUTES + " MINUTE) LIMIT 1";

                try (Statement stmt2 = db.getC().createStatement();
                        ResultSet rs2 = stmt2.executeQuery(sql2)) {
                    if (rs2.next()) {
                        System.out.println("ULTIMO AGGIORNAMENTO: " + sanitizeInput(rs2.getString(2)) + " - "
                                + sanitizeInput(rs2.getString(1))
                                + " NOW: " + sanitizeInput(dt_now.toString(patternnormdate)));
                    } else {
                        content.addAndGet(1);
                        String print = "NON RISULTANO AGGIORNAMENTI IN CENTRALE DA PIU' DI " + limitMINUTES
                                + " MINUTI DA INVIARE ALLE FILIALI. CONTROLLARE.";
                        writer.write(sanitizeInput(print));
                        writer.newLine();
                    }
                }
            }

            if (content.get() > 0) {
                sendMail(txt);
            }
            txt.deleteOnExit();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            db.closeDB();
        }
    }

    private static void ultimo_Aggiornamento() {
        Db db = new Db(host_PROD, false);
        String sql_LE1 = "SELECT dt_start,filiale FROM aggiornamenti_mod_verifica WHERE filiale = '000' ORDER BY date_start DESC LIMIT 1";

        try {
            ResultSet rs = db.getC().createStatement().executeQuery(sql_LE1);
            File txt = new File(db.getPath("temp")
                    + RandomStringUtils.randomAlphanumeric(50) + ".txt");
            AtomicInteger content;
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                content = new AtomicInteger(0);
                DateTime dt_now = new DateTime();
                if (rs.next()) {
                    String last = rs.getString(1);
                    DateTime dt_last = Utility.formatStringtoStringDate(last, patternnormdate);
                    int m = Minutes.minutesBetween(dt_last, dt_now).getMinutes();
                    if (m >= limitMINUTES) {
                        content.addAndGet(1);
                        String print = "NON RISULTANO AGGIORNAMENTI IN CENTRALE DA PIU' DI " + limitMINUTES
                                + " MINUTI PROVENIENTI DALLE FILIALI. CONTROLLARE.";
                        writer.write(print);
                        writer.newLine();
                    } else {
                        System.out.println("ULTIMO AGGIORNAMENTO: " + rs.getString(2) + " - " + rs.getString(1)
                                + " NOW: " + dt_now.toString(patternnormdate));
                    }
                } else {
                    System.err.println("ERRORE-1");
                } // String sql_LE2 = "SELECT * FROM last_agg_f;";
                String sql_LE2 = "SELECT dt_start,filiale FROM aggiornamenti_mod_verifica WHERE filiale <> '000' ORDER BY date_start DESC LIMIT 1";
                System.out.println("mactest.VerificaAggiornamenti.ultimo_Aggiornamento() " + sql_LE2);
                // String sql_LE2 = "SELECT STR_TO_DATE(dt_start, '%d/%m/%Y
                // %H:%i:%s'),dt_start,filiale FROM aggiornamenti_mod "
                // + "WHERE fg_stato='1' AND filiale<>'000' ORDER BY STR_TO_DATE(dt_start,
                // '%d/%m/%Y %H:%i:%s') DESC LIMIT 1";
                //
                ResultSet rs2 = db.getC().createStatement().executeQuery(sql_LE2);
                if (rs2.next()) {
                    String last = rs2.getString(1);
                    DateTime dt_last = Utility.formatStringtoStringDate(last, patternnormdate);
                    // DateTime dt_last = Utility.formatStringtoStringDate(last, patternsqldate);
                    int m = Minutes.minutesBetween(dt_last, dt_now).getMinutes();
                    if (m >= limitMINUTES) {
                        content.addAndGet(1);
                        String print = "NON RISULTANO AGGIORNAMENTI IN CENTRALE DA PIU' DI " + limitMINUTES
                                + " MINUTI DA INVIARE ALLE FILIALI. CONTROLLARE.";
                        writer.write(print);
                        writer.newLine();
                    } else {
                        System.out.println("ULTIMO AGGIORNAMENTO: " + rs2.getString(2) + " - " + rs2.getString(1)
                                + " NOW: " + dt_now.toString(patternnormdate));
                        // System.out.println("ULTIMO AGGIORNAMENTO: " + rs2.getString(3) + " - " +
                        // rs2.getString(2) + " NOW: " + dt_now.toString(patternnormdate));
                    }
                } else {
                    System.err.println("ERRORE-2");
                }
            }
            if (content.get() > 0) {
                sendMail(txt);
            }
            txt.deleteOnExit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        db.closeDB();
    }

    public static void verifica_Aggiornamenti22() {
        try {
            Db db = new Db(host_PROD, false);
            List<String> filialidanoncontrollare = Arrays.asList(db.getConf("filialinoncontrollare").split(","));
            String el1 = StringUtils.replace(filialidanoncontrollare.toString(), "[", "(");
            el1 = StringUtils.replace(el1, "]", ")");

            File txt = new File(sanitizeFilePath(db.getPath("temp") + randomAlphanumeric(50) + ".txt"));
            AtomicInteger index = new AtomicInteger(0);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                String sql1 = "SELECT b.cod FROM branch b WHERE b.fg_annullato='0' AND b.cod NOT IN (?)";
                try (PreparedStatement pstmt = db.getC().prepareStatement(sql1)) {
                    // Imposta il parametro per il PreparedStatement
                    pstmt.setString(1, sanitizeInput(el1));
                    try (ResultSet rs1 = pstmt.executeQuery()) {
                        while (rs1.next()) {
                            StatusBranch sb1 = new StatusBranch();

                            String fildest = sanitizeInput(rs1.getString(1));
                            sb1.setCod(fildest);
                            if (sb1.getCod().equals("000")) {
                                sb1.setIp("AWS");
                            } else {
                                sb1.setIp(db.getIpFiliale(sb1.getCod()).get(0)[1]);
                            }

                            String sql2 = "SELECT count(cod) FROM aggiornamenti_mod WHERE fg_stato='0' AND filiale = ? AND now()>STR_TO_DATE(dt_start, '%d/%m/%Y %H:%i:%s')";
                            try (PreparedStatement pstmt2 = db.getC().prepareStatement(sql2)) {
                                pstmt2.setString(1, sanitizeInput(fildest));
                                try (ResultSet rs2 = pstmt2.executeQuery()) {
                                    if (rs2.next()) {
                                        sb1.setAggto(rs2.getInt(1));
                                    }
                                }
                            }

                            if (!sb1.getCod().equals("000")) {
                                Db dbfil = new Db("//" + sb1.getIp() + ":3306/maccorp", true);
                                if (dbfil.getC() != null) {
                                    String sql3 = "SELECT count(cod) FROM aggiornamenti_mod Where fg_stato='0'";
                                    try (PreparedStatement pstmt3 = dbfil.getC().prepareStatement(sql3)) {
                                        try (ResultSet rs3 = pstmt3.executeQuery()) {
                                            if (rs3.next()) {
                                                sb1.setAggfrom(rs3.getInt(1));
                                            }
                                        }
                                    }
                                    dbfil.closeDB();
                                    sb1.setRagg(true);
                                } else {
                                    sb1.setRagg(false);
                                }
                            }

                            if (sb1.getAggfrom() >= limitAGG || sb1.getAggto() >= limitAGG) {
                                try {
                                    String print = "FILIALE " + sb1.getCod() + " - DA RICEVERE/CARICARE: "
                                            + sb1.getAggfrom()
                                            + " ; DA INVIARE/ESEGUIRE: " + sb1.getAggto() + " - RAGGIUNGIBILITA': "
                                            + sb1.isRagg();
                                    writer.write(sanitizeInput(print));
                                    writer.newLine();
                                    index.addAndGet(1);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }

                            }
                        }
                    }
                }
            }
            db.closeDB();
            if (index.get() > 0) {
                sendMail(txt);
            }
            txt.deleteOnExit();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void verifica_Aggiornamenti() {

        try {
            Db db = new Db(host_PROD, false);
            List<String> filialidanoncontrollare = Arrays.asList(db.getConf("filialinoncontrollare").split(","));
            db.closeDB();
            AtomicInteger index = new AtomicInteger(0);
            List<StatusBranch> complete = agg(null);
            File txt = new File(db.getPath("temp")
                    + RandomStringUtils.randomAlphanumeric(50) + ".txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(txt))) {
                complete.forEach(agg -> {
                    if (agg.getAggfrom() >= limitAGG || agg.getAggto() >= limitAGG) {
                        if (!filialidanoncontrollare.contains(agg.getCod())) {
                            try {
                                String print = "FILIALE " + agg.getCod() + " - DA RICEVERE/CARICARE: "
                                        + agg.getAggfrom() + " ; DA INVIARE/ESEGUIRE: " + agg.getAggto()
                                        + " - RAGGIUNGIBILITA': " + agg.isRagg();
                                writer.write(print);
                                writer.newLine();
                                index.addAndGet(1);
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                });
            }
            if (index.get() > 0) {
                sendMail(txt);
            }
            txt.deleteOnExit();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    // public static void main(String[] args) {
    //
    // }
}
