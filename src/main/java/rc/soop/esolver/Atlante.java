/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import com.google.common.util.concurrent.AtomicDouble;
import static rc.soop.esolver.ESolver.pathin;
import static rc.soop.esolver.ESolver.sanitizeFilePath;
import static rc.soop.esolver.ESolver.sanitizeInput;
import static rc.soop.esolver.ESolver.sanitizePath;
import static rc.soop.esolver.ESolver.separator;
import static rc.soop.esolver.ESolver.tag_GEN;
import static rc.soop.esolver.ESolver.tag_RIG;
import static rc.soop.esolver.ESolver.tag_TES;
import static rc.soop.esolver.Util.checkTXT;
import static rc.soop.esolver.Util.estraiEccezione;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.formatStringtoStringDate;
import static rc.soop.esolver.Util.log;
import static rc.soop.esolver.Util.pattern4;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.patternsqldate;
import static rc.soop.esolver.Util.patternyear;
import static rc.soop.esolver.Util.rilasciaFileEsolver;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import static rc.soop.esolver.Util.test;
import static rc.soop.esolver.Util.verificaClientNumber;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class Atlante {

    private final String mastroCLienti = "110303";
    private static final String scontrino_AG_ATL = "703";
    private static final String tag_IVA = "IVA";

    private List<Atl_dati_clienti> dc_removeBT1(List<Atl_dati_clienti> daticlienti) {
        List<Atl_dati_clienti> out = new ArrayList<>();
        daticlienti.forEach(df -> {
            boolean is_BT = df.getClientcode().startsWith("010 1 1") || df.getClientcode().startsWith("010 8 1");
            if (!is_BT) {
                out.add(df);
            }
        });
        return out;
    }

    private boolean is_BT1(List<Atl_dati_fatture> datifattura, List<Atl_dati_clienti> daticlienti) {
        AtomicInteger in = new AtomicInteger(0);
        if (datifattura != null) {
            datifattura.forEach(incassiATL -> {
                incassiATL.getDetails().forEach(detREG -> {
                    boolean is_BT = detREG.getContoreg().startsWith("010 1 1")
                            || detREG.getContoreg().startsWith("010 8 1");
                    if (is_BT) {
                        in.addAndGet(1);
                    }
                });
            });
        }
        if (daticlienti != null) {
            daticlienti.forEach(df -> {

                boolean is_BT = df.getClientcode().startsWith("010 1 1") || df.getClientcode().startsWith("010 8 1");
                System.out.println(" esolver.Atlante.is_BT1() " + df.getClientcode() + " -- " + is_BT);

                if (is_BT) {
                    in.addAndGet(1);
                    daticlienti.remove(in);
                }
            });
        }
        return in.get() > 0;
    }

    // ATLANTE
    public File FILEP2A(String path, String data, String anno, Branch filiale,
            List<Atl_dati_fatture> datifattura) {

        if (datifattura.isEmpty()) {
            return null;
        }
        try {
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P2A_eSol.txt");
            PrintWriter writer = new PrintWriter(f);
            AtomicInteger nreg = new AtomicInteger(1);

            datifattura.forEach(cor1 -> {
                String d1 = formatStringtoStringDate(cor1.getDatereg(), patternsqldate, patternnormdate_filter);

                writer.println(tag_TES + separator + scontrino_AG_ATL + separator + anno
                        + separator + d1 + separator + nreg.get() + separator + separator
                        + separator + separator + separator + separator
                        + separator + separator + separator + separator + separator + separator
                        + separator + "CORRISPETTIVI " + filiale.getDe_branch() + separator);

                AtomicDouble total = new AtomicDouble(0.0);
                AtomicDouble total2 = new AtomicDouble(0.0);

                AtomicDouble imponibile = new AtomicDouble(0.0);
                AtomicDouble imposta = new AtomicDouble(0.0);
                AtomicDouble importo = new AtomicDouble(0.0);

                StringBuilder conto_esover = new StringBuilder("");
                StringBuilder codiceiva = new StringBuilder("");
                StringBuilder conto_cassa = new StringBuilder("");

                cor1.getDetails().forEach(details_N -> {
                    if (details_N.getSegnoreg().equals("D")) {
                        conto_cassa.append(details_N.getContoreg());
                        importo.addAndGet(fd(details_N.getImporto()));
                        total.addAndGet(fd(details_N.getImporto()));
                    } else if (details_N.getTipoconto().equals("G")) {
                        conto_esover.append(details_N.getContoreg());
                        imponibile.addAndGet(fd(details_N.getImporto()));
                        codiceiva.append(details_N.getIvareg());
                        total2.addAndGet(fd(details_N.getImporto()));
                    } else if (details_N.getTipoconto().equals("I")) {
                        imposta.addAndGet(fd(details_N.getImporto()));
                        total2.addAndGet(fd(details_N.getImporto()));
                    }
                });

                if (total.get() == 0) {
                    total.addAndGet(total2.get());
                }

                String[] conti = { conto_esover.toString(), conto_cassa.toString() };

                // if (total.get() < 0) {
                //
                // total.set(total.get() * -1.00);
                //
                // if (imponibile.get() < 0) {
                // imponibile.set(imponibile.get() * -1.00);
                // }
                //
                // if (imposta.get() < 0) {
                // imposta.set(imposta.get() * -1.00);
                // }
                //
                // if (importo.get() < 0) {
                // importo.set(importo.get() * -1.00);
                // }
                //
                // conti[0] = conto_cassa.toString();
                // conti[1] = conto_esover.toString();
                // log.log(Level.WARNING, "{0}: - FILEP2A() INVERTO I CONTI E METTO IMPORTI
                // POSITIVI", filiale);
                // }
                if (total.get() < 0) {
                    writer.println(tag_RIG
                            + separator + scontrino_AG_ATL + separator + anno + separator + d1
                            + separator + nreg.get() + separator + conti[0]
                            + separator + StringUtils.replace(roundDoubleandFormat(total.get(), 2), ".", ",")
                            + separator + codiceiva.toString()
                            + separator + StringUtils.replace(roundDoubleandFormat(imponibile.get(), 2), ".", ",")
                            + separator + StringUtils.replace(roundDoubleandFormat(imposta.get(), 2), ".", ",")
                            + separator + filiale.getCod()
                            + separator + "CORRISPETTIVI " + filiale.getDe_branch() + " ATLANTE" + separator + separator
                            + separator + separator + separator + separator + separator);

                    if (importo.get() != 0) {
                        writer.println(tag_GEN + separator + scontrino_AG_ATL + separator + anno + separator + d1
                                + separator
                                + nreg.get() + separator + separator + separator + separator + separator + separator
                                + separator + separator + conti[1] + separator
                                + filiale.getCod() + separator + separator
                                + StringUtils.replace(roundDoubleandFormat(importo.get() * -1.00, 2), ".", ",")
                                + separator
                                + "INCASSO CORRISPETTIVI ATLANTE" + separator + separator + filiale.getCod());
                    }

                    // RIG;703;2019;25/06/2019;8; 188 1 1;-30,00;75;-30,00;0,00;079;CORRISPETTIVI
                    // Milano Duomo ATLANTE;;;;;;;
                    // GEN;703;2019;25/06/2019;8;;;;;;;; 0 6 1 2 3;079;; 30,00;INCASSO CORRISPETTIVI
                    // ATLANTE;;079
                    //
                    // RIG;703;2019;25/06/2019;6;188 1 1;-30,00;75;-30,00;0,00;079;CORRISPETTIVI
                    // Milano Duomo ATLANTE;;;;;;;
                    // GEN;703;2019;25/06/2019;6;;;;;;;;0 6 1 2 3;079;-30,00;; raf - INCASSO
                    // CORRISPETTIVI ATLANTE;;079
                } else {
                    // CASO NORMALE
                    writer.println(tag_RIG
                            + separator + scontrino_AG_ATL + separator + anno + separator + d1
                            + separator + nreg.get() + separator + conti[0]
                            + separator + StringUtils.replace(roundDoubleandFormat(total.get(), 2), ".", ",")
                            + separator + codiceiva.toString()
                            + separator + StringUtils.replace(roundDoubleandFormat(imponibile.get(), 2), ".", ",")
                            + separator + StringUtils.replace(roundDoubleandFormat(imposta.get(), 2), ".", ",")
                            + separator + filiale.getCod()
                            + separator + "CORRISPETTIVI " + filiale.getDe_branch() + " ATLANTE" + separator + separator
                            + separator + separator + separator + separator + separator);

                    if (importo.get() != 0) {
                        writer.println(tag_GEN + separator + scontrino_AG_ATL + separator + anno + separator + d1
                                + separator
                                + nreg.get() + separator + separator + separator + separator + separator + separator
                                + separator + separator + conti[1] + separator
                                + filiale.getCod() + separator
                                + StringUtils.replace(roundDoubleandFormat(importo.get(), 2), ".", ",") + separator
                                + separator
                                + "INCASSO CORRISPETTIVI ATLANTE" + separator + separator + filiale.getCod());
                    }
                }

                nreg.addAndGet(1);

            });
            writer.close();

            if (nreg.get() > 1) {
                return f;
            }
            f.delete();
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;

    }

    /////////
    public File FILEP4A(String path, String data, String anno, Branch filiale,
            List<Atl_dati_fatture> datifattura) {

        if (datifattura.isEmpty()) {
            return null;
        }

        try {

            // List<String> branchList_BT = branchList_BT();
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P4A_eSol.txt");
            PrintWriter writer = new PrintWriter(f);
            AtomicInteger integ = new AtomicInteger(1);
            datifattura.forEach(incassiATL -> {
                incassiATL.getDetails().forEach(detREG -> {

                    String d1 = formatStringtoStringDate(incassiATL.getDatereg(), patternsqldate,
                            patternnormdate_filter);

                    if (detREG.getCategory().equals("TUR")) {
                        String importo = StringUtils.replace(detREG.getImporto(), ".", ",");
                        importo = importo.replace("-", "");
                        String valore[] = { importo.trim(), "" };

                        if (!detREG.getSegnoreg().equals("D")) {
                            valore = new String[2];
                            valore[0] = "";
                            valore[1] = importo.trim();
                            if (fd(detREG.getImporto()) < 0) {
                                valore[0] = importo.trim();
                                valore[1] = "";
                            }
                        }

                        String[] conti = { detREG.getContoreg(), "" };
                        // if (detREG.getContoreg().startsWith("010")) {
                        // if (!branchList_BT.contains(filiale.getCod())) {
                        // conti[0] = mastroCLienti;
                        // conti[1] = detREG.getContoreg();
                        // }
                        // }

                        writer.println(tag_GEN + separator + "TUR" + separator + anno + separator + d1 + separator
                                + integ.get() + separator
                                + conti[0] + separator + incassiATL.getBranchid() + separator + conti[1] + separator
                                + valore[0] + separator + valore[1] + separator
                                + incassiATL.getBranchid() + separator + detREG.getDesc().toUpperCase());

                    } else {

                        String[] conti = { detREG.getContoreg(), "" };
                        if (detREG.getContoreg().startsWith("010")) {
                            // String contosenzaspazi = StringUtils.deleteWhitespace(detREG.getContoreg());
                            boolean is_BT = detREG.getContoreg().startsWith("010 1 1")
                                    || detREG.getContoreg().startsWith("010 8 1");
                            if (!is_BT) {
                                // if (!branchList_BT.contains(filiale.getCod())) {
                                conti[0] = mastroCLienti;
                                conti[1] = detREG.getContoreg();
                            }
                        }

                        if (fd(detREG.getImporto()) < 0) {
                            log.log(Level.WARNING, "{0} - LINEA {1} INVERTO LE POSIZIONI E METTO IMPORTI POSITIVI",
                                    new Object[] { f.getName(), integ.get() });
                        }

                        if (detREG.getSegnoreg().equals("D")) {

                            String importo = StringUtils.replace(detREG.getImporto(), ".", ",");
                            importo = importo.replace("-", "");
                            String valore[] = { importo.trim(), "" };

                            if (fd(detREG.getImporto()) < 0) {
                                valore[0] = "";
                                valore[1] = importo.trim();
                            }

                            writer.println(tag_GEN + separator + "IAT" + separator + anno + separator
                                    + d1 + separator + integ.get() + separator
                                    + conti[0] + separator + incassiATL.getBranchid() + separator + conti[1] + separator
                                    + valore[0] + separator + valore[1] + separator
                                    + incassiATL.getBranchid() + separator + detREG.getDesc().toUpperCase());
                        } else {

                            String importo = StringUtils.replace(detREG.getImporto(), ".", ",");
                            importo = importo.replace("-", "");

                            String valore[] = { "", importo.trim() };
                            if (fd(detREG.getImporto()) < 0) {
                                valore[0] = importo.trim();
                                valore[1] = "";
                            }

                            writer.println(tag_GEN + separator + "IAT" + separator + anno + separator + d1 + separator
                                    + integ.get() + separator
                                    + conti[0] + separator + incassiATL.getBranchid() + separator + conti[1] + separator
                                    + valore[0] + separator + valore[1] + separator
                                    + incassiATL.getBranchid() + separator + detREG.getDesc().toUpperCase());
                        }
                    }

                });
                integ.addAndGet(1);
            });
            writer.close();
            if (integ.get() > 1) {
                return f;
            }
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    /////////
    public File FILEP6A(String path, String data, Branch filiale,
            List<Atl_dati_fatture> datifattura) {

        if (datifattura.isEmpty()) {
            return null;
        }

        try {

            boolean is_BT = is_BT1(datifattura, null);

            // List<String> branchList_BT = branchList_BT();
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P6_eSol.txt");
            PrintWriter writer = new PrintWriter(f);
            String tiporiga = "30";

            datifattura.forEach(df -> {

                StringBuilder desc = new StringBuilder("");
                StringBuilder starttipodoc = new StringBuilder("");

                Atl_details_dati_fatture matchingObject = df.getDetails().get(0);
                if (matchingObject != null) {
                    String contocliente = matchingObject.getContoreg();
                    String causaletestata = matchingObject.getDesc();

                    if (df.getTipomov().equals("F")) {
                        desc.append("Fattura Fiscale numero " + df.getNumreg() + " del "
                                + formatStringtoStringDate(df.getDatereg(), patternsqldate, patternnormdate_filter));
                    } else if (df.getTipomov().equals("N")) {
                        desc.append("Nota di credito Fiscale numero " + df.getNumreg() + " del "
                                + formatStringtoStringDate(df.getDatereg(), patternsqldate, patternnormdate_filter));
                        starttipodoc.append("NC");
                    }

                    Tipodoc tipodoc = new Tipodoc(starttipodoc.toString() + "710");

                    if (df.getSezionale().equalsIgnoreCase("V02")) {
                        tipodoc.setId(starttipodoc.toString() + "TER");
                    } else {

                        if (df.getBranchid().equals("AMM") || is_BT) {
                            // if (df.getBranchid().equals("AMM") ||
                            // branchList_BT.contains(filiale.getCod())) {
                            tipodoc.setId(starttipodoc.toString() + "BTR");
                        } else {
                            tipodoc.setId(starttipodoc.toString() + "FTV");
                        }
                    }

                    // if (df.getBranchid().equals("AMM") || df.getBranchid().equals("191")) {
                    // if (df.getBranchid().equals("AMM") ||
                    // branchList_BT.contains(filiale.getCod())) {
                    // tipodoc.setId(starttipodoc.toString() + "BTR");
                    // } else if (df.getSezionale().equalsIgnoreCase("V01")) {
                    // tipodoc.setId(starttipodoc.toString() + "FTV");
                    // } else if (df.getSezionale().equalsIgnoreCase("V02")) {
                    // tipodoc.setId(starttipodoc.toString() + "TER");
                    // }
                    String TES = tag_TES + separator
                            + tipodoc.getId() + separator
                            + formatStringtoStringDate(df.getDatereg(), patternsqldate, patternnormdate_filter)
                            + separator
                            + df.getNumreg() + separator
                            + contocliente + separator + separator + separator + separator + separator + separator
                            + separator + separator
                            + causaletestata + separator + separator + separator + separator + df.getBranchid()
                            + separator
                            + df.getSezionale();

                    writer.println(TES.toUpperCase());

                    df.getDetails().forEach(det1 -> {
                        String RIG = tag_RIG + separator
                                + tipodoc.getId() + separator
                                + formatStringtoStringDate(df.getDatereg(), patternsqldate, patternnormdate_filter)
                                + separator
                                + df.getNumreg() + separator + separator
                                + tiporiga + separator
                                + det1.getContoreg() + separator
                                + StringUtils.replace(det1.getImporto(), ".", ",") + separator
                                + det1.getIvareg() + separator + filiale.getCod() + separator + separator + separator
                                + separator
                                + desc.toString() + separator + separator + separator + separator;
                        if (!contocliente.equals(det1.getContoreg())) {
                            writer.println(RIG.toUpperCase());
                        }
                    });

                    df.getDetailsiva().forEach(det1 -> {
                        String iva_value = StringUtils.replace(roundDoubleandFormat(fd(det1.getIva()), 2), ".", ",");
                        String IVA = tag_IVA + separator
                                + tipodoc.getId() + separator
                                + formatStringtoStringDate(df.getDatereg(), patternsqldate, patternnormdate_filter)
                                + separator
                                + df.getNumreg() + separator + separator + separator
                                + det1.getContoiva() + separator + separator + separator + separator + separator
                                + separator + separator + separator
                                + det1.getCodeiva() + separator + StringUtils.replace(det1.getImponibile(), ".", ",")
                                + separator + iva_value + separator;
                        writer.println(IVA.toUpperCase());
                    });

                } else {
                    log.log(Level.SEVERE, "{0} RIGA CLIENTE NON DISPONIBILE", df.getCod());
                }
            });

            writer.close();
            if (f.length() > 0) {
                return f;
            }
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    /////////
    public File FILEP7A(String path, String data, Branch filiale,
            List<Atl_dati_clienti> daticlienti) {

        daticlienti = dc_removeBT1(daticlienti);

        boolean is_BT1 = is_BT1(null, daticlienti);

        if (daticlienti.isEmpty() || is_BT1) {
            // if (daticlienti.isEmpty() || branchList_BT().contains(filiale.getCod())) {
            return null;
        }

        try {
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P7_eSol.txt");
            try (PrintWriter writer = new PrintWriter(f)) {
                daticlienti.forEach(df -> {
                    String s1 = tag_GEN + separator;
                    String s3 = df.getRagsoc1() + separator;
                    String s4 = df.getRagsoc2() + separator;
                    String s5 = df.getAddress() + separator;
                    String s6 = df.getCity() + separator;
                    String s7 = df.getCountry() + separator;
                    String s8 = separator;
                    String s9 = df.getClientcode() + separator;
                    String s10 = df.getZipcode() + separator;
                    String s11 = df.getDistrict() + separator;
                    // String s12 = separator;
                    String[] s12_s13 = verificaClientNumber(df.getClientnumber());
                    String s12 = s12_s13[0] + separator;
                    String s13 = s12_s13[1] + separator;
                    // String s14 = df.getFatelet();
                    String s14 = "2" + separator;
                    String s15 = "0";
                    if (!df.getCountry().equalsIgnoreCase("IT")) {
                        s10 = separator; // BLANK
                        s11 = separator; // BLANK
                        s12 = separator; // BLANK
                        s13 = separator; // BLANK
                        s14 = "0" + separator;
                        s15 = "1";
                    }

                    String output = (s1 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + s10 + s11 + s12 + s13 + s14 + s15)
                            .toUpperCase();
                    writer.println(output);
                });
            }
            return f;
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public static void rilasciaAtlante(boolean release) {

        Db_Master db = new Db_Master();
        // DateTime dt = db.getNowDT();
        DateTime dt = db.getNowDT().minusDays(1);
        // DateTime dt = getDateRif("24/02/2022");
        db.closeDB();

        String from = dt.toString(patternnormdate_filter);
        String anno = dt.toString(patternyear);
        String data1 = dt.toString(patternsql);
        log.warning("START...");
        String path = pathin + data1 + File.separator;
        path = sanitizePath(path);
        try {
            new File(path).mkdirs();
            log.log(Level.WARNING, "DIRECTORY {0} CREATA", path);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "DIRECTORY {0} ERRORE", path);
        }

        Db_ATL dbA = new Db_ATL();
        int setDate = dbA.setDateCorrect();
        dbA.closeDB();
        log.log(Level.WARNING, "ATLANTE CORRETTI {0} RECORD", setDate);
        Db_Master db1 = new Db_Master();
        ArrayList<Branch> branch = db1.list_branch_enabled();
        db1.closeDB();

        Atlante es = new Atlante();
        List<File> atl_ok = new ArrayList<>();
        for (int i = 0; i < branch.size(); i++) {
            Branch b1 = branch.get(i);
            if (test) {
                if (b1.getCod().equals("043") || b1.getCod().equals("079")) {

                } else {
                    continue;
                }
            }
            Db_ATL db17 = new Db_ATL();
            List<Atl_dati_fatture> daticorrispettivi_P2A = db17.atl_f1_P2(data1, b1.getCod());
            List<Atl_dati_fatture> daticollegati_P4A = db17.atl_f1_P4(data1, b1.getCod());
            List<Atl_dati_fatture> datifattura_P6A = db17.atl_f1_P6(data1, b1.getCod());
            List<Atl_dati_clienti> daticlienti_P7A = db17.atl_c1(data1, b1.getCod());
            db17.closeDB();
            // //
            File base64_2A = es.FILEP2A(sanitizeInput(path), sanitizeInput(from), sanitizeInput(anno), b1,
                    daticorrispettivi_P2A);
            File base64_4A = es.FILEP4A(sanitizeInput(path), sanitizeInput(from), anno, b1, daticollegati_P4A);
            File base64_6A = es.FILEP6A(sanitizeInput(path), sanitizeInput(from), b1, datifattura_P6A);
            File base64_7A = es.FILEP7A(sanitizeInput(path), sanitizeInput(from), b1, daticlienti_P7A);
            // //
            log.log(Level.WARNING, "START BRANCH CODE {0}", b1.getCod());
            try {
                if (base64_2A != null) {
                    if (checkTXT(base64_2A)) {
                        // File out2 = new File(pathout + base64_2A.getName());
                        // copyFile_R(base64_2A, out2);
                        // if (checkTXT(out2)) {
                        atl_ok.add(base64_2A);
                        // log.log(Level.INFO, "{0} RILASCIATO.", out2.getName());
                        // }
                        if (release) {
                            if (rilasciaFileEsolver(base64_2A)) {
                                log.log(Level.INFO, "{0} RILASCIATO.", base64_2A.getName());
                            }
                        }
                    }
                }
                if (base64_4A != null) {
                    if (checkTXT(base64_4A)) {
                        // File out4 = new File(pathout + base64_4A.getName());
                        // copyFile_R(base64_4A, out4);
                        // if (checkTXT(base64_4A)) {
                        atl_ok.add(base64_4A);
                        // log.log(Level.INFO, "{0} RILASCIATO.", out4.getName());
                        // }
                        if (release) {
                            if (rilasciaFileEsolver(base64_4A)) {
                                log.log(Level.INFO, "{0} RILASCIATO.", base64_4A.getName());
                            }
                        }
                    }
                }
                if (base64_6A != null) {
                    if (checkTXT(base64_6A)) {
                        // File out6 = new File(pathout + base64_6A.getName());
                        // copyFile_R(base64_6A, out6);
                        // if (checkTXT(out6)) {
                        atl_ok.add(base64_4A);
                        // log.log(Level.INFO, "{0} RILASCIATO.", out6.getName());
                        // }
                        if (release) {
                            if (rilasciaFileEsolver(base64_6A)) {
                                log.log(Level.INFO, "{0} RILASCIATO.", base64_6A.getName());
                            }
                        }
                    }
                }
                if (base64_7A != null) {
                    if (checkTXT(base64_7A)) {
                        // File out7 = new File(pathout + base64_7A.getName());
                        // copyFile_R(base64_7A, out7);
                        // if (checkTXT(out7)) {
                        atl_ok.add(base64_7A);
                        // log.log(Level.INFO, "{0} RILASCIATO.", out7.getName());
                        // }
                        if (release) {
                            if (rilasciaFileEsolver(base64_7A)) {
                                log.log(Level.INFO, "{0} RILASCIATO.", base64_7A.getName());
                            }
                        }
                    }
                }
                // ATLANTE
            } catch (Exception ex) {
                log.severe(estraiEccezione(ex));
            }
            log.log(Level.WARNING, "END BRANCH CODE {0}", b1.getCod());
        }
        if (!release) {
            boolean mail = true; // PRENDERLI DAL CONF
            // MAIL ATLANTE
            if (!atl_ok.isEmpty()) {
                try {
                    String zipFileName = "PROD_" + sanitizeInput(dt.toString(pattern4)) + ".7z";
                    // Sanifica il nome del file
                    File zippato = new File(sanitizeFilePath(path) + sanitizeFileName(zipFileName));

                    SevenZip.compress(atl_ok, sanitizeFilePath(path),sanitizeFileName (zipFileName));
                    if (zippato.length() > 0) {
                        if (mail) {
                            String[] addr = { "alena@maccorp.it", "lspalvieri@maccorp.it" };
                            Util.sendMailHtml(addr, "ESOLVER ATLANTE " + sanitizeInput(from),
                                    "IN ALLEGATO IL FILE PER ATLANTE RELATIVO ALLA DATA " + sanitizeInput(from),
                                    zippato);
                        } else {
                            log.log(Level.WARNING, "FILE ZIP ATLANTE: {0}", zippato.getPath());
                        }
                    }
                } catch (IOException ex) {
                    log.log(Level.SEVERE, "{0}: {1}",
                            new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
                }
            } else {
                log.warning("NESSUN FILE GENERATO PER ATLANTE.");
            }
        } else {
            log.warning("SOLO RILASCIO FILE.");
        }

        // MAIL ATLANTE
        log.log(Level.WARNING, "END {0}", from);
    }

    public static String sanitizeFileName(String fileName) {
        String illegalChars = "[<>:\"/\\|?*]";
        fileName = fileName.replaceAll(illegalChars, "_");
        return fileName;
    }

}
