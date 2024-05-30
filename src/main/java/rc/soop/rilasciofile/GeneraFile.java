/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import rc.soop.esolver.Branch;
import rc.soop.esolver.NC_category;

import static rc.soop.esolver.ESolver.sanitizeInput;
import static rc.soop.esolver.ESolver.sanitizePath;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import rc.soop.qlik.LoggerNew;
import static rc.soop.rilasciofile.Utility.patternmonthsql;
import java.io.File;
import java.util.ArrayList;
import java.util.Locale;
import java.util.logging.Level;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import rc.soop.sftp.SftpMaccorp;

/**
 *
 * @author rcosco
 */
public class GeneraFile {

    public static String sanitizeFileName(String fileName) {
        // Caratteri non consentiti in un nome di file
        String regex = "[\\\\/:*?\"<>|]";
        // Sostituisci i caratteri non consentiti con uno spazio vuoto
        return fileName.replaceAll(regex, "");
    }

    private static void ondemand(String anno) {
        GeneraFile gf = new GeneraFile();
        gf.setIs_IT(true);
        gf.setIs_UK(false);
        gf.setIs_CZ(false);
        DatabaseCons db = new DatabaseCons(gf);
        String path = db.getPath("temp");
        // ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
        ArrayList<NC_transaction> result = db.query_NC_ondemand(anno);
        if (!result.isEmpty()) {
            String nomereport = "LIST TRANSACTION NOCHANGE " + anno + "_2.xlsx";
            File Output = new File(path + nomereport);
            Excel.excel_transactionnc_list(gf, Output, result);
            System.out.println("com.fl.upload.GeneraFile.ondemand() " + path + nomereport);
        }
        db.closeDB();
    }

    public void rilasciafile(GeneraFile gf, String tipofile) {
        DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue().withHourOfDay(0)
                .withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        DateTime ieri = new DateTime().minusDays(1);
        rilasciafile(gf, tipofile, iniziomese, ieri);
    }

    public void rilasciafile(GeneraFile gf, String tipofile, DateTime iniziomese, DateTime ieri) {
        gf.logger.log.warning("START");

        gf.setIs_IT(true);
        gf.setIs_UK(false);
        gf.setIs_CZ(false);

        DatabaseCons db = new DatabaseCons(gf);
        String datecreation = new DateTime().withZone((DateTimeZone.forID("Europe/Rome"))).toString("yyyyMMddHHmmss");
        // DateTime iniziomese = new
        // DateTime().minusDays(1).dayOfMonth().withMinimumValue();
        // System.out.println("com.fl.upload.GeneraFile.rilasciafile() "+);
        String mesemysql = iniziomese.toString(patternmonthsql);

        String meseriferimento = iniziomese.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
        String annoriferimento = iniziomese.year().getAsText(Locale.ITALY).toUpperCase();

        String data1 = sanitizeInput(iniziomese.toString(patternsql));
        String data2 = sanitizeInput(ieri.toString(patternsql));

        String d3 = sanitizeInput(iniziomese.toString(patternnormdate_filter));
        String d4 = sanitizeInput(ieri.toString(patternnormdate_filter));
        String meseanno_prec = iniziomese.minusMonths(1).toString(patternmonthsql);
        String anno_rif = iniziomese.minusMonths(1).year().getAsText();
        String meseriferimento_prec = iniziomese.minusMonths(1).monthOfYear().getAsText(Locale.ITALY).toUpperCase();
        String path = sanitizeInput(db.getPath("temp"));
        ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
        ArrayList<String> filiali_soloROMA = db.list_branch_RM();
        ArrayList<Branch> allenabledbr = db.list_branch();

        // data1 = "2019-10-01";
        // data2 = "2019-10-31";
        switch (tipofile) {
            case "LOC": {
                // LIST OPEN CLOSE - DA INIZIO MESE A IERI
                ArrayList<Openclose> result = db.query_oc(data1, data2);
                ArrayList<Till> listTill_complete = db.list_till();
                String base64;

                if (!result.isEmpty()) {
                    String nomereport = sanitizeFileName("LIST OPEN CLOSE DA " + data1 + " A " + data2 + "_"
                            + sanitizeFileName(datecreation) + ".xlsx");
                    String percorsoOutput = sanitizePath(path) + File.separator + nomereport;
                    File Output = new File(percorsoOutput);
                    base64 = Excel.excel_openclose(gf, Output, result, listTill_complete);
                    if (base64 != null) {
                        boolean es = rilasciasftp(Output, meseriferimento + "/LIST OPEN CLOSE", annoriferimento, gf);
                        if (es) {
                            gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                        } else {
                            gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                        }
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.WARNING, "NESSUN DATO TROVATO. {0}", tipofile);
                }
                break;
            }
            case "TCH": {
                // LIST TRANSACTION CHANGE - DA INIZIO MESE A IERI
                ArrayList<Ch_transaction> result = db.query_transaction_ch_new(data1, data2, br1);
                String base64;
                if (!result.isEmpty()) {
                    // Sanifica l'input per evitare potenziali attacchi di directory traversal e
                    // injection
                    String nomereport = sanitizeFileName("LIST TRANSACTION CHANGE DA " + data1 + " A " + data2 + "_"
                            + sanitizeFileName(datecreation) + ".xlsx");
                    String percorsoOutput = sanitizePath(path) + File.separator + nomereport;
                    File Output = new File(percorsoOutput);
                    base64 = Excel.excel_transaction_listEVO(gf, Output, result);
                    if (base64 != null) {
                        boolean es = rilasciasftp(Output, meseriferimento + "/LIST TRANSACTION CHANGE", annoriferimento,
                                gf);
                        if (es) {
                            gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                        } else {
                            gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                        }
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.WARNING, "NESSUN DATO TROVATO. {0}", tipofile);
                }
                break;
            }

            case "TNC": {
                // LIST TRANSACTION NOCHANGE - DA INIZIO MESE A IERI
                ArrayList<NC_transaction> result = db.query_NC_transaction_NEW(data1, data2, br1, "NO");
                String base64;
                if (!result.isEmpty()) {
                    // Sanifica l'input per evitare potenziali attacchi di directory traversal e
                    // injection
                    String nomereport = sanitizeFileName("LIST TRANSACTION NOCHANGE DA " + data1 + " A " + data2 + "_"
                            + sanitizeFileName(datecreation) + ".xlsx");
                    String percorsoOutput = sanitizePath(path) + File.separator + nomereport;
                    File Output = new File(percorsoOutput);
                    base64 = Excel.excel_transactionnc_list(gf, Output, result);
                    if (base64 != null) {
                        boolean es = rilasciasftp(Output, meseriferimento + "/LIST TRANSACTION NOCHANGE",
                                annoriferimento, gf);
                        if (es) {
                            gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                        } else {
                            gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                        }
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.WARNING, "NESSUN DATO TROVATO. {0}", tipofile);
                }
                break;
            }

            case "SB1": {
                // SB TRANSACTION - DA INIZIO MESE A IERI
                TillTransactionListBB_value pdfsell = db.list_SBTransactionList(br1, data1, data2, allenabledbr);
                if (pdfsell != null) {
                    ArrayList<String> datifooter = new ArrayList<>();
                    datifooter.add(pdfsell.getTransactionnumberresidentbuy());
                    datifooter.add(pdfsell.getTransactionnumbernonresidentbuy());
                    datifooter.add(pdfsell.getInternetbookingamountyes());
                    datifooter.add(pdfsell.getInternetbookingnumberyes());
                    datifooter.add(pdfsell.getPosbuyamount());
                    datifooter.add(pdfsell.getPosbuynumber());
                    datifooter.add(pdfsell.getBankbuyamount());
                    datifooter.add(pdfsell.getBankbuynumber());
                    datifooter.add(pdfsell.getTransactionnumberresidentsell());
                    datifooter.add(pdfsell.getTransactionnumbernonresidentsell());
                    datifooter.add(pdfsell.getInternetbookingamountno());
                    datifooter.add(pdfsell.getInternetbookingnumberno());
                    datifooter.add(pdfsell.getPossellamount());
                    datifooter.add(pdfsell.getPossellnumber());
                    datifooter.add(pdfsell.getBanksellamount());
                    datifooter.add(pdfsell.getBanksellnumber());
                    pdfsell.setFooterdati(datifooter);

                    String nomereport = sanitizeFileName("SB LIST TRANSACTION DA " + data1 + " A " + data2 + "_"
                            + sanitizeFileName(datecreation) + ".xlsx");
                    File Output = new File(sanitizePath(path) + File.separator + nomereport);
                    String base64 = Excel.BB_receiptexcel(Output, pdfsell, d3, d4,
                            "SellBack Transaction List - Group By Sell-Buy");
                    if (base64 != null) {
                        boolean es = rilasciasftp(Output, meseriferimento + "/SB LIST TRANSACTION", annoriferimento,
                                gf);
                        if (es) {
                            gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                        } else {
                            gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                        }
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }

                } else {
                    gf.logger.log.log(Level.WARNING, "NESSUN DATO TROVATO. {0}", tipofile);
                }
                break;

            }
            case "BB1": {
                // BB TRANSACTION - DA INIZIO MESE A IERI

                TillTransactionListBB_value pdfsell = db.list_BBTransactionList_mod(br1, data1, data2, allenabledbr);
                if (pdfsell != null) {
                    ArrayList<String> datifooter = new ArrayList<>();
                    datifooter.add(pdfsell.getTransactionnumberresidentbuy());
                    datifooter.add(pdfsell.getTransactionnumbernonresidentbuy());
                    datifooter.add(pdfsell.getInternetbookingamountyes());
                    datifooter.add(pdfsell.getInternetbookingnumberyes());
                    datifooter.add(pdfsell.getPosbuyamount());
                    datifooter.add(pdfsell.getPosbuynumber());
                    datifooter.add(pdfsell.getBankbuyamount());
                    datifooter.add(pdfsell.getBankbuynumber());
                    datifooter.add(pdfsell.getTransactionnumberresidentsell());
                    datifooter.add(pdfsell.getTransactionnumbernonresidentsell());
                    datifooter.add(pdfsell.getInternetbookingamountno());
                    datifooter.add(pdfsell.getInternetbookingnumberno());
                    datifooter.add(pdfsell.getPossellamount());
                    datifooter.add(pdfsell.getPossellnumber());
                    datifooter.add(pdfsell.getBanksellamount());
                    datifooter.add(pdfsell.getBanksellnumber());
                    pdfsell.setFooterdati(datifooter);

                    String nomereport = sanitizeFileName("BB LIST TRANSACTION DA " + data1 + " A " + data2 + "_"
                            + sanitizeFileName(datecreation) + ".xlsx");
                    File Output = new File(sanitizePath(path) + File.separator + nomereport);
                    String base64 = Excel.BB_receiptexcel(Output, pdfsell, d3, d4,
                            "BuyBack Transaction List - Group By Buy-Sell ");
                    if (base64 != null) {
                        boolean es = rilasciasftp(Output, meseriferimento + "/BB LIST TRANSACTION", annoriferimento,
                                gf);
                        if (es) {
                            gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                        } else {
                            gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                        }
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.WARNING, "NESSUN DATO TROVATO. {0}", tipofile);
                }
                break;
            }
            case "AML1": {
                // AML MASTER - DA INIZIO MESE A IERI

                String nomereport = sanitizeFileName("MONEY LAUNDERING - MASTER DATA DA " + data1 + " A " + data2 + "_"
                        + sanitizeFileName(datecreation) + ".xls");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = Excel.AML_anagrafica(gf, Output, data1, data2);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MONEY LAUNDERING", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;
            }
            case "AML2": {
                // AML REGISTRATION - DA INIZIO MESE A IERI
                String nomereport = sanitizeFileName("MONEY LAUNDERING - REGISTRATION DA " + data1 + " A " + data2 + "_"
                        + sanitizeFileName(datecreation) + ".xls");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = Excel.AML_registrazione(gf, Output, data1, data2);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MONEY LAUNDERING", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;
            }
            case "COR": {
                // CORA MESE PRECEDENTE
                boolean es = false;
                String nomereport = sanitizeFileName("CORA MENSILE " + sanitizeFileName(meseanno_prec) + "_"
                        + sanitizeFileName(datecreation) + ".zip");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                try {
                    String base64 = db.getCORA(sanitizeFileName(meseanno_prec), "0");
                    FileUtils.writeByteArrayToFile(Output, Base64.decodeBase64(base64.getBytes()));
                    if (Output.exists() && Output.length() > 0) {
                        es = true;
                    }
                } catch (Exception ex) {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0} -- {1}",
                            new Object[] { Output.getPath(), ex.getMessage() });
                    es = false;
                }
                if (es) {
                    boolean es1 = rilasciasftp(Output, meseriferimento_prec + "/CORA", anno_rif, gf);
                    if (es1) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }

                break;
            }
            case "OAM": {
                // OAM MESE PRECEDENTE
                boolean es = false;
                String nomereport = sanitizeFileName("OAM ORDINARY " + sanitizeFileName(meseanno_prec) + "_"
                        + sanitizeFileName(datecreation) + ".zip");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                try {
                    String base64 = db.getOAM(sanitizeFileName(meseanno_prec), "0");
                    FileUtils.writeByteArrayToFile(Output, Base64.decodeBase64(base64.getBytes()));
                    if (Output.exists() && Output.length() > 0) {
                        es = true;
                    }
                } catch (Exception ex) {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0} -- {1}",
                            new Object[] { Output.getPath(), ex.getMessage() });
                    es = false;
                }
                if (es) {
                    boolean es1 = rilasciasftp(Output, meseriferimento_prec + "/OAM", anno_rif, gf);
                    if (es1) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;
            }
            case "CP1": {
                // CASHIER PERFRMANCE - DA INIZIO MESE A IERI

                String nomereport = sanitizeFileName("CASHIER PERFORMANCE DA " + sanitizeFileName(data1) + " A "
                        + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xls");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                ArrayList<String[]> fasce = db.list_fasce_cashier_perf("BS", null);
                ArrayList<C_CashierPerformance_value> dati = db.list_C_CashierPerformance_value(sanitizeFileName(data1),
                        sanitizeFileName(data2), "BS", br1, fasce);
                ArrayList<String> alcolonne = new ArrayList<>();
                alcolonne.add("User");
                alcolonne.add("0%");
                alcolonne.add("NEG");
                alcolonne.add("Full");
                alcolonne.add("#Trans.");
                alcolonne.add("NFF");
                alcolonne.add("DEL");
                alcolonne.add("Volume");
                alcolonne.add("Com+Fix");
                alcolonne.add("%Media");
                alcolonne.add("Val.Medio");
                alcolonne.add("Com.Media");
                alcolonne.add("#ERR");
                alcolonne.add("tot. ERR");
                String base64 = Excel.CP_mainexcel(Output, d3, d4, sanitizeFileName(data1), sanitizeFileName(data2),
                        "BS", dati, alcolonne, br1, allenabledbr);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/CASHIER PERFORMANCE", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }

                break;
            }
            case "DA1": {
                // MANAGEMENT CONTROL - DAILY REPORT - DA INIZIO MESE A IERI //COMPLETO
                String nomereport = sanitizeFileName("MANAGEMENT CONTROL - DAILY REPORT DA " + sanitizeFileName(data1)
                        + " A " + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                ArrayList<NC_category> listnccat = db.list_ALL_nc_category("000");
                String base64 = ControlloGestione.daily_report(Output, br1, mesemysql, meseriferimento, allenabledbr,
                        iniziomese, db, listnccat);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "DAR": {
                // MANAGEMENT CONTROL - DAILY REPORT - DA INIZIO MESE A IERI //ROMA
                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - DAILY REPORT - SOLO ROMA DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                ArrayList<NC_category> listnccat = db.list_ALL_nc_category("000");
                String base64 = ControlloGestione.daily_report(Output, filiali_soloROMA, mesemysql, meseriferimento,
                        allenabledbr, iniziomese, db, listnccat);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "DAUK": {
                // MANAGEMENT CONTROL - DAILY REPORT UK - DA INIZIO MESE A IERI //COMPLETO
                gf.setIs_IT(false);
                gf.setIs_UK(true);
                db.closeDB();
                db = new DatabaseCons(gf);
                ArrayList<NC_category> listnccat = db.list_ALL_nc_category("000");
                br1 = db.list_branchcode_completeAFTER311217();
                allenabledbr = db.list_branch();
                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - DAILY REPORT UK - DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.daily_report(Output, br1, mesemysql, meseriferimento, allenabledbr,
                        iniziomese, db, listnccat);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL UK", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }

                break;
            }
            case "DACZ": {
                // MANAGEMENT CONTROL - DAILY REPORT CZ - DA INIZIO MESE A IERI //COMPLETO
                gf.setIs_IT(false);
                gf.setIs_UK(false);
                gf.setIs_CZ(true);
                db.closeDB();
                db = new DatabaseCons(gf);
                ArrayList<NC_category> listnccat = db.list_ALL_nc_category("000");
                br1 = db.list_branchcode_completeAFTER311217();
                allenabledbr = db.list_branch();
                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - DAILY REPORT CZ - DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.daily_report(Output, br1, mesemysql, meseriferimento, allenabledbr,
                        iniziomese, db, listnccat);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL CZ", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }

                break;
            }
            case "MCO1": {
                // MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL - DA INIZIO MESE A IERI
                // //COMPLETO

                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL N1 DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, true, allenabledbr,
                        db);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "MCO1CZ": {
                // MANAGEMENT CONTROL CZ - REPORT MANAGEMENT CONTROL - DA INIZIO MESE A IERI
                // //COMPLETO
                gf.setIs_IT(false);
                gf.setIs_UK(false);
                gf.setIs_CZ(true);
                db.closeDB();
                db = new DatabaseCons(gf);
                br1 = db.list_branchcode_completeAFTER311217();
                allenabledbr = db.list_branch();

                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL CZ - REPORT MANAGEMENT CONTROL N1 DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, true, allenabledbr,
                        db);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL CZ", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "MCO2": {
                // MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL - DA INIZIO MESE A IERI // NO
                // DELETE
                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL N1 - NO DELETE OPERATIONS DA "
                                + sanitizeFileName(data1) + " A " + sanitizeFileName(data2) + "_"
                                + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, false, allenabledbr,
                        db);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "INS": {
                // MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL - DA INIZIO MESE A IERI // NO
                // DELETE
                String sanitizedData1 = sanitizeInput(data1);
                String sanitizedData2 = sanitizeInput(data2);
                String sanitizedDateCreation = sanitizeInput(datecreation);

                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - REPORT LIMIT INSURANCE BRANCH DA " + sanitizedData1 + " A "
                                + sanitizedData2 + "_" + sanitizedDateCreation + ".xlsx");

                File Output = new File(sanitizePath(path) + File.separator + nomereport);

                String base64 = ControlloGestione.limit_insurance(Output, br1, iniziomese, ieri, allenabledbr, db);

                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "CA1": {
                // MANAGEMENT CONTROL - REPORT CHANGE ACCOUNTING N1 - DA INIZIO MESE A IERI
                // //COMPLETO
                String nomereport = sanitizeFileName(
                        "MANAGEMENT CONTROL - REPORT CHANGE ACCOUNTING N1 DA " + sanitizeFileName(data1) + " A "
                                + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.management_change_accounting1(Output, br1, iniziomese, ieri,
                        allenabledbr, db);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/MANAGEMENT CONTROL", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            case "CAER": {
                //
                String nomereport = sanitizeFileName("CASHIER OPENCLOSE ERRORS DA " + sanitizeFileName(data1) + " A "
                        + sanitizeFileName(data2) + "_" + sanitizeFileName(datecreation) + ".xlsx");
                File Output = new File(sanitizePath(path) + File.separator + nomereport);
                String base64 = ControlloGestione.C_OpenCloseError(Output, br1, data1, data2, allenabledbr, db);
                if (base64 != null) {
                    boolean es = rilasciasftp(Output, meseriferimento + "/CASHIER PERFORMANCE", annoriferimento, gf);
                    if (es) {
                        gf.logger.log.log(Level.WARNING, "FILE RILASCIATO CON SUCCESSO: {0}", Output.getPath());
                    } else {
                        gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                    }
                } else {
                    gf.logger.log.log(Level.SEVERE, "ERRORE RILASCIO FILE: {0}", Output.getPath());
                }
                break;

            }
            default:
                break;
        }

        db.closeDB();
        gf.logger.log.warning("END");
    }

    // public static void main(String[] args) {
    // GeneraFile gf = new GeneraFile();
    // gf.rilasciafile(gf, "LOC");
    // }
    private boolean rilasciasftp(File file, String meseriferimento, String annoriferimento, GeneraFile gf) {
        return SftpMaccorp.UPLOAD_AWS(file, meseriferimento, annoriferimento, gf.logger);
    }

    // private boolean rilasciasftp(File file, String meseriferimento, String
    // annoriferimento) {
    // boolean ok = true;
    // String folder_dest = "/mnt/MasterVolume/macsftp/Files/" + annoriferimento +
    // "/" + meseriferimento + "/";
    // ChannelSftp sftpaws = SftpConnection.connect(se_user, se_pwd, se_ip, se_port,
    // this.logger);//inizio dell'upload dei file.
    // if (sftpaws.isConnected()) {
    // if (!isDirectory(sftpaws, folder_dest)) {
    // logger.log.log(Level.INFO, "CREO CARTELLA {0}", folder_dest);
    // try {
    // sftpaws.mkdir(folder_dest);
    // } catch (SftpException ex) {
    // ok = false;
    // logger.log.log(Level.SEVERE, "ERRORE CREAZIONE CARTELLA {0}: {1}", new
    // Object[]{folder_dest, ex.getMessage()});
    // }
    // }
    // try {
    // sftpaws.put(new FileInputStream(file), folder_dest + file.getName());
    // logger.log.log(Level.INFO, "{3}: FILE CARICATO: {0} - SIZE: {1}", new
    // Object[]{file.getName(), file.length(), "SFTP_MAC_FILES"});
    // } catch (SftpException | FileNotFoundException ex) {
    // ok = false;
    // logger.log.log(Level.SEVERE, "ERRORE UPLOAD FILE {0}: {1}", new
    // Object[]{file.getName(), ex.getMessage()});
    // }
    // SftpConnection.closeConnection(sftpaws, logger);
    // } else {
    // ok = false;
    // }
    // return ok;
    // }
    public boolean is_IT = false;
    public boolean is_CZ = false;
    public boolean is_UK = true;

    public LoggerNew logger = new LoggerNew("SFTP_MAC_FILES", "/mnt/mac/temp/");

    public boolean isIs_IT() {
        return is_IT;
    }

    public void setIs_IT(boolean is_IT) {
        this.is_IT = is_IT;
    }

    public boolean isIs_CZ() {
        return is_CZ;
    }

    public void setIs_CZ(boolean is_CZ) {
        this.is_CZ = is_CZ;
    }

    public boolean isIs_UK() {
        return is_UK;
    }

    public void setIs_UK(boolean is_UK) {
        this.is_UK = is_UK;
    }

    public String getThousand() {
        if (isIs_UK()) {
            return ",";
        } else {
            return ".";
        }
    }

    public String getDecimal() {
        if (isIs_UK()) {
            return ".";
        } else {
            return ",";
        }
    }

    public String getCODNAZ() {
        if (isIs_UK()) {
            return "031";
        } else if (isIs_CZ()) {
            return "275";
        } else {
            return "086";
        }
    }

    // ITA
    // public static final String thousand = ".";
    // public static final String decimal = ",";
    // public static final String codnaz = "086";
    // CZ
    // public static final String thousand = ".";
    // public static final String decimal = ",";
    // public static final String codnaz = "275";
    // UK
    // public static final String thousand = ",";
    // public static final String decimal = ".";
    // public static final String codnaz = "031";
}
