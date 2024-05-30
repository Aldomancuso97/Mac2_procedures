/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.main;

import static rc.soop.maintenance.Clear.CENTRAL_delete_chtransactiondoc_story;
import static rc.soop.maintenance.Clear.CENTRAL_deleteaggiornamentimenouno;
import static rc.soop.cora.MacCORA.generaannuale;
import static rc.soop.cora.MacCORA.generamensile;
import static rc.soop.esolver.Atlante.rilasciaAtlante;
import static rc.soop.esolver.ESolver.rilascia;
import static rc.soop.gs.Client.invia2022;
import rc.soop.gs.DatiInvio;
import rc.soop.gs.Db_Master;
import rc.soop.gs.Filiale;
import rc.soop.rilasciofile.GeneraFile;
import rc.soop.sftp.SftpSIA;
import rc.soop.start.Central_Branch;
import rc.soop.start.EngineExcel;
import static rc.soop.start.Utility.createLog;
import static rc.soop.start.Utility.pattern4;
import static rc.soop.start.Utility.rb;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import static rc.soop.aggiornamenti.VerificaAggiornamenti.ultimo_Aggiornamento22;
import static rc.soop.aggiornamenti.VerificaAggiornamenti.verificaSpreadKO;
import static rc.soop.aggiornamenti.VerificaAggiornamenti.verifica_Aggiornamenti22;
import static rc.soop.aggiornamenti.VerificaAggiornamenti.verificafileOAMCORA;
import static rc.soop.crm.Engine.recap_greenNumber;
import static rc.soop.crm.Engine.refresh_branch;
import static rc.soop.crm.Engine.set_expired_noshow_NEW;
import static rc.soop.crm.Engine.updateSpreadSito;
import rc.soop.indicerischio.RiskIndex;
import rc.soop.maintenance.Branch;
import rc.soop.maintenance.Monitor;
import static rc.soop.maintenance.ProceduraDaily.allinea;
import rc.soop.maintenance.RateStockPrice;
import rc.soop.maintenance.Rate_BCE;
import rc.soop.movepdf.MovePdf;
import rc.soop.newsletters.Newsletters;
import rc.soop.oam.MacOAM;
import rc.soop.qlik.CopyAtlante;
import rc.soop.qlik.GeneraVatRef;
import rc.soop.riallinea.CorreggiSpread;
import rc.soop.riallinea.ReloadingDati;
import rc.soop.rilasciofile.GeneraStockPriceHistorical;
import rc.soop.sftp.SftpATM;
import rc.soop.sftp.SftpMaccorp;
import rc.soop.spreadexcel.SpreadExcel;
import rc.soop.start.ReadTracking;
import rc.soop.start.UnlockOperation;

/**
 *
 * @author raf
 */
public class MainSelector {

    public static void main(String[] args) {

        int scelta;
        String repvalue;

        String filiale;
        String metodo;

        try {
            scelta = Integer.parseInt(args[0]);
        } catch (Exception e) {
            scelta = 1;
        }

        try {
            repvalue = sanitizeInput(args[1]);
        } catch (Exception e) {
            repvalue = "MCO1CZ";
        }

        try {
            filiale = sanitizeInput(args[1]);
            metodo = sanitizeInput(args[2]);
        } catch (Exception e) {
            filiale = "000";
            metodo = "NO";
        }

        String datastart;
        String dataend;
        try {
            datastart = sanitizeInput(args[2]);
            dataend = sanitizeInput(args[3]);
        } catch (Exception e) {
            datastart = "2022-12-01";
            dataend = "2022-12-31";
        }

        switch (scelta) {
            case 1: //  ESOLVER
                rilascia(null);
                break;
            case 2: //  ATLANTE
                rilasciaAtlante(true);
                break;
            case 3: //  SFTP SIA/NEXI - FILE TRIMESTRALE ESOLVER
                new SftpSIA().rilasciaFIle(new File(sanitizePath(args[1])), true);
                break;
            case 4: //  GRANDI STAZIONI
                Db_Master db1 = new Db_Master();
                List<Filiale> input = db1.getConfList();
                List<DatiInvio> dati = db1.query_datiinvio(input, new DateTime());
                db1.closeDB();
                invia2022(dati);
                break;
            case 5: //  RILASCIO FILE
                GeneraFile gf = new GeneraFile();
                gf.rilasciafile(gf, repvalue);
                break;
            case 6: //  CORA MENSILE
                generamensile();
                break;
            case 7: { //  CORA ANNUALE
                generaannuale();
                break;
            }
            case 8: { //ALLINEAMENTO DATI CENTRALE
                ReloadingDati.riallinea();
                break;
            }
            case 9: {// SITO ENGINE
                recap_greenNumber();
                refresh_branch();
                updateSpreadSito();
                set_expired_noshow_NEW();
                break;
            }
            case 10: {
                //AGGIORNA RATE BCE 
                Rate_BCE.engine();
                break;
            }
            case 11: // AGGIORNAMENTI CENTRALE
                Logger log = createLog("Mac2.0_AGG_CENTRAL_" + "000", rb.getString("path.log"), pattern4);
                log.warning("START...");
                Central_Branch cb = new Central_Branch("000");
                try {
                    if (!cb.updateCentral(log)) {
                        log.warning("AGGIORNAMENTO1 CENTRALE COMPLETATO");
                    } else {
                        log.severe("ERRORE AGGIORNAMENTO1 CENTRALE");
                    }
                } catch (Exception e) {
                }
                try {
                    if (!cb.updateCentral2(log)) {
                        log.warning("AGGIORNAMENTO2 CENTRALE COMPLETATO");
                    } else {
                        log.severe("ERRORE AGGIORNAMENTO2 CENTRALE");
                    }
                } catch (Exception e) {
                }
                try {
                    if (!cb.updateCentral3(log)) {
                        log.warning("AGGIORNAMENTO3 CENTRALE COMPLETATO");
                    } else {
                        log.severe("ERRORE AGGIORNAMENTO3 CENTRALE");
                    }
                } catch (Exception e) {
                }

                break;
            case 12: //MONITOR
                Monitor.exe();
                break;
            case 13: //VERIFICA AGGIORNAMENTI
                ultimo_Aggiornamento22();
                verifica_Aggiornamenti22();
                break;
            case 14: //ATLANTE SOLO MAIL
                rilasciaAtlante(false);
                break;
            case 15: // PROCEDURA CARICA EXCEL CAMBIO
                EngineExcel.insert_value_excel();
                break;
            case 16: // PROCEDURA CARICA EXCEL SPREAD
                SpreadExcel.engine();
                break;
            case 17: // STOCK PRICE STORICO ANNUO
                GeneraStockPriceHistorical.engine();
                break;
            case 18: // INDICE RICHIO
                RiskIndex.engine();
                break;
            case 19: // RATE STOCK PRICE IT
                RateStockPrice.engine("IT");
                break;
            case 20: // RATE STOCK PRICE CZ
                RateStockPrice.engine("CZ");
                break;
            case 21: // NEWSLETTERS
                Newsletters.engine();
                break;
            case 22: // OAM
                MacOAM.engine();
                break;
            case 23: // DELETE MAINTENANCE
                CENTRAL_deleteaggiornamentimenouno(new DateTime().minusDays(1).toString("YYMMdd"));
                CENTRAL_delete_chtransactiondoc_story();
                break;
            case 24: // QLIK COPY EXCEL
                CopyAtlante.engine();
                break;
            case 25: // QLIK GENERA VAT REFUND
                GeneraVatRef.engine();
                break;
            case 26: // RILASCIO FILE ATM
                SftpATM.engine();
                break;
            case 27: {// RILASCIO FILE MERCURY-BASSILICHI/NEXI
                    //try {
                    //new SftpMaccorp().sftpbassilichinexi(true);
                    //} catch (Exception e) {
                    //}
                try {
                    new SftpMaccorp().sftpmercury();
                } catch (Exception e) {
                }
                break;
            }
            case 28: //UNLOCK OPERAZIONI
                new UnlockOperation().engine();
                break;
            case 29: //READ TRACKING
                ReadTracking.engine();
                break;
            case 30: // CORREGGI SPREAD CZ
                CorreggiSpread.enginecz();
                break;
            case 31: {// ALLINEA FILIALE CZ
                rc.soop.maintenance.Db_Master dbm = new rc.soop.maintenance.Db_Master(false, true);
                ArrayList<Branch> li = dbm.list_branch_enabled();
                for (int y = 0; y < li.size(); y++) {
                    try {
                        allinea(dbm, li.get(y), li);
                    } catch (Exception e) {
                    }
                }
                dbm.closeDB();
                break;
            }
            case 32: //MOVE PDF CHANGE
                MovePdf.remove_CH_Transaction_DOC();
                break;
            case 33: //SFTP SIA/NEXI
                new SftpSIA().sftpsia(true);
                break;
            case 34: //VERIFICA SPREAD KO
                verificaSpreadKO();
                try {
                    verificafileOAMCORA();
                } catch (Exception e) {
                }
                break;
            case 44: { //AGGIORNA FILIALI    
                Logger log1 = createLog("Mac2.0_AGG_" + metodo + "_" + filiale, rb.getString("path.log"), pattern4);
                log1.warning("START...");
                Central_Branch cb1 = new Central_Branch(filiale);
                switch (metodo) {
                    case "TOBRANCH": {

                        try {
                            if (!cb1.updateToBranch(log1)) {
                                log1.log(Level.WARNING, "AGGIORNAMENTO1 VERSO LA  FILIALE: {0} COMPLETATO", filiale);
                            } else {
                                log1.log(Level.SEVERE, "ERRORE AGGIORNAMENTO1 VERSO LA FILIALE: {0}", filiale);
                            }
                        } catch (Exception e) {
                        }
                        try {
                            if (!cb1.updateToBranch2(log1)) {
                                log1.log(Level.WARNING, "AGGIORNAMENTO2 VERSO LA  FILIALE: {0} COMPLETATO", filiale);
                            } else {
                                log1.log(Level.SEVERE, "ERRORE AGGIORNAMENTO2 VERSO LA FILIALE: {0}", filiale);
                            }
                        } catch (Exception e) {
                        }
                        try {
                            if (!cb1.updateToBranch3(log1)) {
                                log1.log(Level.WARNING, "AGGIORNAMENTO3 VERSO LA  FILIALE: {0} COMPLETATO", filiale);
                            } else {
                                log1.log(Level.SEVERE, "ERRORE AGGIORNAMENTO3 VERSO LA FILIALE: {0}", filiale);
                            }
                        } catch (Exception e) {
                        }

                    }
                    break;
                    case "TOCENTRAL":
                        if (!cb1.updateFromBranch(log1)) {
                            log1.log(Level.WARNING, "AGGIORNAMENTO DALLA  FILIALE: {0} COMPLETATO", filiale);
                        } else {
                            log1.log(Level.SEVERE, "ERRORE AGGIORNAMENTO DALLA FILIALE: {0}", filiale);
                        }

                        break;
                }
                break;
            }
            case 55: { //GENERA FILE ONDEMAND              
                System.out.println("rc.soop.main.MainSelector.main(1) " + datastart);
                System.out.println("rc.soop.main.MainSelector.main(2) " + dataend);
                DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
                DateTime iniziomese = dtf.parseDateTime(datastart);
                DateTime ieri = dtf.parseDateTime(dataend);
                GeneraFile gf2 = new GeneraFile();
                gf2.rilasciafile(gf2, repvalue, iniziomese, ieri);
                break;

            }
            default:
                break;
        }

    }

    
private static String sanitizeInput(String input) {
    // Simple sanitization, you may want to use a more robust method
    return input.replaceAll("[^a-zA-Z0-9_-]", "");
}

private static String sanitizePath(String path) {
    // Assuming a base directory to restrict the file access
    String baseDir = "/allowed/base/directory/";
    File file = new File(baseDir, path);
    try {
        String canonicalPath = file.getCanonicalPath();
        if (!canonicalPath.startsWith(new File(baseDir).getCanonicalPath())) {
            throw new IllegalArgumentException("Invalid file path: " + path);
        }
        return canonicalPath;
    } catch (Exception e) {
        throw new IllegalArgumentException("Invalid file path: " + path, e);
    }
}
}
