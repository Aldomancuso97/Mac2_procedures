package rc.soop.esolver;

import com.google.common.base.Splitter;
import static rc.soop.esolver.Util.calcolaIva;
import static rc.soop.esolver.Util.checkTXT;
import static rc.soop.esolver.Util.estraiEccezione;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.formatAL;
import static rc.soop.esolver.Util.formatBankBranchReport;
import static rc.soop.esolver.Util.formatDoubleforMysql;
import static rc.soop.esolver.Util.getCity_apm;
import static rc.soop.esolver.Util.getNC_category;
import static rc.soop.esolver.Util.getNC_causal;
import static rc.soop.esolver.Util.get_ET_change_value;
import static rc.soop.esolver.Util.get_customerKind;
import static rc.soop.esolver.Util.get_user;
import static rc.soop.esolver.Util.get_vat;
import static rc.soop.esolver.Util.list_oc_errors;
import static rc.soop.esolver.Util.log;
import static rc.soop.esolver.Util.parseDoubleR;
import static rc.soop.esolver.Util.parseIntR;
import static rc.soop.esolver.Util.patternnormdate_filter;
import static rc.soop.esolver.Util.patternsql;
import static rc.soop.esolver.Util.patternyear;
import static rc.soop.esolver.Util.query_transaction_ch;
import static rc.soop.esolver.Util.query_transaction_value;
import static rc.soop.esolver.Util.rb;
import static rc.soop.esolver.Util.removeDuplicatesAL;
import static rc.soop.esolver.Util.removeDuplicatesALAr;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 *
 * @author rcosco
 */
public class ESolver {

    // 11/12/2020
    private static final boolean newSpread = true;

    private String codiceNegozi = "190310";
    private final String fat_tiporiga = "30";

    private final String incassofattura = "110302";

    public static final String pathin = rb.getString("path.in");
    public static String pathout = rb.getString("path.out");

    public static final String separator = ";";
    public static final String tag_GEN = "GEN";
    public static final String tag_RIG = "RIG";
    public static final String tag_TES = "TES";

    private static final String scontrino_NORM = "701";
    private static final String scontrino_AG_VIA = "702";

    private String listaAG_viaggi = "";

    public ESolver() {
        Db_Master dbm = new Db_Master();
        this.codiceNegozi = dbm.contabilita("CANE");
        this.listaAG_viaggi = dbm.getPath("ag_viaggio");
        dbm.closeDB();
    }

    public static NC_vatcode get_NC_vatcode(String cod) {
        Db_Master db8 = new Db_Master();
        NC_vatcode sr = db8.get_NC_vatcode(cod);
        db8.closeDB();
        return sr;
    }

    private void anticipoDipendenti(String type, String anno, String data, String n_reg, String conto_dipendente,
            String importo, String kind_causal_descrizione_filiale, String codice_filiale, PrintWriter writer,
            String codicemastro) {
        // uno per ogni dipendente
        if (type.equals("AND")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + codicemastro + separator + separator + conto_dipendente + separator + importo
                    + separator + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + importo + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
        } else if (type.equals("ADR")) {
            // rettifica
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + importo + separator
                    + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + codicemastro + separator + conto_dipendente + separator + separator + importo
                    + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
        }
    }

    private void cashAdvance(String type, String anno, String data, String n_reg, String conto_esolver1, String net,
            String total, String tot_comm, String descrizione_filiale,
            String codice_filiale, String conto_esolver2, String conto_esolver3, String codiceiva, PrintWriter writer,
            String codscontrino) {
        // complessivo giornaliero, conto diverso in base al pos utilizzato
        if (type.equals("CAD")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver1 + separator + separator + separator + net + separator + separator
                    + codice_filiale + separator + "Cash Advance su carte di credito " + descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + net + separator + codice_filiale + separator + "Cash Advance su carte di credito "
                    + descrizione_filiale);
            // writer.println(tag_GEN + separator + type + separator + anno + separator +
            // data + separator + n_reg + separator + conto_esolver2 + separator + separator
            // + separator + separator + net + separator + separator + "Cash Advance su
            // carte di credito " + descrizione_filiale);
        } else if (type.equals("CAC")) {
            writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver1 + separator + tot_comm + separator + codiceiva + separator + tot_comm
                    + separator + separator + codice_filiale + separator + "Corrispettivi cash advance "
                    + descrizione_filiale + separator + separator + separator + separator + separator + separator
                    + separator);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator + separator + separator + separator + separator + separator + separator
                    + conto_esolver2 + separator + separator + total + separator + separator
                    + "Corrispettivi cash advance " + descrizione_filiale + separator + separator + codice_filiale);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator + separator + separator + separator + separator + separator + separator
                    + conto_esolver3 + separator + codice_filiale + separator + separator + net + separator
                    + "Corrispettivi cash advance " + descrizione_filiale + separator + separator + codice_filiale);
        }
    }

    private void erroriCassa(String type, String anno, String data, String n_reg, String conto_esolver, String importo,
            String descrizione_filiale,
            String codice_filiale, PrintWriter writer) {
        // singolo per ogni errore?
        if (type.equals("ERA")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + importo
                    + separator + separator + codice_filiale + separator + "Differenza cassa positiva - "
                    + descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + separator + separator + separator + importo + separator
                    + codice_filiale + separator + "Differenza cassa positiva - " + descrizione_filiale);
        } else if (type.equals("ERR")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + separator + separator + importo + separator + separator
                    + codice_filiale + separator + "Differenza cassa negativa - " + descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + importo + separator + codice_filiale + separator + "Differenza cassa negativa - "
                    + descrizione_filiale);
        }
    }

    private void extBranch(String type, String anno, String data, String n_reg, String codice_filiale1,
            String codice_filiale2, String importo,
            String descrizione_filiale1, String descrizione_filiale2, PrintWriter writer) {
        // singolo per ogni filiale
        if (type.equals("TBR")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale1 + separator + separator + importo
                    + separator + separator + codice_filiale1 + separator + descrizione_filiale1 + " - To branch - "
                    + descrizione_filiale2);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale2 + separator + separator + separator
                    + importo + separator + codice_filiale2 + separator + descrizione_filiale1 + " - To branch - "
                    + descrizione_filiale2);
        }
    }

    private void vatRefound(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String codice_esolver,
            String kind_causal_descrizione_filiale, PrintWriter writer) {
        // una per ogni compagnia
        // VAT
        writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg + separator
                + codice_esolver + separator + separator + separator + importo + separator + separator + codice_filiale
                + separator + kind_causal_descrizione_filiale);
        writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg + separator
                + this.codiceNegozi + separator + codice_filiale + separator + separator + separator + importo
                + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
    }

    private void westernUnion(String type, String anno, String data, String n_reg, String codice_filiale,
            String importo, String codice_esolver,
            String kind_causal_descrizione_filiale, String cod_banca_lungo, String cod_banca_corto,
            PrintWriter writer) {
        if (type.equals("WUR")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + codice_esolver + separator + separator + separator + importo + separator + separator
                    + codice_filiale + separator + kind_causal_descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + importo + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
        } else if (type.equals("WUS")) {
            if (cod_banca_lungo != null) {
                // POS
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + importo
                        + separator + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver + separator + separator + separator + separator + importo
                        + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
            } else {
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + importo
                        + separator + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver + separator + separator + separator + separator + importo
                        + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
            }
        }
    }

    private void extBank(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String descrizione_filiale, String cod_banca_lungo, String cod_banca_corto,
            String conto_esolver, PrintWriter writer, boolean locale,
            String codice_spread, String net, String spread) {
        // giornaliera per ogni filiale, una per valuta estera (controvalore), una per
        // valuta locale

        String valueSpread = "";
        String valueSpreadNeg = "";
        boolean insertspread = (spread != null) && (newSpread) && (fd(spread) != 0.00);
        if (spread != null) {
            if (spread.contains("-")) {
                valueSpreadNeg = StringUtils.replace(spread, "-", "");
            } else {
                valueSpread = spread;
            }
        }

        switch (type) {
            case "FBL":
            case "FBE":
                if (locale) {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + conto_esolver + separator + codice_filiale + separator + separator + importo
                            + separator + separator + codice_filiale + separator + "From bank - "
                            + descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + separator + importo + separator + codice_filiale + separator + "From bank - "
                            + descrizione_filiale);
                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + conto_esolver + separator + separator + separator + importo + separator
                            + separator + codice_filiale + separator + "From bank - " + descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + separator + importo + separator + codice_filiale + separator + "From bank - "
                            + descrizione_filiale);
                }
                break;
            case "TBL":
            case "TBE":
                if (locale) {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + importo + separator + separator + codice_filiale + separator + "To bank - "
                            + descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + conto_esolver + separator + codice_filiale + separator + separator + separator
                            + importo + separator + codice_filiale + separator + "To bank - " + descrizione_filiale);
                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + importo + separator + separator + codice_filiale + separator + "To bank - "
                            + descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + conto_esolver + separator + separator + separator + separator + importo
                            + separator + codice_filiale + separator + "To bank - " + descrizione_filiale);
                }
                break;
            case "N_TBE":
                writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg
                        + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + importo
                        + separator + separator + codice_filiale + separator + "To bank - " + descrizione_filiale);
                if (insertspread) {
                    writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg
                            + separator + codice_spread + separator + separator + separator + valueSpreadNeg + separator
                            + valueSpread + separator + codice_filiale + separator + "Spread To bank - "
                            + descrizione_filiale);
                }
                writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg
                        + separator + conto_esolver + separator + separator + separator + separator + net + separator
                        + codice_filiale + separator + "To bank - " + descrizione_filiale);
                break;
            case "N_FBE":
                writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg
                        + separator + conto_esolver + separator + separator + separator + net + separator + separator
                        + codice_filiale + separator + "From bank - " + descrizione_filiale);
                if (insertspread) {
                    writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg
                            + separator + codice_spread + separator + separator + separator + valueSpreadNeg + separator
                            + valueSpread + separator + codice_filiale + separator + "Spread From bank - "
                            + descrizione_filiale);
                }
                writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg
                        + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + separator
                        + importo + separator + codice_filiale + separator + "From bank - " + descrizione_filiale);
                break;
            default:
                break;
        }

    }

    private void ticket_new(String type, String anno, String data, String n_reg,
            String codice_filiale,
            String fee, String iva, String imponibile, String imposta,
            String totale, String netto,
            String codice_esolver,
            String conto_ticket,
            String kind_causal_descrizione_filiale,
            String conto_Incasso,
            String codice_incasso,
            PrintWriter writer,
            String codscontrino) {
        if (type.equals("TCC")) {
            writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + codice_esolver + separator + fee + separator + iva + separator + imponibile
                    + separator + imposta + separator + codice_filiale + separator + "Corrispettivi "
                    + kind_causal_descrizione_filiale + separator + separator + separator + separator + separator
                    + separator + separator);

            String[] riga1 = { totale, "" };
            String[] riga2 = { "", netto };

            if (totale.contains("-")) {
                riga1[0] = "";
                riga1[1] = StringUtils.replace(totale, "-", "");
            }
            if (netto.contains("-")) {
                riga2[0] = StringUtils.replace(netto, "-", "");
                riga2[1] = "";
            }

            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator
                    + separator + separator + separator + separator + separator + separator + conto_Incasso + separator
                    + codice_incasso + separator
                    + riga1[0] + separator + riga1[1] + separator + kind_causal_descrizione_filiale
                    + " Incasso Corrispettivi" + separator + separator + codice_filiale);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator
                    + separator + separator + separator + separator + separator + separator + conto_ticket + separator
                    + separator
                    + riga2[0] + separator + riga2[1] + separator + "Biglietteria " + kind_causal_descrizione_filiale
                    + separator + separator + codice_filiale);
        }
    }

    private void ticket_sc_corrispettivo(String type, String anno, String data, String n_reg, String conto_esover,
            String total, String imponibile, String imposta, String contocassa, String codicecassa,
            String codice_filiale,
            String de_filiale, String decausale, String codiceiva, PrintWriter writer) {
        if (type.equals("CTI")) {
            String codscontrino = scontrino_NORM;
            if (this.listaAG_viaggi.contains(codiceiva)) {
                codscontrino = scontrino_AG_VIA;
            }
            writer.println(tag_TES + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator + separator + separator + separator + separator
                    + separator + separator + separator + separator + separator + separator + separator
                    + "Corrispettivi " + de_filiale + separator);
            writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esover + separator + total + separator
                    + codiceiva
                    + separator + imponibile + separator + imposta + separator + codice_filiale + separator
                    + "Corrispettivi " + de_filiale + " " + decausale
                    + separator + separator + separator + separator + separator + separator + separator);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg
                    + separator + separator + separator + separator + separator + separator
                    + separator + separator + contocassa + separator
                    + codicecassa + separator + total + separator + separator + "Incasso corrispettivi " + decausale
                    + separator + separator + codice_filiale);
        }
    }

    private void ticket(String type, String anno, String data, String n_reg, String codice_filiale, String valore,
            String codice_esolver, String kind_causal_descrizione_filiale, String cod_banca_lungo,
            String cod_banca_corto,
            PrintWriter writer) {
        // registrazione per ogni vendita
        if (type.equals("TSC")) {
            String importo = StringUtils.replace(StringUtils.replace(valore, ".", ","), "-", "").trim();
            if (fd(valore) > 0) {
                if (cod_banca_lungo == null) {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                }
            } else {
                if (cod_banca_lungo == null) {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + importo + separator
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + separator
                            + separator + importo + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);

                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + importo + separator
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + separator + importo + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                }
            }
        }
    }

    private void noChange(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String codice_esolver, String kind_causal_descrizione_filiale, String cod_banca_lungo,
            String cod_banca_corto,
            PrintWriter writer, String verificaaltroconto) {
        if (type.equals("NCV")) {

            if (verificaaltroconto.contains(";")) {
                String contoesolver2 = verificaaltroconto.trim().split(";")[0];
                String percentuale = verificaaltroconto.trim().split(";")[1];

                double p1 = fd(percentuale);
                double max = fd(formatDoubleforMysql(importo));

                double conto2 = fd(roundDoubleandFormat(max * p1 / 100, 2));
                double conto1 = max - conto2;

                String importo1 = StringUtils
                        .replace(StringUtils.replace(roundDoubleandFormat(conto1, 2), ".", ","), "-", "").trim();
                String importo2 = StringUtils
                        .replace(StringUtils.replace(roundDoubleandFormat(conto2, 2), ".", ","), "-", "").trim();
                if (cod_banca_lungo != null) {
                    // POS
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo1
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + contoesolver2 + separator + separator + separator + separator + importo2
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo1
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + contoesolver2 + separator + separator + separator + separator + importo2
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                }
            } else {
                if (cod_banca_lungo != null) {
                    // POS
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                } else {
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + separator
                            + importo + separator + separator + codice_filiale + separator
                            + kind_causal_descrizione_filiale);
                    writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                            + separator + codice_esolver + separator + separator + separator + separator + importo
                            + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
                }
            }
        }
        if (type.equals("NCR")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + codice_esolver + separator + separator + separator + importo + separator + separator
                    + codice_filiale + separator + kind_causal_descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + importo + separator + codice_filiale + separator + kind_causal_descrizione_filiale);
        }
    }

    private void buy(String type, String anno, String data, String n_reg, String codice_filiale,
            String importo,
            String codice_esolver1, String importo_comm, String codice_esolver2, String totale, String codiceiva,
            PrintWriter writer, String codscontrino,
            String codice_spread, String net, String spread) {

        String valueSpread = "";
        String valueSpreadNeg = "";

        boolean insertspread = (spread != null) && (newSpread) && (fd(spread) != 0.00);
        if (spread != null) {
            if (spread.contains("-")) {
                valueSpreadNeg = StringUtils.replace(spread, "-", "");
            } else {
                valueSpread = spread;
            }
        }

        switch (type) {
            case "BUY":
                writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                        + separator + importo_comm + separator + separator + codice_filiale + separator + separator
                        + separator + separator + separator + separator + separator + separator);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + separator + separator + separator + separator + separator + separator
                        + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + importo
                        + separator + "Pagamento acquisto valuta" + separator + separator + codice_filiale);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + separator + separator + separator + separator + separator + separator
                        + separator + codice_esolver2 + separator + separator + totale + separator + separator
                        + "Costo acquisto valuta" + separator + separator + codice_filiale);
                break;
            case "BSC":
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver2 + separator + separator + separator + totale + separator
                        + separator + codice_filiale + separator + "Costo acquisto valuta");
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                        + totale + separator + separator + "Pagamento acquisto valuta");
                break;
            case "N_BUY":
                writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                        + separator + importo_comm + separator + separator + codice_filiale + separator + separator
                        + separator + separator + separator + separator + separator + separator);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + separator + separator + separator + separator + separator + separator
                        + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + importo
                        + separator + "Pagamento acquisto valuta" + separator + separator + codice_filiale);
                if (insertspread) {
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + codice_spread + separator + separator + valueSpreadNeg + separator
                            + valueSpread + separator + "Spread acquisto valuta" + separator + separator
                            + codice_filiale);
                }
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                        + n_reg + separator + separator + separator + separator + separator + separator + separator
                        + separator + codice_esolver2 + separator + separator + net + separator + separator
                        + "Costo acquisto valuta" + separator + separator + codice_filiale);
                break;
            case "N_BSC":
                writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver2 + separator + separator + separator + net + separator + separator
                        + codice_filiale + separator + "Costo acquisto valuta");
                if (insertspread) {
                    writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg
                            + separator + codice_spread + separator + codice_filiale + separator + separator
                            + valueSpreadNeg + separator + valueSpread + separator + separator
                            + "Spread acquisto valuta");
                }
                writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg
                        + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                        + importo + separator + separator + "Pagamento acquisto valuta");
                break;
            default:
                break;
        }
    }

    private void sell(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String importo_comm,
            String descrizione_filiale, String codice_esolver1, String codice_esolver2, String cod_banca_lungo,
            String cod_banca_corto, String codiceiva,
            PrintWriter writer, String newimp, String codscontrino, String codice_spread, String net, String spread) {
        // registrazione giornaliera
        String valueSpread = "";
        String valueSpreadNeg = "";
        boolean insertspread = (spread != null) && (newSpread) && (fd(spread) != 0.00);
        if (spread != null) {
            if (spread.contains("-")) {
                valueSpreadNeg = StringUtils.replace(spread, "-", "");
            } else {
                valueSpread = spread;
            }
        }
        switch (type) {
            case "SEL":
                if (cod_banca_lungo != null) {
                    writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                            + separator + importo_comm + separator + separator + codice_filiale + separator
                            + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator
                            + separator + separator + separator);
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + codice_esolver2 + separator + separator + separator + importo + separator
                            + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + newimp + separator
                            + separator + "Incasso vendita valuta " + descrizione_filiale + separator + separator
                            + codice_filiale);
                } else {
                    writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                            + separator + importo_comm + separator + separator + codice_filiale + separator
                            + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator
                            + separator + separator + separator);
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + codice_esolver2 + separator + separator + separator + importo + separator
                            + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + newimp
                            + separator + separator + "Incasso vendita valuta " + descrizione_filiale + separator
                            + separator + codice_filiale);
                }
                break;
            case "N_SEL":
                if (cod_banca_lungo != null) {
                    writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                            + separator + importo_comm + separator + separator + codice_filiale + separator
                            + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator
                            + separator + separator + separator);
                    if (insertspread) {
                        writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data
                                + separator + n_reg + separator + separator + separator + separator + separator
                                + separator + separator + separator + codice_spread + separator + separator
                                + valueSpreadNeg + separator + valueSpread + separator + "Spread Vendita valuta "
                                + descrizione_filiale + separator + separator + codice_filiale);
                    }
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + codice_esolver2 + separator + separator + separator + net + separator
                            + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + cod_banca_lungo + separator + cod_banca_corto + separator + newimp + separator
                            + separator + "Incasso vendita valuta " + descrizione_filiale + separator + separator
                            + codice_filiale);
                } else {
                    writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva
                            + separator + importo_comm + separator + separator + codice_filiale + separator
                            + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator
                            + separator + separator + separator);
                    if (insertspread) {
                        writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data
                                + separator + n_reg + separator + separator + separator + separator + separator
                                + separator + separator + separator + codice_spread + separator + separator
                                + valueSpreadNeg + separator + valueSpread + separator + "Spread Vendita valuta "
                                + descrizione_filiale + separator + separator + codice_filiale);
                    }
                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + codice_esolver2 + separator + separator + separator + net + separator
                            + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);

                    writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator
                            + n_reg + separator + separator + separator + separator + separator + separator + separator
                            + separator + this.codiceNegozi + separator + codice_filiale + separator + newimp
                            + separator + separator + "Incasso vendita valuta " + descrizione_filiale + separator
                            + separator + codice_filiale);
                }
                break;
            case "SSC":
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + newimp
                        + separator + separator + codice_filiale + separator + "Incasso vendita valuta "
                        + descrizione_filiale);
                writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver2 + separator + separator + separator + separator + newimp
                        + separator + codice_filiale + separator + "Vendita valuta " + descrizione_filiale);
                break;
            case "N_SSC":
                writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg
                        + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + newimp
                        + separator + separator + codice_filiale + separator + "Incasso vendita valuta "
                        + descrizione_filiale);
                if (insertspread) {
                    writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg
                            + separator + codice_spread + separator + separator + separator + valueSpreadNeg + separator
                            + valueSpread + separator + codice_filiale + separator + "Spread su Vendita valuta "
                            + descrizione_filiale);
                }
                writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg
                        + separator + codice_esolver2 + separator + separator + separator + separator + net + separator
                        + codice_filiale + separator + "Vendita valuta " + descrizione_filiale);
                break;
            default:
                break;
        }
    }

    private void rimborso(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String descrizione_filiale,
            String codice_esolver, PrintWriter writer) {
        if (type.equals("RIB")) {
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + codice_esolver + separator + separator + separator + importo + separator + separator
                    + codice_filiale + separator + "Rimborso valuta - " + descrizione_filiale);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + this.codiceNegozi + separator + codice_filiale + separator + separator + separator
                    + importo + separator + codice_filiale + separator + "Rimborso valuta - " + descrizione_filiale);
        }
    }

    private String estraiNumeroFattura(String numfat) {
        Iterable<String> parameters = Splitter.on(" ").split(numfat);
        Iterator<String> it = parameters.iterator();
        String n = it.next().trim();
        int r = parseIntR(n);
        return String.valueOf(r);
    }

    private void fattura_new(String type, String data, String codicecliente,
            String conto_esolver, String importoincassato, String commissione, String codice_filiale, String numfat,
            String cognome,
            String nome, String codiceiva, PrintWriter writer) {
        if (type.equals("FAT") || type.equals("NDC")) {
            String num = estraiNumeroFattura(numfat);
            writer.println(tag_TES + separator + type + separator + data + separator + num + separator + codicecliente
                    + separator + separator + separator + separator + separator + separator + importoincassato
                    + separator + codice_filiale + separator + StringUtils.capitalize(type.toLowerCase()) + " "
                    + cognome + " " + nome + separator);
            writer.println(tag_RIG + separator + type + separator + data + separator + num + separator + separator
                    + this.fat_tiporiga + separator
                    + conto_esolver + separator + commissione + separator + codiceiva + separator + codice_filiale
                    + separator + separator + separator + separator
                    + StringUtils.capitalize(type.toLowerCase()) + " N. " + numfat + " " + cognome + " " + nome);
        }
    }

    private void crea_anagrafica_clienti(String cognome, String nome, String indirizzo, String citta, String naz,
            String numdoc,
            String codcl, PrintWriter writer) {
        writer.println(tag_GEN + separator + cognome + separator + nome + separator + indirizzo + separator + citta
                + separator + naz + separator + numdoc + separator + codcl);
    }

    private void tofrombank_fattura(String type, String anno, String data, String n_reg, String conto_esolver,
            String contocassa, String codicecassa, String importo, String codice_filiale, String numfat,
            String codice_cliente, String startdescr, PrintWriter writer, String codice_spread, String net,
            String spread) {
        String valueSpread = "";
        String valueSpreadNeg = "";
        boolean insertspread = (spread != null) && (newSpread) && (fd(spread) != 0.00);

        if (spread != null) {
            if (spread.contains("-")) {
                valueSpreadNeg = StringUtils.replace(spread, "-", "");
            } else {
                valueSpread = spread;
            }
        }
        if (type.equals("FBB")) { // fattura bank buy 19
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + codice_cliente + separator + separator + importo
                    + separator + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + contocassa + separator + codicecassa + separator + separator + separator + importo
                    + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        } else if (type.equals("FBS")) { // fattura bank sell 20
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + contocassa + separator + codice_filiale + separator + separator + importo + separator
                    + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + separator + separator + separator + importo + separator
                    + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        } else if (type.equals("N_FBB")) { // fattura bank buy 19
            writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + codice_cliente + separator + separator + net + separator
                    + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            if (insertspread) {
                writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg
                        + separator + codice_spread + separator + separator + separator + valueSpreadNeg + separator
                        + valueSpread + separator + codice_filiale + separator + "Spread " + startdescr + " a Ft. N. "
                        + numfat);
            }
            writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg
                    + separator + contocassa + separator + codicecassa + separator + separator + separator + importo
                    + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        } else if (type.equals("N_FBS")) { // fattura bank sell 20
            writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg
                    + separator + contocassa + separator + codice_filiale + separator + separator + importo + separator
                    + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            if (insertspread) {
                writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg
                        + separator + codice_spread + separator + separator + separator + valueSpreadNeg + separator
                        + valueSpread + separator + codice_filiale + separator + "Spread " + startdescr + " a Ft. N. "
                        + numfat);
            }
            writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolver + separator + separator + separator + separator + net + separator
                    + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        }
    }

    private void incasso_fattura(String type, String anno, String data, String n_reg, String cassa,
            String conto_esolverAVERE, String importo, String codice_filiale,
            String numfat, String cognome, String nome, String codice_cliente,
            PrintWriter writer) {
        if (type.equals("IFA")) { // imposta bollo 22
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + cassa + separator
                    + codice_filiale + separator + separator + importo + separator + separator + codice_filiale
                    + separator + "Incasso Fat. N. " + numfat + " " + cognome + " " + nome);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolverAVERE
                    + separator + separator + codice_cliente + separator + separator + importo + separator
                    + codice_filiale + separator + "Incasso Fat. N. " + numfat + " " + cognome + " " + nome);
        }
    }

    private void impostabollo(String type, String anno, String data, String n_reg, String conto_esolverDARE,
            String conto_esolverAVERE,
            String importo, String codice_filiale, String numfat, String cognome, String nome, PrintWriter writer) {
        if (type.equals("FIB")) { // imposta bollo 22
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolverDARE
                    + separator + separator + separator + importo + separator + separator + codice_filiale + separator
                    + "Bollo virtuale su Fat. N. " + numfat + " " + cognome + " " + nome);
            writer.println(tag_GEN + separator + type + separator + anno + separator + data + separator + n_reg
                    + separator + conto_esolverAVERE
                    + separator + separator + separator + separator + importo + separator + codice_filiale + separator
                    + "Bollo virtuale su Fat. N. " + numfat + " " + cognome + " " + nome);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private Client getCL(ArrayList<Client> list, String cod) {
        try {
            return list.stream().filter(r -> r.getCode().equalsIgnoreCase(cod)).findAny().orElse(null);
        } catch (Exception e) {
        }
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getCode().equalsIgnoreCase(cod)) {
                return list.get(i);
            }
        }
        return null;
    }

    public File FILEP5(String path, String data, String anno,
            ArrayList<String[]> fatt_note,
            Branch filiale,
            ArrayList<String[]> contabilita_codici,
            ArrayList<String[]> bank,
            ArrayList<String[]> country,
            String valuta_locale,
            ArrayList<Branch> branch) {

        try {

            ArrayList<String> cod_cliente = new ArrayList<>();
            ArrayList<Client> lista_client = new ArrayList<>();

            for (int i = 0; i < fatt_note.size(); i++) {
                String valori[] = fatt_note.get(i);
                if (valori[0].equals(filiale.getCod())) {
                    ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
                    boolean ca = false;
                    if (livalue.size() > 0) {
                        for (int x = 0; x < livalue.size(); x++) {
                            if (livalue.get(x).getSupporto().equals("04")) {
                                ca = true;
                                break;
                            }
                        }
                        Db_Master db1 = new Db_Master();
                        Client cl = db1.query_Client_transaction(valori[1], valori[2]);
                        db1.closeDB();
                        if (cl != null) {
                            lista_client.add(cl);
                            if (!ca) {
                                if (valori[7].equals("B")) {
                                    cod_cliente.add(cl.getCode());
                                } else if (valori[7].equals("S")) {
                                    cod_cliente.add(cl.getCode());
                                }
                            } else {
                                cod_cliente.add(cl.getCode());
                            }
                        }
                    }
                }
            }

            removeDuplicatesAL(cod_cliente);
            if (cod_cliente.size() > 0) {
                File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P5_eSol.txt");
                try (PrintWriter writer = new PrintWriter(f)) {
                    for (int i = 0; i < cod_cliente.size(); i++) {
                        String cc = cod_cliente.get(i);
                        Client cl = getCL(lista_client, cc);
                        if (cl != null) {
                            Db_Master db1 = new Db_Master();
                            String naz = db1.codnaz_esolv(cl.getNazione());
                            db1.closeDB();

                            String city[] = getCity_apm(cl.getCitta());
                            crea_anagrafica_clienti(cl.getCognome().toUpperCase(),
                                    cl.getNome().toUpperCase(),
                                    cl.getIndirizzo().toUpperCase(),
                                    city[1].toUpperCase(),
                                    naz, cl.getNumero_documento(),
                                    cc, writer);
                        }
                    }
                }
                return f;
            }
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public File FILEP4(String path, String data, String anno,
            ArrayList<String[]> fatt_note,
            Branch filiale,
            ArrayList<String[]> contabilita_codici,
            ArrayList<String[]> bank,
            ArrayList<String[]> country,
            String valuta_locale,
            ArrayList<Branch> branch) {

        try {

            // String conto_RVE = Utility.formatAL("RVE", contabilita_codici, 2);
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P4_eSol.txt");
            int n_reg;
            try (PrintWriter writer = new PrintWriter(f)) {
                n_reg = 1;
                for (int i = 0; i < fatt_note.size(); i++) {
                    String valori[] = fatt_note.get(i);
                    if (valori[0].equals(filiale.getCod())) {
                        String tip = valori[6];
                        if (tip.equals("F")) {
                            tip = "FAT";
                        } else {
                            tip = "NDC";
                        }

                        Ch_transaction ch = query_transaction_ch(valori[1]);
                        CustomerKind ck = get_customerKind(ch.getTipocliente());
                        ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
                        boolean ca = false;
                        if (livalue.size() > 0) {
                            String cadval = "";
                            for (int x = 0; x < livalue.size(); x++) {
                                if (livalue.get(x).getSupporto().equals("04")) {
                                    ca = true;
                                    cadval = livalue.get(x).getPos();
                                    break;
                                }
                            }
                            Db_Master db1 = new Db_Master();
                            Client cl = db1.query_Client_transaction(valori[1], valori[2]);
                            db1.closeDB();

                            if (cl != null) {

                                String contocassa = null;
                                String total = StringUtils.replace(valori[4], ".", ",");
                                String pay = StringUtils.replace(valori[3], ".", ",");

                                double commission = fd(valori[5]) + parseDoubleR(valori[8]);
                                String comm = StringUtils.replace(
                                        roundDoubleandFormat((fd(valori[5]) + parseDoubleR(valori[8])), 2), ".", ",");

                                if (valori[12].trim().equals("0")) {

                                    String pos;
                                    if (valori[10].equals("01")) {
                                        pos = "00";
                                    } else if (valori[10].equals("08")) {
                                        pos = valori[11];
                                    } else {
                                        if (valori[10].equals("-")) {
                                            if (cadval.equals("")) {
                                                pos = "00";
                                            } else {
                                                pos = cadval;
                                            }
                                        } else {
                                            pos = valori[11];
                                        }
                                    }
                                    String spread = StringUtils.replace(valori[13], ".", ",");
                                    String net = StringUtils.replace(
                                            roundDoubleandFormat((fd(valori[4]) - fd(valori[13])), 2), ".", ",");

                                    if (valori[7].equals("B")) {
                                        net = StringUtils.replace(
                                                roundDoubleandFormat((fd(valori[4]) + fd(valori[13])), 2), ".", ",");
                                    }

                                    if (!ca) {

                                        if (valori[7].equals("B")) {
                                            String startdescr = "Acquisto valuta";
                                            if (pos.equals("00")) {
                                                if (tip.equals("FAT")) {
                                                    String codice = formatNewSpread("FBB");
                                                    tofrombank_fattura(codice, anno, data,
                                                            String.valueOf(n_reg),
                                                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                                                            // conto_RVE,
                                                            this.codiceNegozi, filiale.getCod(),
                                                            total, filiale.getCod(), valori[9],
                                                            "", startdescr, writer,
                                                            formatAL("SVVEC", contabilita_codici, 2),
                                                            net,
                                                            spread);
                                                    n_reg++;
                                                }
                                                contocassa = this.codiceNegozi;
                                            } else if (pos.equals("99")) {
                                                if (tip.equals("FAT")) {
                                                    String codice = formatNewSpread("FBB");
                                                    tofrombank_fattura(codice, anno, data,
                                                            String.valueOf(n_reg),
                                                            // conto_RVE,
                                                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                                                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                                                            filiale.getCod(),
                                                            total, filiale.getCod(), valori[9], "", startdescr, writer,
                                                            formatAL("SVVEC", contabilita_codici, 2),
                                                            net, spread);

                                                    n_reg++;
                                                }
                                                contocassa = formatAL(formatNewSpread("AVEC"), contabilita_codici, 2);
                                                // contocassa = conto_RVE;
                                            } else {
                                                if (tip.equals("FAT")) {
                                                    String codice = formatNewSpread("FBB");
                                                    tofrombank_fattura(codice, anno, data,
                                                            String.valueOf(n_reg),
                                                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                                                            // conto_RVE,
                                                            formatAL(pos, bank, 2), pos,
                                                            total, filiale.getCod(), valori[9], "", startdescr, writer,
                                                            formatAL("SVVEC", contabilita_codici, 2),
                                                            net, spread);
                                                    n_reg++;

                                                }
                                                contocassa = formatAL(pos, bank, 2);
                                            }
                                            if (tip.equals("FAT")) {
                                                if (commission >= fd(ck.getIp_soglia_bollo())) {
                                                    impostabollo("FIB", anno, data, String.valueOf(n_reg),
                                                            "630520", // CHIEDERE
                                                            "390509", // CHEIDERE
                                                            StringUtils.replace(ck.getIp_value_bollo(), ".", ","),
                                                            filiale.getCod(), valori[9],
                                                            cl.getCognome(), cl.getNome(),
                                                            writer);
                                                    n_reg++;
                                                }
                                            }
                                        } else if (valori[7].equals("S")) {

                                            if (tip.equals("FAT")) {
                                                String startdescr = "Vendita valuta";
                                                if (pos.equals("00")) {

                                                    if (tip.equals("FAT")) {
                                                        String codice = formatNewSpread("FBS");

                                                        tofrombank_fattura(codice, anno, data,
                                                                String.valueOf(n_reg),
                                                                formatAL(formatNewSpread("VVEC"), contabilita_codici,
                                                                        2),
                                                                this.codiceNegozi, filiale.getCod(),
                                                                total, filiale.getCod(), valori[9], valori[2],
                                                                startdescr, writer,
                                                                formatAL("SVVEC", contabilita_codici, 2),
                                                                net, spread);
                                                        n_reg++;
                                                    }
                                                    contocassa = this.codiceNegozi;
                                                } else if (pos.equals("99")) {
                                                    if (tip.equals("FAT")) {
                                                        String codice = formatNewSpread("FBS");
                                                        tofrombank_fattura(codice, anno, data,
                                                                String.valueOf(n_reg),
                                                                formatAL(formatNewSpread("VVEC"), contabilita_codici,
                                                                        2),
                                                                formatAL(formatNewSpread("VVEC"), contabilita_codici,
                                                                        2),
                                                                filiale.getCod(),
                                                                total, filiale.getCod(), valori[9], valori[2],
                                                                startdescr, writer,
                                                                formatAL("SVVEC", contabilita_codici, 2),
                                                                net, spread);
                                                        n_reg++;

                                                    }
                                                    contocassa = formatAL(formatNewSpread("VVEC"), contabilita_codici,
                                                            2);
                                                } else {

                                                    if (tip.equals("FAT")) {
                                                        String codice = formatNewSpread("FBS");

                                                        tofrombank_fattura(codice, anno, data,
                                                                String.valueOf(n_reg),
                                                                formatAL(formatNewSpread("VVEC"), contabilita_codici,
                                                                        2),
                                                                formatAL(pos, bank, 2), pos,
                                                                total, filiale.getCod(), valori[9], valori[2],
                                                                startdescr, writer,
                                                                formatAL("SVVEC", contabilita_codici, 2),
                                                                net, spread);
                                                        n_reg++;

                                                    }
                                                    contocassa = formatAL(pos, bank, 2);
                                                }
                                                if (tip.equals("FAT")) {
                                                    if (commission >= fd(ck.getIp_soglia_bollo())) {
                                                        impostabollo("FIB", anno, data, String.valueOf(n_reg),
                                                                "630520", // CHIEDERE
                                                                "390509", // CHEIDERE
                                                                StringUtils.replace(ck.getIp_value_bollo(), ".", ","),
                                                                filiale.getCod(), valori[9],
                                                                cl.getCognome(), cl.getNome(),
                                                                writer);
                                                        n_reg++;
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // NUOVO ASPETTARE RAGGI
                                        String startdescr = "Cash advance";
                                        if (pos.equals("00")) {
                                            if (tip.equals("FAT")) {
                                                String codice = "FBB";
                                                tofrombank_fattura(codice, anno, data,
                                                        String.valueOf(n_reg),
                                                        formatAL("CCCA", contabilita_codici, 2),
                                                        this.codiceNegozi,
                                                        filiale.getCod(),
                                                        pay, filiale.getCod(), valori[9],
                                                        "", startdescr, writer, null, null, null);
                                                n_reg++;
                                            }
                                            contocassa = this.codiceNegozi;
                                        } else if (pos.equals("99")) {
                                            if (tip.equals("FAT")) {
                                                String codice = "FBB";
                                                tofrombank_fattura(codice, anno, data,
                                                        String.valueOf(n_reg),
                                                        formatAL("CCCA", contabilita_codici, 2),
                                                        formatAL(formatNewSpread("AVEB"), contabilita_codici, 2),
                                                        // conto_RVE,
                                                        filiale.getCod(),
                                                        pay, filiale.getCod(), valori[9], "", startdescr, writer, null,
                                                        null, null);
                                                n_reg++;
                                            }
                                            contocassa = formatAL(formatNewSpread("AVEB"), contabilita_codici, 2);
                                            // contocassa = conto_RVE;
                                        } else {
                                            String codice = "FBB";
                                            if (tip.equals("FAT")) {
                                                tofrombank_fattura(codice, anno, data,
                                                        String.valueOf(n_reg),
                                                        formatAL(pos, bank, 2),
                                                        formatAL("CANE", contabilita_codici, 2),
                                                        filiale.getCod(),
                                                        pay, filiale.getCod(), valori[9],
                                                        pos, startdescr, writer,
                                                        formatAL("SVVEC", contabilita_codici, 2),
                                                        net, spread);
                                                n_reg++;
                                            }
                                            contocassa = formatAL(pos, bank, 2);
                                        }
                                    }

                                    if (contocassa != null) {
                                        if (tip.equals("FAT")) {
                                            incasso_fattura("IFA", anno, data,
                                                    String.valueOf(n_reg),
                                                    contocassa, this.incassofattura, comm,
                                                    filiale.getCod(), valori[9],
                                                    cl.getCognome(), cl.getNome(),
                                                    valori[2], writer);
                                            n_reg++;
                                        }
                                    }
                                }

                            }

                        }

                    }
                }
            }
            if (n_reg > 1) {
                return f;
            }

        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public File FILEP3(String path, String data, String anno,
            ArrayList<String[]> fatt_note,
            Branch filiale,
            ArrayList<String[]> contabilita_codici,
            ArrayList<String[]> bank,
            ArrayList<String[]> country,
            String valuta_locale,
            ArrayList<Branch> branch) {

        try {

            boolean pr = false;

            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P3_eSol.txt");
            try (PrintWriter writer = new PrintWriter(f)) {
                for (int i = 0; i < fatt_note.size(); i++) {
                    String valori[] = fatt_note.get(i);
                    if (valori[0].equals(filiale.getCod())) {
                        String tip = valori[6];
                        if (tip.equals("F")) {
                            tip = "FAT";
                        } else {
                            tip = "NDC";
                        }

                        ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
                        boolean ca = false;
                        if (livalue.size() > 0) {
                            for (int x = 0; x < livalue.size(); x++) {
                                if (livalue.get(x).getSupporto().equals("04")) {
                                    ca = true;
                                    break;
                                }
                            }
                            Db_Master db1 = new Db_Master();
                            Client cl = db1.query_Client_transaction(valori[1], valori[2]);
                            CustomerKind ck = db1
                                    .get_customerKind(db1.query_transaction_ch(valori[1]).getTipocliente());
                            // String naz = db1.codnaz_esolv(cl.getNazione());
                            db1.closeDB();

                            if (cl != null) {
                                String incassato = "";
                                String comm = StringUtils.replace(
                                        roundDoubleandFormat((fd(valori[5]) + parseDoubleR(valori[8])), 2), ".", ",");
                                if (!ca) {
                                    if (valori[7].equals("B")) {
                                        fattura_new(tip, data, cl.getCode(),
                                                formatAL("CAV", contabilita_codici, 2),
                                                incassato, comm,
                                                filiale.getCod(),
                                                valori[9],
                                                cl.getCognome(),
                                                cl.getNome(), ck.getVatcode(), writer);
                                        pr = true;
                                    } else if (valori[7].equals("S")) {
                                        fattura_new(tip, data, cl.getCode(),
                                                formatAL("CIVV", contabilita_codici, 2),
                                                incassato, comm,
                                                filiale.getCod(),
                                                valori[9],
                                                cl.getCognome(),
                                                cl.getNome(), ck.getVatcode(), writer);
                                        pr = true;
                                    }
                                } else {

                                    // NUOVO ASPETTARE RAGGI
                                    fattura_new(tip, data, cl.getCode(),
                                            // Utility.formatAL(pos, bank, 2),
                                            formatAL("CCCA", contabilita_codici, 2),
                                            incassato, comm,
                                            filiale.getCod(),
                                            valori[9],
                                            cl.getCognome(),
                                            cl.getNome(), ck.getVatcode(), writer);
                                    pr = true;
                                }
                            }
                        }
                    }
                }
            }
            if (pr) {
                return f;
            }
            f.delete();
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public File FILEP2(String path, String data, String anno,
            ArrayList<Ch_transaction> ch_list,
            ArrayList<NC_transaction> nc_list,
            ArrayList<NC_category> listcategory,
            ArrayList<NC_causal> listcausal,
            Branch filiale,
            ArrayList<String[]> contabilita_codici,
            ArrayList<String[]> bank,
            String valuta_locale,
            ArrayList<Branch> branch,
            ArrayList<VATcode> vat,
            CustomerKind ck) {
        try {
            // String conto_RVE = formatAL("RVE", contabilita_codici, 2);
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P2_eSol.txt");
            int nreg;
            try (PrintWriter writer = new PrintWriter(f)) {
                nreg = 1;
                // SELL
                ArrayList<String> se = new ArrayList<>();
                ArrayList<String[]> se_value_temp = new ArrayList<>();
                ArrayList<String[]> se_value = new ArrayList<>();
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("S")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());

                        String pos;
                        switch (ch_list.get(i).getLocalfigures()) {
                            case "01":
                                pos = "00";
                                break;
                            case "08":
                                // pos = "99";
                                pos = ch_list.get(i).getPos();
                                break;
                            default:
                                pos = ch_list.get(i).getPos();
                                break;
                        }
                        if (!valori.isEmpty()) {
                            se.add(pos);
                            for (int x = 0; x < valori.size(); x++) {

                                String[] tmp = { pos, valori.get(x).getNet(), valori.get(x).getTotal(),
                                        valori.get(x).getTot_com(), valori.get(x).getRoundvalue(),
                                        valori.get(x).getSpread() };

                                se_value_temp.add(tmp);
                            }
                        }
                    }
                }
                removeDuplicatesAL(se);
                for (int x = 0; x < se.size(); x++) {

                    double importo_comm = 0.00;
                    double importo_net = 0.00;
                    double importo_t1 = 0.00;
                    double importo_sp = 0.00;

                    for (int i = 0; i < se_value_temp.size(); i++) {
                        if (se.get(x).equals(se_value_temp.get(i)[0])) {
                            importo_comm = importo_comm + parseDoubleR(se_value_temp.get(i)[3])
                                    + parseDoubleR(se_value_temp.get(i)[4]);
                            importo_net = importo_net + parseDoubleR(se_value_temp.get(i)[2]);
                            importo_t1 = importo_t1 + parseDoubleR(se_value_temp.get(i)[1]);
                            importo_sp = importo_sp + fd(se_value_temp.get(i)[5]);
                        }
                    }

                    String[] tmp = {
                            se.get(x),
                            roundDoubleandFormat(importo_comm, 2),
                            roundDoubleandFormat(importo_net, 2),
                            roundDoubleandFormat(importo_t1, 2),
                            roundDoubleandFormat(importo_sp, 2)
                    };
                    se_value.add(tmp);
                }
                for (int x = 0; x < se_value.size(); x++) {

                    String[] valore = se_value.get(x);

                    double corrisp = fd(valore[1]);
                    double nuovoimporto = fd(valore[3]);
                    double com1 = fd(valore[1]);

                    double rig1 = nuovoimporto - com1;

                    double spr_val = fd(valore[4]);
                    double net_val = rig1 - spr_val;

                    if (corrisp != 0) {

                        String newrig1 = StringUtils.replace(roundDoubleandFormat(rig1, 2), ".", ",");
                        String newimp = StringUtils.replace(roundDoubleandFormat(nuovoimporto, 2), ".", ",");
                        String commissioni = StringUtils.replace(valore[1], ".", ",");

                        String net = StringUtils.replace(roundDoubleandFormat(net_val, 2), ".", ",");
                        String spread = StringUtils.replace(roundDoubleandFormat(spr_val, 2), ".", ",");

                        switch (valore[0]) {
                            case "00": {
                                writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator
                                        + data + separator + nreg
                                        + separator + separator + separator + separator + separator + separator
                                        + separator + separator + separator + separator + separator + separator
                                        + separator
                                        + "Corrispettivi " + filiale.getDe_branch() + separator);
                                String codice = formatNewSpread("SEL");
                                sell(codice, anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        newrig1,
                                        commissioni,
                                        filiale.getDe_branch(),
                                        formatAL("CIVV", contabilita_codici, 2),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        null, null, ck.getVatcode(), writer,
                                        newimp, scontrino_NORM,
                                        formatAL("SVVEC", contabilita_codici, 2),
                                        net, spread);
                                nreg++;
                                break;
                            }
                            case "99": {
                                writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator
                                        + data + separator + nreg
                                        + separator + separator + separator + separator + separator + separator
                                        + separator + separator + separator + separator + separator + separator
                                        + separator
                                        + "Corrispettivi " + filiale.getDe_branch() + separator);
                                String codice = formatNewSpread("SEL");
                                sell(codice, anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        newrig1,
                                        commissioni,
                                        filiale.getDe_branch(),
                                        formatAL("CIVV", contabilita_codici, 2),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        valore[0], ck.getVatcode(), writer,
                                        newimp, scontrino_NORM,
                                        formatAL("SVVEC", contabilita_codici, 2),
                                        net, spread);
                                nreg++;
                                break;
                            }
                            default: {
                                writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator
                                        + data + separator + nreg
                                        + separator + separator + separator + separator + separator + separator
                                        + separator + separator + separator + separator + separator + separator
                                        + separator
                                        + "Corrispettivi " + filiale.getDe_branch() + separator);
                                String codice = formatNewSpread("SEL");
                                sell(codice, anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        newrig1,
                                        commissioni,
                                        filiale.getDe_branch(),
                                        formatAL("CIVV", contabilita_codici, 2),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        formatAL(valore[0], bank, 2),
                                        valore[0],
                                        ck.getVatcode(), writer, newimp, scontrino_NORM,
                                        formatAL("SVVEC", contabilita_codici, 2),
                                        net, spread);
                                nreg++;
                                break;
                            }
                        }
                    }
                } // BUY
                double sprB1 = 0.00;
                double tot1 = 0.00;
                double comm1 = 0.00;
                double net1 = 0.00;
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("B")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
                        if (!valori.isEmpty()) {
                            for (int x = 0; x < valori.size(); x++) {
                                Ch_transaction_value val = valori.get(x);
                                if (!val.getSupporto().equals("04")) {
                                    tot1 = tot1 + parseDoubleR(val.getTotal());
                                    comm1 = comm1 + parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue());
                                    net1 = net1 + parseDoubleR(val.getNet());
                                    sprB1 = sprB1 + fd(val.getSpread());
                                }
                            }
                        }
                    }
                }

                if (tot1 > 0 && comm1 != 0) {

                    // double rig1 = tot1 - comm1;
                    // double net_val = rig1 - sprB1;
                    double spread_ok = tot1 + sprB1 - net1 - comm1;

                    writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data
                            + separator + nreg
                            + separator + separator + separator + separator + separator + separator
                            + separator + separator + separator + separator + separator + separator + separator
                            + "Corrispettivi " + filiale.getDe_branch() + separator);

                    String codice = formatNewSpread("BUY");
                    buy(codice, anno, data,
                            String.valueOf(nreg),
                            filiale.getCod(),
                            StringUtils.replace(roundDoubleandFormat(net1, 2), ".", ","),
                            formatAL("CAV", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(comm1, 2), ".", ","),
                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(tot1, 2), ".", ","),
                            ck.getVatcode(), writer, scontrino_NORM,
                            formatAL("SVVEC", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(tot1 + sprB1, 2), ".", ","),
                            StringUtils.replace(roundDoubleandFormat(spread_ok, 2), ".", ",")
                    // StringUtils.replace(roundDoubleandFormat(sprB1, 2), ".", ",")
                    );
                    nreg++;
                }

                // BUY - CASH ADVANCE - CON COMMISSIONI
                ArrayList<String> b_ca_c = new ArrayList<>();
                ArrayList<String[]> b_ca_c_value_temp = new ArrayList<>();
                ArrayList<String[]> b_ca_c_value = new ArrayList<>();
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("B")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
                        if (!valori.isEmpty()) {
                            for (int x = 0; x < valori.size(); x++) {
                                Ch_transaction_value val = valori.get(x);
                                if (val.getSupporto().equals("04")) {
                                    if (parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue()) > 0.0D) {
                                        b_ca_c.add(val.getPos());
                                        String[] tmp = { val.getPos(), val.getNet(), val.getTotal(), val.getTot_com(),
                                                val.getRoundvalue() };
                                        b_ca_c_value_temp.add(tmp);
                                    }
                                }
                            }
                        }
                    }
                }
                removeDuplicatesAL(b_ca_c);
                for (int x = 0; x < b_ca_c.size(); x++) {
                    double importo_b_ca_sc_net = 0.00;
                    double importo_b_ca_sc_total = 0.00;
                    double importo_b_ca_sc_comm = 0.00;
                    for (int i = 0; i < b_ca_c_value_temp.size(); i++) {
                        if (b_ca_c.get(x).equals(b_ca_c_value_temp.get(i)[0])) {
                            importo_b_ca_sc_net = importo_b_ca_sc_net + parseDoubleR(b_ca_c_value_temp.get(i)[1]);
                            importo_b_ca_sc_total = importo_b_ca_sc_total + parseDoubleR(b_ca_c_value_temp.get(i)[2]);
                            importo_b_ca_sc_comm = importo_b_ca_sc_comm + parseDoubleR(b_ca_c_value_temp.get(i)[3])
                                    + parseDoubleR(b_ca_c_value_temp.get(i)[4]);
                        }
                    }
                    String[] tmp = { b_ca_c.get(x), roundDoubleandFormat(importo_b_ca_sc_net, 2),
                            roundDoubleandFormat(importo_b_ca_sc_total, 2),
                            roundDoubleandFormat(importo_b_ca_sc_comm, 2) };
                    b_ca_c_value.add(tmp);
                }
                for (int x = 0; x < b_ca_c_value.size(); x++) {
                    String[] valore = b_ca_c_value.get(x);
                    writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data
                            + separator + nreg
                            + separator + separator + separator + separator + separator + separator
                            + separator + separator + separator + separator + separator + separator + separator
                            + "Corrispettivi " + filiale.getDe_branch() + separator);
                    cashAdvance("CAC", anno, data,
                            String.valueOf(nreg),
                            formatAL("CCCA", contabilita_codici, 2),
                            StringUtils.replace(valore[1], ".", ","),
                            StringUtils.replace(valore[2], ".", ","),
                            StringUtils.replace(valore[3], ".", ","),
                            filiale.getDe_branch(),
                            filiale.getCod(),
                            formatAL(valore[0], bank, 2),
                            formatAL("CANE", contabilita_codici, 2), // CASSE NEGOZI //14/11
                            ck.getVatcode(), writer, scontrino_NORM);
                    nreg++;

                }
                ArrayList<String[]> nochangecorr = new ArrayList<>();
                ArrayList<String[]> nochangecorr_value = new ArrayList<>();
                ArrayList<String[]> ti = new ArrayList<>();
                ArrayList<String[]> ti_value = new ArrayList<>();
                for (int i = 0; i < nc_list.size(); i++) {
                    NC_category nc0 = getNC_category(listcategory, nc_list.get(i).getGruppo_nc());
                    NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc(),
                            nc_list.get(i).getGruppo_nc());

                    if (nc0 != null && nc1 != null && !nc1.getNc_de().equals("14")) {

                        if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                                && nc_list.get(i).getFg_tipo_transazione_nc().equals("21")) {
                            String comm;
                            if (fd(nc_list.get(i).getCommissione()) > 0) {
                                comm = nc_list.get(i).getCommissione();
                            } else {
                                comm = nc_list.get(i).getTi_ticket_fee();
                            }

                            comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
                            if (parseDoubleR(comm) > 0.0D) {

                                String pos;
                                if (nc_list.get(i).getSupporto().equals("01")
                                        || nc_list.get(i).getSupporto().equals("...")) {
                                    pos = "00";
                                } else if (nc_list.get(i).getSupporto().equals("08")) {
                                    // pos = "99";
                                    pos = nc_list.get(i).getPos();
                                } else {
                                    pos = nc_list.get(i).getPos();
                                }
                                String[] va = { nc_list.get(i).getGruppo_nc(), pos };
                                ti.add(va);
                            }
                        } else if (nc_list.get(i).getFiliale().equals(filiale.getCod())) { // PROD

                            NC_vatcode vat1 = get_NC_vatcode(nc0.getGruppo_nc());
                            if (vat1 != null) {
                                String pos;
                                if (nc_list.get(i).getSupporto().equals("01")
                                        || nc_list.get(i).getSupporto().equals("...")) {
                                    pos = "00";
                                } else if (nc_list.get(i).getSupporto().equals("08")) {
                                    pos = nc_list.get(i).getPos();
                                } else {
                                    pos = nc_list.get(i).getPos();
                                }
                                String[] va = { nc_list.get(i).getGruppo_nc(), pos };
                                nochangecorr.add(va);
                            }
                        }
                    }
                }

                removeDuplicatesALAr(ti);
                removeDuplicatesALAr(nochangecorr);
                for (int x = 0; x < nochangecorr.size(); x++) {
                    double importo = 0.00;
                    double QUANTITA = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }

                        if (nc_list.get(i).getGruppo_nc().equals(nochangecorr.get(x)[0])
                                & nc_list.get(i).getFiliale().equals(filiale.getCod())
                                && pos.equals(nochangecorr.get(x)[1])) {
                            NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc(),
                                    nc_list.get(i).getGruppo_nc());
                            if (nc1 != null) {
                                if (!nc1.getDe_causale_nc().toUpperCase().contains("ACQUISTO")) {
                                    importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                                    QUANTITA = QUANTITA + parseDoubleR(nc_list.get(i).getQuantita());
                                }
                            }
                        }

                    }

                    String[] va = { nochangecorr.get(x)[0], nochangecorr.get(x)[1], roundDoubleandFormat(importo, 2),
                            roundDoubleandFormat(QUANTITA, 0) };
                    nochangecorr_value.add(va);
                } // NUOVA VERSIONE DI CONTI SEPARATI RAF
                for (int x = 0; x < nochangecorr_value.size(); x++) {
                    String valori[] = nochangecorr_value.get(x);
                    NC_category nc0 = getNC_category(listcategory, valori[0]);
                    if (nc0 != null) {
                        if (nc0.getFg_tipo_transazione_nc().equals("2")) {

                            NC_vatcode vat1 = get_NC_vatcode(nc0.getGruppo_nc());

                            VATcode va2 = null;
                            if (vat1.getAccountingcode1().equals("")) {
                                vat1.setAccountingcode1(nc0.getConto_coge_01());
                                vat1.setVatcode1(nc0.getConto_coge_02());
                                vat1.setPrice1(nc0.getIp_prezzo_nc());
                            } else {
                                va2 = get_vat(vat1.getVatcode2(), vat);
                            }

                            VATcode va1 = get_vat(vat1.getVatcode1(), vat);

                            if (va1 != null) {
                                double total1 = fd(valori[3]) * fd(vat1.getPrice1());
                                double imposta1 = calcolaIva(total1, fd(va1.getAliquota()));
                                double imponibile1 = total1 - fd(roundDoubleandFormat(imposta1, 2));
                                String importo = StringUtils.replace(
                                        StringUtils.replace(roundDoubleandFormat(total1, 2), ".", ","), "-", "").trim();
                                String imposta = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imposta1, 2), ".", ","), "-",
                                                "")
                                        .trim();
                                String imponibile = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imponibile1, 2), ".", ","),
                                                "-", "")
                                        .trim();
                                String contocassa;
                                String codicecassa = "";
                                if (valori[1].equals("00")) {
                                    contocassa = this.codiceNegozi;
                                    codicecassa = filiale.getCod();
                                } else if (valori[1].equals("99")) {
                                    contocassa = formatAL(formatNewSpread("VVEC"), contabilita_codici, 2);
                                    // contocassa = conto_RVE;
                                } else {
                                    contocassa = formatAL(valori[1], bank, 2);
                                    codicecassa = valori[1];
                                }
                                ticket_sc_corrispettivo("CTI", anno, data, String.valueOf(nreg),
                                        vat1.getAccountingcode1(), importo, imponibile, imposta, contocassa,
                                        codicecassa,
                                        filiale.getCod(), filiale.getDe_branch(), nc0.getDe_gruppo_nc(),
                                        vat1.getVatcode1(), writer);
                                nreg++;

                            }

                            if (va2 != null) {
                                double total1 = fd(valori[3]) * fd(vat1.getPrice2());
                                double imposta1 = calcolaIva(total1, fd(va2.getAliquota()));
                                double imponibile1 = total1 - fd(roundDoubleandFormat(imposta1, 2));
                                String importo = StringUtils.replace(StringUtils.replace(
                                        roundDoubleandFormat(total1, 2),
                                        ".", ","), "-", "").trim();
                                String imposta = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imposta1, 2), ".", ","), "-",
                                                "")
                                        .trim();
                                String imponibile = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imponibile1, 2), ".", ","),
                                                "-", "")
                                        .trim();
                                String contocassa;
                                String codicecassa = "";
                                if (valori[1].equals("00")) {
                                    contocassa = this.codiceNegozi;
                                    codicecassa = filiale.getCod();
                                } else if (valori[1].equals("99")) {
                                    contocassa = formatAL(formatNewSpread("VVEC"), contabilita_codici, 2);
                                } else {
                                    contocassa = formatAL(valori[1], bank, 2);
                                    codicecassa = valori[1];
                                }
                                ticket_sc_corrispettivo("CTI", anno, data, String.valueOf(nreg),
                                        vat1.getAccountingcode2(), importo, imponibile, imposta, contocassa,
                                        codicecassa,
                                        filiale.getCod(), filiale.getDe_branch(), nc0.getDe_gruppo_nc(),
                                        vat1.getVatcode2(), writer);
                                nreg++;
                            }
                        } else {

                            VATcode va1 = get_vat(nc0.getConto_coge_02(), vat);
                            if (va1 != null) {
                                double total1 = fd(valori[2]);
                                double imposta1 = calcolaIva(total1, fd(va1.getAliquota()));
                                double imponibile1 = total1 - fd(roundDoubleandFormat(imposta1, 2));
                                String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "")
                                        .trim();
                                String imposta = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imposta1, 2), ".", ","), "-",
                                                "")
                                        .trim();
                                String imponibile = StringUtils
                                        .replace(StringUtils.replace(roundDoubleandFormat(imponibile1, 2), ".", ","),
                                                "-", "")
                                        .trim();
                                String contocassa;
                                String codicecassa = "";
                                if (valori[1].equals("00")) {
                                    contocassa = this.codiceNegozi;
                                    codicecassa = filiale.getCod();
                                } else if (valori[1].equals("99")) {
                                    contocassa = formatAL(formatNewSpread("VVEC"), contabilita_codici, 2);
                                } else {
                                    contocassa = formatAL(valori[1], bank, 2);
                                    codicecassa = valori[1];
                                }
                                ticket_sc_corrispettivo("CTI", anno, data, String.valueOf(nreg),
                                        nc0.getConto_coge_01(), importo, imponibile, imposta, contocassa, codicecassa,
                                        filiale.getCod(), filiale.getDe_branch(), nc0.getDe_gruppo_nc(),
                                        nc0.getConto_coge_02(), writer);
                                nreg++;
                            }
                        }
                    }
                }
                for (int x = 0; x < ti.size(); x++) {
                    double importo = 0.00;
                    double fee = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }
                        String comm;
                        if (fd(nc_list.get(i).getCommissione()) > 0) {
                            comm = nc_list.get(i).getCommissione();
                        } else {
                            comm = nc_list.get(i).getTi_ticket_fee();
                        }

                        if (fd(comm) > 0) {
                            boolean add = fd(nc_list.get(i).getTotal()) >= 0;
                            if (nc_list.get(i).getGruppo_nc().equals(ti.get(x)[0])
                                    & nc_list.get(i).getFiliale().equals(filiale.getCod())
                                    && pos.equals(ti.get(x)[1])) {
                                if (add) {
                                    importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                                    fee = fee + parseDoubleR(comm);
                                } else {
                                    importo = importo - parseDoubleR(nc_list.get(i).getTotal());
                                    fee = fee - parseDoubleR(comm);
                                }
                            }
                        }
                    }

                    VATcode va1 = get_vat("22", vat); //

                    double valueiva = fd(va1.getAliquota());
                    double d1 = calcolaIva(fee, valueiva);
                    double d2 = fee - fd(roundDoubleandFormat(d1, 2));

                    String[] va = { ti.get(x)[0], ti.get(x)[1],
                            roundDoubleandFormat(importo, 2),
                            roundDoubleandFormat(fee, 2),
                            roundDoubleandFormat(importo - fee, 2),
                            roundDoubleandFormat(d1, 2),
                            roundDoubleandFormat(d2, 2),
                            roundDoubleandFormat(valueiva, 0)
                    };
                    ti_value.add(va);
                }
                for (int x = 0; x < ti_value.size(); x++) {
                    String valori[] = ti_value.get(x);
                    String importo = StringUtils.replace(valori[2], ".", ",").trim();
                    String fee = StringUtils.replace(valori[3], ".", ",").trim();
                    String net = StringUtils.replace(valori[4], ".", ",").trim();
                    String imp1 = StringUtils.replace(valori[6], ".", ",").trim();
                    String netimposta = StringUtils.replace(valori[5], ".", ",").trim();
                    // String importo = StringUtils.replace(StringUtils.replace(valori[2], ".",
                    // ","), "-", "").trim();
                    // String fee = StringUtils.replace(StringUtils.replace(valori[3], ".", ","),
                    // "-", "").trim();
                    // String net = StringUtils.replace(StringUtils.replace(valori[4], ".", ","),
                    // "-", "").trim();
                    // String imp1 = StringUtils.replace(StringUtils.replace(valori[6], ".", ","),
                    // "-", "").trim();
                    // String netimposta = StringUtils.replace(StringUtils.replace(valori[5], ".",
                    // ","), "-", "").trim();

                    NC_category nc0 = getNC_category(listcategory, valori[0]);
                    if (nc0 != null) {
                        if (valori[1].equals("00")) {
                            writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data
                                    + separator + nreg
                                    + separator + separator + separator + separator + separator + separator
                                    + separator + separator + separator + separator + separator + separator + separator
                                    + "Corrispettivi " + filiale.getDe_branch() + separator);
                            ticket_new("TCC", anno, data, String.valueOf(nreg),
                                    filiale.getCod(),
                                    fee, valori[7],
                                    imp1, netimposta, importo,
                                    net,
                                    nc0.getConto_coge_02(),
                                    nc0.getConto_coge_01(),
                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                    this.codiceNegozi, filiale.getCod(), writer, scontrino_NORM);
                            nreg++;
                        } else if (valori[1].equals("99")) {
                            writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data
                                    + separator + nreg
                                    + separator + separator + separator + separator + separator + separator
                                    + separator + separator + separator + separator + separator + separator + separator
                                    + "Corrispettivi " + filiale.getDe_branch() + separator);
                            ticket_new("TCC", anno, data, String.valueOf(nreg),
                                    filiale.getCod(),
                                    fee, valori[7],
                                    imp1, netimposta, importo,
                                    net,
                                    nc0.getConto_coge_02(),
                                    nc0.getConto_coge_01(),
                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                    formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                    // conto_RVE,
                                    "",
                                    writer, scontrino_NORM);
                            nreg++;
                        } else {
                            writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data
                                    + separator + nreg
                                    + separator + separator + separator + separator + separator + separator
                                    + separator + separator + separator + separator + separator + separator + separator
                                    + "Corrispettivi " + filiale.getDe_branch() + separator);
                            ticket_new("TCC", anno, data, String.valueOf(nreg),
                                    filiale.getCod(),
                                    fee, valori[7],
                                    imp1, netimposta, importo,
                                    net,
                                    nc0.getConto_coge_02(),
                                    nc0.getConto_coge_01(),
                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                    formatAL(valori[1], bank, 2),
                                    valori[1],
                                    writer, scontrino_NORM);
                            nreg++;
                        }
                    }
                }
            }
            if (nreg > 1) {
                return f;
            }
            f.delete();
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public File FILEP1(String path, String data, String anno,
            ArrayList<Ch_transaction> ch_list,
            ArrayList<NC_transaction> nc_list,
            ArrayList<Ch_transaction_refund> list_esolver_refund,
            ArrayList<ET_change> et_list,
            ArrayList<Openclose> oc_list,
            ArrayList<NC_category> listcategory,
            ArrayList<NC_causal> listcausal,
            ArrayList<Users> listusers,
            Branch filiale,
            ArrayList<String[]> contabilita_codici,
            ArrayList<String[]> bank,
            String valuta_locale,
            ArrayList<Branch> branch, boolean dividi,
            CustomerKind ck) {

        try {
            // String conto_RVE = formatAL("RVE", contabilita_codici, 2);
            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P1_eSol.txt");
            int nreg;
            try (PrintWriter writer = new PrintWriter(f)) {
                nreg = 1;
                // SELL NO COMM
                ArrayList<String> se = new ArrayList<>();
                ArrayList<String[]> se_value_temp = new ArrayList<>();
                ArrayList<String[]> se_value = new ArrayList<>();
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("S")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
                        String pos;
                        if (ch_list.get(i).getLocalfigures().equals("01")) {
                            pos = "00";
                        } else if (ch_list.get(i).getLocalfigures().equals("08")) {
                            pos = ch_list.get(i).getPos();
                        } else {
                            pos = ch_list.get(i).getPos();
                        }
                        if (!valori.isEmpty()) {
                            se.add(pos);
                            for (int x = 0; x < valori.size(); x++) {
                                String[] tmp = { pos, valori.get(x).getNet(), valori.get(x).getTotal(),
                                        valori.get(x).getTot_com(), valori.get(x).getRoundvalue(),
                                        valori.get(x).getSpread() };
                                se_value_temp.add(tmp);
                            }
                        }
                    }
                }
                removeDuplicatesAL(se);
                for (int x = 0; x < se.size(); x++) {

                    double importo_spread = 0.00;

                    double importo_comm = 0.00;
                    double importo_net = 0.00;

                    for (int i = 0; i < se_value_temp.size(); i++) {
                        if (se.get(x).equals(se_value_temp.get(i)[0])) {
                            importo_comm = importo_comm + parseDoubleR(se_value_temp.get(i)[3])
                                    + parseDoubleR(se_value_temp.get(i)[4]);
                            importo_net = importo_net + parseDoubleR(se_value_temp.get(i)[1]);
                            importo_spread = importo_spread + fd(se_value_temp.get(i)[5]);
                        }
                    }
                    String[] tmp = {
                            se.get(x),
                            roundDoubleandFormat(importo_comm, 2),
                            roundDoubleandFormat(importo_net, 2),
                            roundDoubleandFormat(importo_spread, 2)

                    };
                    se_value.add(tmp);
                }
                for (int x = 0; x < se_value.size(); x++) {
                    String[] valore = se_value.get(x);

                    double corrisp = fd(valore[1]);

                    double spr_dbl = fd(valore[3]);

                    double nuovoimporto = fd(valore[2]) + fd(valore[1]);
                    if (corrisp == 0) {
                        String newimp = StringUtils.replace(roundDoubleandFormat(nuovoimporto, 2), ".", ",");
                        String net = StringUtils.replace(roundDoubleandFormat(nuovoimporto - spr_dbl, 2), ".", ",");
                        String spread = StringUtils.replace(roundDoubleandFormat(spr_dbl, 2), ".", ",");

                        if (valore[0].equals("00")) {

                            String codice = formatNewSpread("SSC");

                            sell(codice, anno, data,
                                    String.valueOf(nreg),
                                    filiale.getCod(),
                                    StringUtils.replace(valore[2], ".", ","),
                                    StringUtils.replace(valore[1], ".", ","),
                                    filiale.getDe_branch(),
                                    formatAL("CIVV", contabilita_codici, 2),
                                    formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                    this.codiceNegozi,
                                    filiale.getCod(),
                                    ck.getVatcode(), writer,
                                    newimp, scontrino_NORM,
                                    formatAL("SVVEC", contabilita_codici, 2),
                                    net, spread);
                            nreg++;
                        } else if (valore[0].equals("99")) {
                            String codice = formatNewSpread("SSC");
                            sell(codice, anno, data, String.valueOf(nreg),
                                    filiale.getCod(),
                                    StringUtils.replace(valore[2], ".", ","), StringUtils.replace(valore[1], ".", ","),
                                    filiale.getDe_branch(),
                                    formatAL("CIVV", contabilita_codici, 2),
                                    formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                    formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                    valore[0], ck.getVatcode(), writer, newimp, scontrino_NORM,
                                    formatAL("SVVEC", contabilita_codici, 2), // SOSTITUIRE
                                    net, spread);

                            nreg++;
                        } else {
                            String codice = formatNewSpread("SSC");
                            sell(codice, anno, data, String.valueOf(nreg),
                                    filiale.getCod(),
                                    StringUtils.replace(valore[2], ".", ","), StringUtils.replace(valore[1], ".", ","),
                                    filiale.getDe_branch(),
                                    formatAL("CIVV", contabilita_codici, 2),
                                    formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                    formatAL(valore[0], bank, 2),
                                    valore[0],
                                    ck.getVatcode(), writer, newimp, scontrino_NORM,
                                    formatAL("SVVEC", contabilita_codici, 2), // SOSTITUIRE
                                    net, spread);
                            nreg++;
                        }
                    }
                } // BUY
                double tot1 = 0.00;
                double comm1 = 0.00;
                double net1 = 0.00;
                double sprB1 = 0.00;
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("B")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
                        if (!valori.isEmpty()) {
                            for (int x = 0; x < valori.size(); x++) {
                                Ch_transaction_value val = valori.get(x);
                                if (!val.getSupporto().equals("04")) {
                                    tot1 = tot1 + parseDoubleR(val.getTotal());
                                    comm1 = comm1 + parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue());
                                    net1 = net1 + parseDoubleR(val.getNet());
                                    sprB1 = sprB1 + fd(val.getSpread());
                                }
                            }
                        }
                    }
                }
                if (tot1 > 0 && comm1 == 0) {
                    String codice = formatNewSpread("BSC");
                    buy(codice, anno, data,
                            String.valueOf(nreg),
                            filiale.getCod(),
                            StringUtils.replace(roundDoubleandFormat(net1, 2), ".", ","),
                            formatAL("CAV", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(comm1, 2), ".", ","),
                            formatAL(formatNewSpread("AVEB"), contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(tot1, 2), ".", ","),
                            ck.getVatcode(), writer, scontrino_NORM,
                            formatAL("SVVEC", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(tot1 + sprB1, 2), ".", ","),
                            StringUtils.replace(roundDoubleandFormat(sprB1, 2), ".", ","));
                    nreg++;
                }

                // ERRORI CASSA
                double errori_cassa_pos = 0.00;
                double errori_cassa_neg = 0.00;

                for (int i = 0; i < oc_list.size(); i++) {
                    if (oc_list.get(i).getFiliale().equals(filiale.getCod())) {
                        ArrayList<String[]> list_oc_errors = list_oc_errors(oc_list.get(i).getCod());
                        for (int c = 0; c < list_oc_errors.size(); c++) {
                            String[] value = list_oc_errors.get(c);

                            if (value[1].equals("CH") && value[2].equals(valuta_locale)) {
                                String diff = value[7];
                                if (fd(diff) > 0) {
                                    errori_cassa_pos = errori_cassa_pos + parseDoubleR(diff);
                                } else {
                                    errori_cassa_neg = errori_cassa_neg + parseDoubleR(diff);
                                }
                            }
                        }
                    }
                }

                boolean erpos = false;
                boolean erneg = false;

                if (errori_cassa_pos > errori_cassa_neg) {
                    errori_cassa_pos = errori_cassa_pos - errori_cassa_neg;
                    erpos = true;
                } else if (errori_cassa_pos < errori_cassa_neg) {
                    errori_cassa_neg = errori_cassa_neg - errori_cassa_pos;
                    erneg = true;
                }

                if (erpos) {
                    erroriCassa("ERA", anno, data,
                            String.valueOf(nreg),
                            formatAL("EPT", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(errori_cassa_pos, 2), ".", ","),
                            filiale.getDe_branch(), filiale.getCod(), writer);

                    nreg++;
                } else if (erneg) {
                    erroriCassa("ERR", anno, data,
                            String.valueOf(nreg),
                            formatAL("ENT", contabilita_codici, 2),
                            StringUtils.replace(roundDoubleandFormat(errori_cassa_neg, 2), ".", ","),
                            filiale.getDe_branch(), filiale.getCod(), writer);
                    nreg++;
                }

                // RIMBORSI
                for (int i = 0; i < list_esolver_refund.size(); i++) {
                    if (list_esolver_refund.get(i).getBranch_cod().equals(filiale.getCod())) {
                        String importo = StringUtils.replace(list_esolver_refund.get(i).getValue(), "-", "");
                        rimborso("RIB", anno, data,
                                String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(importo, ".", ","),
                                filiale.getDe_branch(),
                                formatAL("CORI", contabilita_codici, 2), writer);
                        nreg++;
                    }
                } // TO BRANCH
                ArrayList<String> to_br = new ArrayList<>();
                ArrayList<String[]> to_br_value = new ArrayList<>();
                for (int i = 0; i < et_list.size(); i++) {
                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
                            && et_list.get(i).getFg_tofrom().equals("T")
                            && et_list.get(i).getFg_brba().equals("BR")) {
                        to_br.add(et_list.get(i).getCod_dest());
                    }
                }
                removeDuplicatesAL(to_br);
                for (int x = 0; x < to_br.size(); x++) {
                    double importo_val_loc = 0.00;
                    for (int i = 0; i < et_list.size(); i++) {
                        if (et_list.get(i).getFiliale().equals(filiale.getCod())
                                && et_list.get(i).getFg_tofrom().equals("T")
                                && et_list.get(i).getFg_brba().equals("BR")
                                && et_list.get(i).getCod_dest().equals(to_br.get(x))) {
                            ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
                            if (!valori.isEmpty()) {
                                for (int z = 0; z < valori.size(); z++) {
                                    if (valori.get(z).getValuta().equals(valuta_locale)
                                            && valori.get(z).getSupporto().equals("01")) {
                                        importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
                                    }
                                }
                            }
                        }
                    }

                    String[] val = { to_br.get(x), roundDoubleandFormat(importo_val_loc, 2) };
                    to_br_value.add(val);
                }
                for (int i = 0; i < to_br_value.size(); i++) {
                    String valore[] = to_br_value.get(i);

                    if (parseDoubleR(valore[1]) > 0) { // LOCALE
                        extBranch("TBR", anno, data,
                                String.valueOf(nreg), valore[0],
                                filiale.getCod(),
                                StringUtils.replace(valore[1], ".", ","),
                                filiale.getDe_branch(),
                                formatBankBranchReport(valore[0], "BR", null, branch), writer);
                        nreg++;
                    }
                } // TO BANK
                ArrayList<String> to_ba = new ArrayList<>();
                ArrayList<String[]> to_ba_value = new ArrayList<>();
                for (int i = 0; i < et_list.size(); i++) {

                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
                            && et_list.get(i).getFg_tofrom().equals("T")
                            && et_list.get(i).getFg_brba().equals("BA")) {
                        to_ba.add(et_list.get(i).getCod_dest());
                    }
                }
                removeDuplicatesAL(to_ba);
                for (int x = 0; x < to_ba.size(); x++) {

                    double importo_val_loc = 0.00;
                    double importo_val_est = 0.00;
                    double importo_spread = 0.00;

                    for (int i = 0; i < et_list.size(); i++) {
                        if (et_list.get(i).getFiliale().equals(filiale.getCod())
                                && et_list.get(i).getFg_tofrom().equals("T")
                                && et_list.get(i).getFg_brba().equals("BA")
                                && et_list.get(i).getCod_dest().equals(to_ba.get(x))) {
                            ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
                            if (!valori.isEmpty()) {
                                for (int z = 0; z < valori.size(); z++) {
                                    if (valori.get(z).getValuta().equals(valuta_locale)
                                            && valori.get(z).getSupporto().equals("01")) {
                                        importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
                                    } else {
                                        importo_val_est = importo_val_est + parseDoubleR(valori.get(z).getIp_total());
                                    }

                                    importo_spread = importo_spread + fd(valori.get(z).getIp_spread());

                                }
                            }
                        }
                    }
                    String[] val = { to_ba.get(x), roundDoubleandFormat(importo_val_loc, 2),
                            roundDoubleandFormat(importo_val_est, 2), roundDoubleandFormat(importo_spread, 2) };
                    to_ba_value.add(val);

                }
                for (int i = 0; i < to_ba_value.size(); i++) {
                    String valore[] = to_ba_value.get(i);

                    if (parseDoubleR(valore[1]) > 0) { // LOCALE

                        extBank("TBL", anno, data,
                                String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(valore[1], ".", ","),
                                formatAL(valore[0], bank, 1),
                                formatAL(valore[0], bank, 2),
                                valore[0],
                                this.codiceNegozi,
                                writer, true, null, null, null);
                        nreg++;
                    }
                    if (parseDoubleR(valore[2]) > 0) { // ESTERA

                        double spr_dbl = fd(valore[3]);
                        double net_dbl = fd(valore[2]) - spr_dbl;

                        String codice = formatNewSpread("TBE");

                        extBank(codice, anno, data,
                                String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(valore[2], ".", ","),
                                formatAL(valore[0], bank, 1),
                                formatAL(valore[0], bank, 2),
                                valore[0],
                                formatAL(formatNewSpread("VVEB"), contabilita_codici, 2),
                                writer, false,
                                formatAL("SVVEB", contabilita_codici, 2), // SOSTITUIRE
                                StringUtils.replace(roundDoubleandFormat(net_dbl, 2), ".", ","),
                                StringUtils.replace(roundDoubleandFormat(spr_dbl, 2), ".", ","));
                        nreg++;
                    }
                } // FROM BANK
                ArrayList<String> fr_ba = new ArrayList<>();
                ArrayList<String[]> fr_ba_value = new ArrayList<>();
                for (int i = 0; i < et_list.size(); i++) {
                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
                            && et_list.get(i).getFg_tofrom().equals("F")
                            && et_list.get(i).getFg_brba().equals("BA")) {
                        fr_ba.add(et_list.get(i).getCod_dest());
                    }
                }
                removeDuplicatesAL(fr_ba);
                for (int x = 0; x < fr_ba.size(); x++) {

                    double importo_val_loc = 0.00;
                    double importo_val_est = 0.00;
                    double importo_spread = 0.00;
                    for (int i = 0; i < et_list.size(); i++) {
                        if (et_list.get(i).getFiliale().equals(filiale.getCod())
                                && et_list.get(i).getFg_tofrom().equals("F")
                                && et_list.get(i).getFg_brba().equals("BA")
                                && et_list.get(i).getCod_dest().equals(fr_ba.get(x))) {
                            ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
                            if (!valori.isEmpty()) {
                                for (int z = 0; z < valori.size(); z++) {
                                    if (valori.get(z).getValuta().equals(valuta_locale)
                                            && valori.get(z).getSupporto().equals("01")) {
                                        importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
                                    } else {
                                        importo_val_est = importo_val_est + parseDoubleR(valori.get(z).getIp_total());
                                    }
                                    importo_spread = importo_spread + fd(valori.get(z).getIp_spread());
                                }
                            }
                        }
                    }
                    String[] val = { fr_ba.get(x), roundDoubleandFormat(importo_val_loc, 2),
                            roundDoubleandFormat(importo_val_est, 2), roundDoubleandFormat(importo_spread, 2) };
                    fr_ba_value.add(val);
                }
                for (int i = 0; i < fr_ba_value.size(); i++) {
                    String valore[] = fr_ba_value.get(i);

                    if (parseDoubleR(valore[1]) > 0) { // LOCALE
                        extBank("FBL", anno, data,
                                String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(valore[1], ".", ","),
                                formatAL(valore[0], bank, 1),
                                formatAL(valore[0], bank, 2),
                                // Utility.formatAL("BANC", contabilita_codici, 2),
                                valore[0],
                                this.codiceNegozi,
                                writer, true, null, null, null);
                        nreg++;
                    }
                    if (parseDoubleR(valore[2]) > 0) { // ESTERA

                        double spr_dbl = fd(valore[3]);
                        double net_dbl = fd(valore[2]) + spr_dbl;

                        String codice = formatNewSpread("FBE");

                        extBank(codice, anno, data,
                                String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(valore[2], ".", ","),
                                formatAL(valore[0], bank, 1),
                                formatAL(valore[0], bank, 2),
                                valore[0],
                                formatAL(formatNewSpread("AVEB"), contabilita_codici, 2),
                                writer,
                                false,
                                formatAL("SVVEB", contabilita_codici, 2), // SOSTITUIRE
                                StringUtils.replace(roundDoubleandFormat(net_dbl, 2), ".", ","),
                                StringUtils.replace(roundDoubleandFormat(spr_dbl, 2), ".", ","));
                        nreg++;
                    }
                } // BUY - CASH ADVANCE - SENZA COMMISSIONI
                ArrayList<String> b_ca_sc = new ArrayList<>();
                ArrayList<String[]> b_ca_sc_value_temp = new ArrayList<>();
                ArrayList<String[]> b_ca_sc_value = new ArrayList<>();
                for (int i = 0; i < ch_list.size(); i++) {
                    if (ch_list.get(i).getFiliale().equals(filiale.getCod())
                            && ch_list.get(i).getTipotr().equals("B")) {
                        ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
                        if (!valori.isEmpty()) {
                            for (int x = 0; x < valori.size(); x++) {
                                Ch_transaction_value val = valori.get(x);
                                if (val.getSupporto().equals("04")) {
                                    if (parseDoubleR(val.getTot_com())
                                            + parseDoubleR(val.getRoundvalue()) == 0.0D) {
                                        b_ca_sc.add(val.getPos());
                                        String[] tmp = { val.getPos(), val.getNet() };
                                        b_ca_sc_value_temp.add(tmp);
                                    }
                                }
                            }
                        }
                    }
                }
                removeDuplicatesAL(b_ca_sc);
                for (int x = 0; x < b_ca_sc.size(); x++) {
                    double importo_b_ca_sc = 0.00;
                    for (int i = 0; i < b_ca_sc_value_temp.size(); i++) {
                        if (b_ca_sc.get(x).equals(b_ca_sc_value_temp.get(i)[0])) {
                            importo_b_ca_sc = importo_b_ca_sc + parseDoubleR(b_ca_sc_value_temp.get(i)[1]);
                        }
                    }
                    String[] tmp = { b_ca_sc.get(x), roundDoubleandFormat(importo_b_ca_sc, 2) };
                    b_ca_sc_value.add(tmp);
                }
                for (int x = 0; x < b_ca_sc_value.size(); x++) {
                    String[] valore = b_ca_sc_value.get(x);

                    cashAdvance("CAD", anno, data,
                            String.valueOf(nreg),
                            formatAL(valore[0], bank, 2),
                            StringUtils.replace(valore[1], ".", ","), null, null,
                            filiale.getDe_branch(),
                            filiale.getCod(),
                            formatAL(formatNewSpread("AVEC"), contabilita_codici, 2),
                            // conto_RVE,
                            "", ck.getVatcode(), writer, scontrino_NORM);

                    nreg++;
                } // WESTERN UNION
                ArrayList<String[]> wu = new ArrayList<>();
                ArrayList<String[]> wu_value = new ArrayList<>();
                ArrayList<String[]> nc = new ArrayList<>();
                ArrayList<String[]> nc_value = new ArrayList<>();
                ArrayList<String[]> tisc = new ArrayList<>();
                ArrayList<String[]> tisc_value = new ArrayList<>();
                for (int i = 0; i < nc_list.size(); i++) {

                    if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                            && nc_list.get(i).getFg_tipo_transazione_nc().equals("1")) {

                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }
                        String[] va = { nc_list.get(i).getCausale_nc(), pos, nc_list.get(i).getGruppo_nc() };
                        wu.add(va);
                    } else if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                            && nc_list.get(i).getFg_tipo_transazione_nc().equals("21")) { // ticket senzacomm
                        String comm;
                        if (fd(nc_list.get(i).getCommissione()) > 0) {
                            comm = nc_list.get(i).getCommissione();
                        } else {
                            comm = nc_list.get(i).getTi_ticket_fee();
                        }
                        comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
                        if (parseDoubleR(comm) == 0.0D) {
                            String pos;
                            if (nc_list.get(i).getSupporto().equals("01")
                                    || nc_list.get(i).getSupporto().equals("...")) {
                                pos = "00";
                            } else if (nc_list.get(i).getSupporto().equals("08")) {
                                pos = nc_list.get(i).getPos();
                            } else {
                                pos = nc_list.get(i).getPos();
                            }
                            String[] va = { nc_list.get(i).getGruppo_nc(), pos };
                            tisc.add(va);
                        }
                    } else if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                            && (nc_list.get(i).getFg_tipo_transazione_nc().equals("2")
                                    || nc_list.get(i).getFg_tipo_transazione_nc().equals("4")
                                    || nc_list.get(i).getFg_tipo_transazione_nc().equals("5"))) {

                        NC_category nc0 = getNC_category(listcategory, nc_list.get(i).getGruppo_nc());
                        NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc(),
                                nc_list.get(i).getGruppo_nc());
                        if (nc0 != null && nc1 != null) {
                            if (!nc0.getConto_coge_01().trim().equals("")
                                    && !nc1.getDe_causale_nc().toUpperCase().contains("ACQUISTO")) {
                                if (nc0.getFg_tipo_transazione_nc().equals("2")) {
                                    if (nc0.getFg_registratore().equals("0")) {
                                        String pos;
                                        if (nc_list.get(i).getSupporto().equals("01")
                                                || nc_list.get(i).getSupporto().equals("...")) {
                                            pos = "00";
                                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                                            pos = nc_list.get(i).getPos();
                                        } else {
                                            pos = nc_list.get(i).getPos();
                                        }
                                        String[] va = { nc_list.get(i).getGruppo_nc(), pos, nc1.getCodice_integr() };
                                        nc.add(va);
                                    }
                                } else {
                                    if (!nc0.getConto_coge_01().trim().equals("")
                                            && nc0.getConto_coge_02().trim().equals("")) {
                                        String pos;
                                        if (nc_list.get(i).getSupporto().equals("01")
                                                || nc_list.get(i).getSupporto().equals("...")) {
                                            pos = "00";
                                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                                            pos = nc_list.get(i).getPos();
                                        } else {
                                            pos = nc_list.get(i).getPos();
                                        }
                                        String[] va = { nc_list.get(i).getGruppo_nc(), pos, nc1.getCodice_integr() };
                                        nc.add(va);
                                    }
                                }

                            }
                        }
                    }
                }
                removeDuplicatesALAr(wu);
                removeDuplicatesALAr(tisc);
                removeDuplicatesALAr(nc);
                for (int x = 0; x < tisc.size(); x++) {
                    double importo = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }
                        if (nc_list.get(i).getGruppo_nc().equals(tisc.get(x)[0])
                                && nc_list.get(i).getFiliale().equals(filiale.getCod())
                                && pos.equals(tisc.get(x)[1])) {
                            String comm;
                            if (fd(nc_list.get(i).getCommissione()) > 0) {
                                comm = nc_list.get(i).getCommissione();
                            } else {
                                comm = nc_list.get(i).getTi_ticket_fee();
                            }
                            comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
                            if (parseDoubleR(comm) == 0.0D) {
                                importo = importo + fd(nc_list.get(i).getTotal());
                            }
                        }
                    }
                    String[] va = { tisc.get(x)[0], tisc.get(x)[1], roundDoubleandFormat(importo, 2) };
                    tisc_value.add(va);
                }
                for (int x = 0; x < wu.size(); x++) {
                    double importo = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }
                        if (nc_list.get(i).getCausale_nc().equals(wu.get(x)[0])
                                && nc_list.get(i).getFiliale().equals(filiale.getCod()) && pos.equals(wu.get(x)[1])) {
                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                        }
                    }
                    String[] va = { wu.get(x)[0], wu.get(x)[1], roundDoubleandFormat(importo, 2), wu.get(x)[2] };
                    wu_value.add(va);
                }
                for (int x = 0; x < nc.size(); x++) {
                    double importo = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc(),
                                nc_list.get(i).getGruppo_nc());
                        String pos;
                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
                            pos = "00";
                        } else if (nc_list.get(i).getSupporto().equals("08")) {
                            pos = nc_list.get(i).getPos();
                        } else {
                            pos = nc_list.get(i).getPos();
                        }
                        if (nc_list.get(i).getGruppo_nc().equals(nc.get(x)[0])
                                && nc_list.get(i).getFiliale().equals(filiale.getCod()) && pos.equals(nc.get(x)[1])) {
                            if (nc1.getNc_de().equals("09") || nc1.getNc_de().equals("15")
                                    || nc1.getNc_de().equals("05") || nc1.getNc_de().equals("17")) {
                                importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                            } else if (nc1.getNc_de().equals("10") || nc1.getNc_de().equals("16")
                                    || nc1.getNc_de().equals("06") || nc1.getNc_de().equals("18")) {
                                importo = importo - parseDoubleR(nc_list.get(i).getTotal());
                            }
                        }
                    }
                    String[] va = { nc.get(x)[0], nc.get(x)[1], roundDoubleandFormat(importo, 2), nc.get(x)[2] };
                    nc_value.add(va);
                }
                for (int x = 0; x < nc_value.size(); x++) {
                    String valori[] = nc_value.get(x);
                    NC_category nc0 = getNC_category(listcategory, valori[0]);
                    if (nc0 != null) {

                        String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "").trim();
                        if (valori[1].equals("00")) { // CONTANTI
                            if (fd(valori[2]) > 0) {
                                noChange("NCV", anno, data,
                                        String.valueOf(nreg),
                                        filiale.getCod(),
                                        importo,
                                        nc0.getConto_coge_01(),
                                        // "470106", //nc0.getConto_coge_01(), SOSTITUIRE
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        null, null, writer, valori[3]);
                                nreg++;
                            } else if (fd(valori[2]) < 0) {
                                noChange("NCR", anno, data,
                                        String.valueOf(nreg),
                                        filiale.getCod(),
                                        importo,
                                        nc0.getConto_coge_01(),
                                        nc0.getDe_gruppo_nc() + " RETTIFICA - " + filiale.getDe_branch(),
                                        null, null, writer, valori[3]);
                                nreg++;
                            }
                        } else if (valori[1].equals("99")) {
                            if (fd(valori[2]) > 0) {
                                noChange("NCV", anno, data,
                                        String.valueOf(nreg),
                                        filiale.getCod(),
                                        importo,
                                        nc0.getConto_coge_01(),
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        "", writer, valori[3]);
                                nreg++;
                            }
                        } else {
                            if (fd(valori[2]) > 0) {
                                noChange("NCV", anno, data,
                                        String.valueOf(nreg),
                                        filiale.getCod(),
                                        importo,
                                        nc0.getConto_coge_01(), // nc0.getConto_coge_01(), SOSTITUIRE
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        formatAL(valori[1], bank, 2),
                                        "", writer, valori[3]);
                                nreg++;
                            }
                        }

                    }

                }
                for (int x = 0; x < tisc_value.size(); x++) {
                    String valori[] = tisc_value.get(x);
                    // String importo = StringUtils.replace(StringUtils.replace(valori[2], ".",
                    // ","), "-", "").trim();
                    // System.out.println("rc.soop.esolver.ESolver.FILEP1(a) " + valori[2]);
                    NC_category nc0 = getNC_category(listcategory, valori[0]);
                    if (nc0 != null) {
                        switch (valori[1]) {
                            case "00":
                                ticket("TSC", anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        valori[2],
                                        nc0.getConto_coge_01(),
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        null, null, writer);
                                nreg++;
                                break;
                            case "99":
                                ticket("TSC", anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        valori[2],
                                        nc0.getConto_coge_01(),
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                        // conto_RVE,
                                        "", writer);
                                nreg++;
                                break;
                            default:
                                ticket("TSC", anno, data, String.valueOf(nreg),
                                        filiale.getCod(),
                                        valori[2],
                                        nc0.getConto_coge_01(),
                                        nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
                                        formatAL(valori[1], bank, 2),
                                        "", writer);
                                nreg++;
                                break;
                        }
                    }

                }
                for (int x = 0; x < wu_value.size(); x++) {
                    String valori[] = wu_value.get(x);

                    NC_causal nc1 = getNC_causal(listcausal, valori[0], valori[3]);
                    if (nc1 != null) {
                        NC_category nc0 = getNC_category(listcategory, valori[3]);
                        if (nc0 != null) {
                            if (nc1.getFg_in_out().equals("2")) { // OUT - RECEIVE
                                westernUnion("WUR", anno, data,
                                        String.valueOf(nreg),
                                        filiale.getCod(),
                                        StringUtils.replace(valori[2], ".", ","),
                                        nc0.getConto_coge_01(),
                                        nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                        "", "", writer);
                                nreg++;
                            } else if (nc1.getFg_in_out().equals("1")) { // IN - SEND
                                if (valori[1].equals("00")) { // CONTANTI
                                    westernUnion("WUS", anno, data,
                                            String.valueOf(nreg),
                                            filiale.getCod(),
                                            StringUtils.replace(valori[2], ".", ","),
                                            nc0.getConto_coge_01(),
                                            nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                            null, null, writer);
                                    nreg++;
                                } else if (valori[1].equals("99")) { // BANK ACCOUNT
                                    westernUnion("WUS", anno, data,
                                            String.valueOf(nreg),
                                            filiale.getCod(),
                                            StringUtils.replace(valori[2], ".", ","),
                                            nc0.getConto_coge_01(),
                                            nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                            formatAL(formatNewSpread("VVEC"), contabilita_codici, 2),
                                            // conto_RVE,
                                            "", writer);
                                    nreg++;
                                } else { // POS
                                    westernUnion("WUS", anno, data,
                                            String.valueOf(nreg),
                                            filiale.getCod(),
                                            StringUtils.replace(valori[2], ".", ","),
                                            nc0.getConto_coge_01(),
                                            nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                            formatAL(valori[1], bank, 2),
                                            "", writer);
                                    nreg++;
                                }
                            }
                        }
                    }
                } // VAT REFUND
                ArrayList<String> vat_refund = new ArrayList<>();
                ArrayList<String[]> vat_refund_value = new ArrayList<>();
                for (int i = 0; i < nc_list.size(); i++) {
                    if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                            && nc_list.get(i).getFg_tipo_transazione_nc().equals("3")) {
                        vat_refund.add(nc_list.get(i).getGruppo_nc());
                    }
                }
                removeDuplicatesAL(vat_refund);
                for (int x = 0; x < vat_refund.size(); x++) {
                    double importo = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        if (nc_list.get(i).getGruppo_nc().equals(vat_refund.get(x))
                                && nc_list.get(i).getFiliale().equals(filiale.getCod())) {
                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                        }
                    }
                    String[] va = { vat_refund.get(x), roundDoubleandFormat(importo, 2) };
                    vat_refund_value.add(va);
                }
                for (int x = 0; x < vat_refund_value.size(); x++) {
                    String valori[] = vat_refund_value.get(x);
                    NC_category nc1 = getNC_category(listcategory, valori[0]);
                    if (nc1 != null) {
                        vatRefound("VAT", anno, data, String.valueOf(nreg),
                                filiale.getCod(),
                                StringUtils.replace(valori[1], ".", ","),
                                nc1.getConto_coge_01(),
                                nc1.getDe_gruppo_nc() + " " + filiale.getDe_branch(), writer);
                        nreg++;
                    }
                } // ANTICIPO DIP
                ArrayList<String[]> anticipo_dip = new ArrayList<>();
                ArrayList<String[]> anticipo_dip_value = new ArrayList<>();
                for (int i = 0; i < nc_list.size(); i++) {
                    if (nc_list.get(i).getFiliale().equals(filiale.getCod())
                            && nc_list.get(i).getFg_tipo_transazione_nc().equals("6")) {
                        String[] va = { nc_list.get(i).getUser(), nc_list.get(i).getCausale_nc(),
                                nc_list.get(i).getGruppo_nc() };
                        anticipo_dip.add(va);
                    }

                }
                removeDuplicatesALAr(anticipo_dip);
                for (int x = 0; x < anticipo_dip.size(); x++) {
                    double importo = 0.00;
                    for (int i = 0; i < nc_list.size(); i++) {
                        if (nc_list.get(i).getUser().equals(anticipo_dip.get(x)[0])
                                && nc_list.get(i).getCausale_nc().equals(anticipo_dip.get(x)[1])
                                && nc_list.get(i).getFiliale().equals(filiale.getCod())) {
                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
                        }
                    }
                    String[] va = { anticipo_dip.get(x)[0], anticipo_dip.get(x)[1], roundDoubleandFormat(importo, 2),
                            anticipo_dip.get(x)[2] };
                    anticipo_dip_value.add(va);
                }
                for (int x = 0; x < anticipo_dip_value.size(); x++) {
                    String valori[] = anticipo_dip_value.get(x);
                    NC_causal nc1 = getNC_causal(listcausal, valori[1], valori[3]);
                    if (nc1 != null) {
                        NC_category nc0 = getNC_category(listcategory, valori[3]);
                        Users us = get_user(valori[0], listusers);
                        if (nc1.getFg_in_out().equals("2")) { // OUT - AND
                            anticipoDipendenti("AND",
                                    anno, data,
                                    String.valueOf(nreg),
                                    us.getConto(),
                                    StringUtils.replace(valori[2], ".", ","),
                                    us.getCod() + " " + nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                    filiale.getCod(), writer, nc0.getConto_coge_01());
                            nreg++;
                        } else if (nc1.getFg_in_out().equals("1")) { // IN - ADR
                            anticipoDipendenti("ADR",
                                    anno, data,
                                    String.valueOf(nreg),
                                    us.getConto(),
                                    StringUtils.replace(valori[2], ".", ","),
                                    us.getCod() + " " + nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
                                    filiale.getCod(), writer, nc0.getConto_coge_01());
                            nreg++;
                        }
                    }
                }
            }
            if (nreg > 1) {
                return f;
            }
            Files.delete(f.toPath());
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }
    //

    // public static void main(String[] args) {
    // DateTime dt = new DateTime(2024,
    // 4, 14, 0, 0);
    // rilascia(dt);
    // }

    public static String sanitizePath(String inputPath) {
        try {
            // Converti il percorso in un oggetto Path
            Path path = Paths.get(inputPath);

            // Risolve il percorso per rimuovere eventuali elementi indesiderati (es. '../')
            Path resolvedPath = path.normalize();

            // Assicurati che il percorso sia assoluto per evitare riferimenti relativi
            Path absolutePath = resolvedPath.toAbsolutePath();

            // Assicurati che il percorso sia sicuro e non contenga caratteri non validi
            if (!absolutePath.startsWith(FileSystems.getDefault().getPath("/"))) {
                throw new SecurityException("Il percorso non è assoluto o è al di fuori del file system radice.");
            }

            // Restituisci il percorso normalizzato come stringa
            return absolutePath.toString();
        } catch (InvalidPathException | SecurityException e) {
            // Gestisci eventuali eccezioni
            e.printStackTrace();
            return null; // Oppure puoi restituire una stringa vuota o un valore di default
        }
    }

    public static void rilascia(DateTime dt) {
        if (dt == null) {
            Db_Master db = new Db_Master();
            dt = db.getNowDT().minusDays(1);
            db.closeDB();
        }

        String from = sanitizeInput(dt.toString(patternnormdate_filter));
        String anno = sanitizeInput(dt.toString(patternyear));
        String data1 = sanitizeInput(dt.toString(patternsql));

        log.warning("START...");
        String path = sanitizeFilePath(pathin + data1 + File.separator);

        try {
            new File(path).mkdirs();
            log.log(Level.WARNING, "DIRECTORY {0} CREATA", path);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "DIRECTORY {0} ERRORE", path);
        }

        Db_Master db1 = new Db_Master();
        ArrayList<Branch> branch = db1.list_branch_enabled();
        db1.closeDB();

        Db_Master db2 = new Db_Master();
        ArrayList<Ch_transaction> list_esolver_ch = db2.list_esolver_ch(data1, branch, null);
        ArrayList<Ch_transaction_refund> list_esolver_refund = db2.list_esolver_refund(data1, branch, null);
        db2.closeDB();

        Db_Master db4 = new Db_Master();
        ArrayList<NC_transaction> list_esolver_nc = db4.list_esolver_nc(data1, branch, null);
        db4.closeDB();

        Db_Master db5 = new Db_Master();
        ArrayList<ET_change> list_esolver_et = db5.list_esolver_et(data1, branch, null);
        ArrayList<Openclose> list_esolver_ocerr = db5.list_esolver_ocerr(data1, branch, null);
        db5.closeDB();

        Db_Master db7 = new Db_Master();
        ArrayList<NC_causal> listcausal = db7.list_nc_causal_enabled();
        ArrayList<NC_category> listcategory = db7.list_nc_category_enabled();
        ArrayList<Users> listusers = db7.list_all_users();
        db7.closeDB();

        Db_Master db10 = new Db_Master();
        ArrayList<String[]> fatt_note = db10.get_fatt_note(data1, branch, null);
        ArrayList<String[]> contabilita_codici = db10.contabilita();
        ArrayList<String[]> bank = db10.list_bank();
        db10.closeDB();

        Db_Master db13 = new Db_Master();
        ArrayList<String[]> country = db13.country();
        String valuta_locale = db13.get_local_currency()[0];
        boolean dividi = db13.get_national_office().getChangetype().equals("/");
        ArrayList<VATcode> listvat = db13.li_vat(null);
        CustomerKind ckres = db13.get_customerKind("002");
        db13.closeDB();

        boolean release = true;

        ESolver es = new ESolver();
        for (int i = 0; i < branch.size(); i++) {
            Branch b1 = branch.get(i);

            File base64_1 = es.FILEP1(path, from, anno, list_esolver_ch, list_esolver_nc, list_esolver_refund,
                    list_esolver_et, list_esolver_ocerr, listcategory, listcausal, listusers, b1, contabilita_codici,
                    bank, valuta_locale, branch, dividi, ckres);

            File base64_2 = es.FILEP2(path, from, anno, list_esolver_ch, list_esolver_nc, listcategory, listcausal, b1,
                    contabilita_codici, bank, valuta_locale, branch, listvat, ckres);

            File base64_3 = es.FILEP3(path, from, anno, fatt_note, b1, contabilita_codici,
                    bank, country, valuta_locale, branch);

            File base64_4 = es.FILEP4(path, from, anno, fatt_note, b1, contabilita_codici,
                    bank, country, valuta_locale, branch);

            File base64_5 = es.FILEP5(path, from, anno, fatt_note, b1, contabilita_codici,
                    bank, country, valuta_locale, branch);

            log.log(Level.WARNING, "START BRANCH CODE {0}", sanitizeInput(b1.getCod()));

            if (release) {
                try {
                    rilasciaFileEsolver(base64_1, base64_2, base64_3, base64_4, base64_5);
                } catch (Exception ex) {
                    log.severe(estraiEccezione(ex));
                }
            } else {
                log.log(Level.INFO, "{0} RILASCIATO", base64_1.getPath());
                log.log(Level.INFO, "{0} RILASCIATO", base64_2.getPath());
                log.log(Level.INFO, "{0} RILASCIATO", base64_3.getPath());
                log.log(Level.INFO, "{0} RILASCIATO", base64_4.getPath());
                log.log(Level.INFO, "{0} RILASCIATO", base64_5.getPath());
            }
            log.log(Level.WARNING, "END BRANCH CODE {0}", sanitizeInput(b1.getCod()));
        }

        log.log(Level.WARNING, "END {0}", from);
    }

    private static String formatNewSpread(String ing) {
        if (newSpread) {
            if (ing.equals("VVEB")
                    || ing.equals("VVEC")
                    || ing.equals("AVEB")
                    || ing.equals("AVEC")) {
                return "RVE";
            } else if (ing.equals("FBS")
                    || ing.equals("FBB")
                    || ing.equals("FBE")
                    || ing.equals("SEL")
                    || ing.equals("BUY")
                    || ing.equals("BSC")
                    || ing.equals("TBE")
                    || ing.equals("SSC")) {
                return "N_" + ing;
            }
        }
        return ing;
    }

    public static String sanitizeInput(String input) {
        if (input == null) {
            return null;
        }
        input = input.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
        input = input.replaceAll("[\r\n]", "");
        return StringEscapeUtils.escapeHtml4(input);
    }

    public static boolean rilasciaFileEsolver(File... files) {
        boolean success = true;
        for (File file : files) {
            if (file.exists()) {
                success = file.delete();
            } else {
                System.out.println("Il file " + file.getName() + " non esiste.");
                success = false;
            }
        }
        return success;
    }

    public static String sanitizeFilePath(String filePath) {
        filePath = filePath.replaceAll("\\.{2,}", ".");
        filePath = filePath.replaceAll("[^a-zA-Z0-9_\\-\\.\\\\/]", "");
        Path sanitizedPath2 = Paths.get(filePath).normalize();
        return sanitizedPath2.toString();
    }

    public static String sanitizePath2(String input) {
        input = input.replaceAll("\\.\\./", "")
                     .replaceAll("~", "") 
                     .replaceAll("\\\\", "/");

        input = input.replaceAll("[^a-zA-Z0-9_./-]", "");
        Path sanitizedPath = Paths.get(input).normalize();
        
        return sanitizedPath.toString();
    }

}

class Tipodoc {

    String id;

    public Tipodoc() {
    }

    public Tipodoc(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
