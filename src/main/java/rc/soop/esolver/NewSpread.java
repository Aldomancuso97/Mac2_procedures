/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import static rc.soop.esolver.ESolver.separator;
import static rc.soop.esolver.ESolver.tag_GEN;
import static rc.soop.esolver.ESolver.tag_RIG;
import java.io.PrintWriter;

/**
 *
 * @author rcosco
 */
public class NewSpread {

    @SuppressWarnings("unused")
    private void extBank(String type, String anno, String data, String n_reg, String codice_filiale,
            String importo,
            String descrizione_filiale, String cod_banca_lungo, String cod_banca_corto,
            String conto_esolver, PrintWriter writer,
            String codice_spread, String net, String spread) {
        if (type.equals("N_FBE")) {
            writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg + separator + conto_esolver + separator + separator + separator + importo + separator + separator + codice_filiale + separator + "From bank - " + descrizione_filiale);
            writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread From bank - " + descrizione_filiale);
            writer.println(tag_GEN + separator + "FBE" + separator + anno + separator + data + separator + n_reg + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + separator + importo + separator + codice_filiale + separator + "From bank - " + descrizione_filiale);
        } else if (type.equals("N_TBE")) {
            writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + importo + separator + separator + codice_filiale + separator + "To bank - " + descrizione_filiale);
            writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread To bank - " + descrizione_filiale);
            writer.println(tag_GEN + separator + "TBE" + separator + anno + separator + data + separator + n_reg + separator + conto_esolver + separator + separator + separator + separator + net + separator + codice_filiale + separator + "To bank - " + descrizione_filiale);
        }
    }
    
    
    
    @SuppressWarnings("unused")
    private void sell(String type, String anno, String data, String n_reg, String codice_filiale, String importo, String importo_comm,
            String descrizione_filiale, String codice_esolver1, String codice_esolver2, String cod_banca_lungo, String cod_banca_corto, String codiceiva,
            PrintWriter writer, String newimp, String codscontrino, String codice_spread, String net, String spread, String codiceNegozi) {
        if (type.equals("N_SEL")) {
            if (cod_banca_lungo != null) {
                writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva + separator + importo_comm + separator + separator + codice_filiale + separator + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator + separator + separator + separator);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_spread + separator + separator + separator + spread + separator + "Spread Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_esolver2 + separator + separator + separator + net + separator + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + cod_banca_lungo + separator + cod_banca_corto + separator + newimp + separator + separator + "Incasso vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
            } else {
                writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva + separator + importo_comm + separator + separator + codice_filiale + separator + "Corrispettivi " + descrizione_filiale + separator + separator + separator + separator + separator + separator + separator);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_spread + separator + separator + separator + spread + separator + "Spread Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_esolver2 + separator + separator + separator + net + separator + "Vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
                writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codiceNegozi + separator + codice_filiale + separator + newimp + separator + separator + "Incasso vendita valuta " + descrizione_filiale + separator + separator + codice_filiale);
            }
        } else if (type.equals("N_SSC")) {
            writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg + separator + cod_banca_lungo + separator + cod_banca_corto + separator + separator + newimp + separator + separator + codice_filiale + separator + "Incasso vendita valuta " + descrizione_filiale);
            writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread su Vendita valuta " + descrizione_filiale);
            writer.println(tag_GEN + separator + "SSC" + separator + anno + separator + data + separator + n_reg + separator + codice_esolver2 + separator + separator + separator + separator + net + separator + codice_filiale + separator + "Vendita valuta " + descrizione_filiale);
        }
    }
    
     @SuppressWarnings("unused")
    private void buy(String type, String anno, String data, String n_reg, String codice_filiale, String importo,
            String codice_esolver1, String importo_comm, String codice_esolver2, String totale, String codiceiva,
            PrintWriter writer, String codscontrino,String codice_spread, String net, String spread,String codiceNegozi) {
        //registrazione giornaliera
        if (type.equals("N_BUY")) {
            writer.println(tag_RIG + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + codice_esolver1 + separator + importo_comm + separator + codiceiva + separator + importo_comm + separator + separator + codice_filiale + separator + separator + separator + separator + separator + separator + separator + separator);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codiceNegozi + separator + codice_filiale + separator + separator + importo + separator + "Pagamento acquisto valuta" + separator + separator + codice_filiale);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_spread + separator + separator + separator + spread + separator + "Spread acquisto valuta" + separator + separator + codice_filiale);
            writer.println(tag_GEN + separator + codscontrino + separator + anno + separator + data + separator + n_reg + separator + separator + separator + separator + separator + separator + separator + separator + codice_esolver2 + separator + separator + net + separator + separator + "Costo acquisto valuta" + separator + separator + codice_filiale);
        } else if (type.equals("N_BSC")) {
            writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg + separator + codice_esolver2 + separator + separator + separator + totale + separator + separator + codice_filiale + separator + "Costo acquisto valuta");
            writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread su acquisto valuta");
            writer.println(tag_GEN + separator + "BSC" + separator + anno + separator + data + separator + n_reg + separator + codice_esolver2 + separator + separator + separator + separator + net + separator + codice_filiale + separator + "Acquisto valuta");
        }
    } 
    
    @SuppressWarnings("unused")
    private void tofrombank_fattura(String type, String anno, String data, String n_reg, String conto_esolver,
            String contocassa, String codicecassa, String importo, String codice_filiale, String numfat,
            String codice_cliente, String startdescr, PrintWriter writer, String codice_spread, String net, String spread) {
        if (type.equals("N_FBB")) { //fattura bank sell 20
            writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg + separator + contocassa + separator + codice_filiale + separator + separator + importo + separator + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread " + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + "FBB" + separator + anno + separator + data + separator + n_reg + separator + conto_esolver + separator + separator + separator + separator + net + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        } else if (type.equals("N_FBS")) { //fattura bank sell 20
            writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg + separator + contocassa + separator + codice_filiale + separator + separator + importo + separator + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg + separator + codice_spread + separator + separator + separator + separator + spread + separator + codice_filiale + separator + "Spread " + startdescr + " a Ft. N. " + numfat);
            writer.println(tag_GEN + separator + "FBS" + separator + anno + separator + data + separator + n_reg + separator + conto_esolver + separator + separator + separator + separator + net + separator + codice_filiale + separator + startdescr + " a Ft. N. " + numfat);
        }
    }

}
