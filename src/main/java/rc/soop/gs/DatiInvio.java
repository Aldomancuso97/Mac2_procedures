/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.gs;

/**
 *
 * @author rcosco
 */
public class DatiInvio {

    String codiceTenant, codiceContratto, dataVendite, categoria, vendutoNetto, vendutoLordo, numScontrini;

    public DatiInvio(String codiceTenant, String codiceContratto, String dataVendite, String categoria, String vendutoNetto, String vendutoLordo, String numScontrini) {
        this.codiceTenant = codiceTenant;
        this.codiceContratto = codiceContratto;
        this.dataVendite = dataVendite;
        this.categoria = categoria;
        this.vendutoNetto = vendutoNetto;
        this.vendutoLordo = vendutoLordo;
        this.numScontrini = numScontrini;
    }

    public DatiInvio() {
    }
    
    public String getCodiceTenant() {
        return codiceTenant;
    }

    public void setCodiceTenant(String codiceTenant) {
        this.codiceTenant = codiceTenant;
    }

    public String getCodiceContratto() {
        return codiceContratto;
    }

    public void setCodiceContratto(String codiceContratto) {
        this.codiceContratto = codiceContratto;
    }

    public String getDataVendite() {
        return dataVendite;
    }

    public void setDataVendite(String dataVendite) {
        this.dataVendite = dataVendite;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    public String getVendutoNetto() {
        return vendutoNetto;
    }

    public void setVendutoNetto(String vendutoNetto) {
        this.vendutoNetto = vendutoNetto;
    }

    public String getVendutoLordo() {
        return vendutoLordo;
    }

    public void setVendutoLordo(String vendutoLordo) {
        this.vendutoLordo = vendutoLordo;
    }

    public String getNumScontrini() {
        return numScontrini;
    }

    public void setNumScontrini(String numScontrini) {
        this.numScontrini = numScontrini;
    }
    
}
