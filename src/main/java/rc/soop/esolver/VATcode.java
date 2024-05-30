/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

/**
 *
 * @author rcosco
 */
public class VATcode {
    String id,codice,descrizione,aliquota,fg_annullato,dt;

    public VATcode() {
    }

    public VATcode(String id, String codice, String descrizione, String aliquota, String fg_annullato, String dt) {
        this.id = id;
        this.codice = codice;
        this.descrizione = descrizione;
        this.aliquota = aliquota;
        this.fg_annullato = fg_annullato;
        this.dt = dt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getDescrizione() {
        return descrizione;
    }

    public void setDescrizione(String descrizione) {
        this.descrizione = descrizione;
    }

    public String getAliquota() {
        return aliquota;
    }

    public void setAliquota(String aliquota) {
        this.aliquota = aliquota;
    }

    public String getFg_annullato() {
        return fg_annullato;
    }

    public void setFg_annullato(String fg_annullato) {
        this.fg_annullato = fg_annullato;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }
    
    

}
