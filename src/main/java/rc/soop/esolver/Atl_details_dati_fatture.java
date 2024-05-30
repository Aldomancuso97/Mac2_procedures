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
public class Atl_details_dati_fatture {

    String cod, numriga, category, casual, segnoreg, tipoconto, contoreg, desc, ivareg, datadoc, numdoc, modpaga, importo;

    public Atl_details_dati_fatture(String cod, String numriga, String category, String casual, String segnoreg, String tipoconto, String contoreg, String desc, String ivareg, String datadoc, String numdoc, String modpaga, String importo) {
        this.cod = cod;
        this.numriga = numriga;
        this.category = category;
        this.casual = casual;
        this.segnoreg = segnoreg;
        this.tipoconto = tipoconto;
        this.contoreg = contoreg;
        this.desc = desc;
        this.ivareg = ivareg;
        this.datadoc = datadoc;
        this.numdoc = numdoc;
        this.modpaga = modpaga;
        this.importo = importo;
    }

    public String controlli_details() {
        return "OK";
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getNumriga() {
        return numriga;
    }

    public void setNumriga(String numriga) {
        this.numriga = numriga;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCasual() {
        return casual;
    }

    public void setCasual(String casual) {
        this.casual = casual;
    }

    public String getSegnoreg() {
        return segnoreg;
    }

    public void setSegnoreg(String segnoreg) {
        this.segnoreg = segnoreg;
    }

    public String getTipoconto() {
        return tipoconto;
    }

    public void setTipoconto(String tipoconto) {
        this.tipoconto = tipoconto;
    }

    public String getContoreg() {
        return contoreg;
    }

    public void setContoreg(String contoreg) {
        this.contoreg = contoreg;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getIvareg() {
        return ivareg;
    }

    public void setIvareg(String ivareg) {
        this.ivareg = ivareg;
    }

    public String getDatadoc() {
        return datadoc;
    }

    public void setDatadoc(String datadoc) {
        this.datadoc = datadoc;
    }

    public String getNumdoc() {
        return numdoc;
    }

    public void setNumdoc(String numdoc) {
        this.numdoc = numdoc;
    }

    public String getModpaga() {
        return modpaga;
    }

    public void setModpaga(String modpaga) {
        this.modpaga = modpaga;
    }

    public String getImporto() {
        return importo;
    }

    public void setImporto(String importo) {
        this.importo = importo;
    }

}
