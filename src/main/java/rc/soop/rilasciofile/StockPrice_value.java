/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.ArrayList;

/**
 *
 * @author srotella
 */
public class StockPrice_value {

    ArrayList<StockPrice_value> dati;

    double equivalent,delta;

    public double getEquivalent() {
        return equivalent;
    }

    public void setEquivalent(double equivalent) {
        this.equivalent = equivalent;
    }

    public double getDelta() {
        return delta;
    }

    public void setDelta(double delta) {
        this.delta = delta;
    }
    
    
    

    /**
     *
     * @return
     */
    public ArrayList<StockPrice_value> getDati() {
        return dati;
    }

    /**
     *
     * @param dati
     */
    public void setDati(ArrayList<StockPrice_value> dati) {
        this.dati = dati;
    }

    String id_filiale, de_filiale, currency, de_currency, bcanconote1, supportocod, banconote2, cambio, quantita, controvalore, supportodesc, supportovalue;

    /**
     *
     * @return
     */
    public String getId_filiale() {
        return id_filiale;
    }

    /**
     *
     * @param id_filiale
     */
    public void setId_filiale(String id_filiale) {
        this.id_filiale = id_filiale;
    }

    /**
     *
     * @return
     */
    public String getDe_filiale() {
        return de_filiale;
    }

    /**
     *
     * @param de_filiale
     */
    public void setDe_filiale(String de_filiale) {
        this.de_filiale = de_filiale;
    }

    /**
     *
     * @return
     */
    public String getCurrency() {
        return currency;
    }

    /**
     *
     * @param currency
     */
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    /**
     *
     * @return
     */
    public String getDe_currency() {
        return de_currency;
    }

    /**
     *
     * @param de_currency
     */
    public void setDe_currency(String de_currency) {
        this.de_currency = de_currency;
    }

    /**
     *
     * @return
     */
    public String getBcanconote1() {
        return bcanconote1;
    }

    /**
     *
     * @param bcanconote1
     */
    public void setBcanconote1(String bcanconote1) {
        this.bcanconote1 = bcanconote1;
    }

    /**
     *
     * @return
     */
    public String getSupportocod() {
        return supportocod;
    }

    /**
     *
     * @param supportocod
     */
    public void setSupportocod(String supportocod) {
        this.supportocod = supportocod;
    }

    /**
     *
     * @return
     */
    public String getSupportodesc() {
        return supportodesc;
    }

    /**
     *
     * @param supportodesc
     */
    public void setSupportodesc(String supportodesc) {
        this.supportodesc = supportodesc;
    }

    /**
     *
     * @return
     */
    public String getSupportovalue() {
        return supportovalue;
    }

    /**
     *
     * @param supportovalue
     */
    public void setSupportovalue(String supportovalue) {
        this.supportovalue = supportovalue;
    }

    /**
     *
     * @return
     */
    public String getQuantitaSenzaFormattazione() {
        return quantita;
    }

    /**
     *
     * @return
     */
    public String getControvaloreSenzaFormattazione() {
        return controvalore;
    }

    /**
     *
     * @return
     */
    public String getBanconote2() {
        return banconote2;
    }

    /**
     *
     * @param banconote2
     */
    public void setBanconote2(String banconote2) {
        this.banconote2 = banconote2;
    }

    /**
     *
     * @return
     */
    public String getCambio() {
        return cambio;
    }

    /**
     *
     * @param cambio
     */
    public void setCambio(String cambio) {
        this.cambio = cambio;
    }

    /**
     *
     * @return
     */
    public String getQuantita() {
        return (quantita);
    }

    /**
     *
     * @param quantita
     */
    public void setQuantita(String quantita) {
        this.quantita = (quantita);
    }

    /**
     *
     * @return
     */
    public String getControvalore() {
        return (controvalore);
    }

    /**
     *
     * @param controvalore
     */
    public void setControvalore(String controvalore) {
        this.controvalore = controvalore;
    }

    //generici
    String dateactual, bceactual;

    //valori
    String HistoricalBCE, HistoricalBCEEquivalent, DeltaEquivalent;

    public String getDateactual() {
        return dateactual;
    }

    public void setDateactual(String dateactual) {
        this.dateactual = dateactual;
    }

    public String getBceactual() {
        return bceactual;
    }

    public void setBceactual(String bceactual) {
        this.bceactual = bceactual;
    }

    public String getHistoricalBCE() {
        return HistoricalBCE;
    }

    public void setHistoricalBCE(String HistoricalBCE) {
        this.HistoricalBCE = HistoricalBCE;
    }

    public String getHistoricalBCEEquivalent() {
        return HistoricalBCEEquivalent;
    }

    public void setHistoricalBCEEquivalent(String HistoricalBCEEquivalent) {
        this.HistoricalBCEEquivalent = HistoricalBCEEquivalent;
    }

    public String getDeltaEquivalent() {
        return DeltaEquivalent;
    }

    public void setDeltaEquivalent(String DeltaEquivalent) {
        this.DeltaEquivalent = DeltaEquivalent;
    }

}
