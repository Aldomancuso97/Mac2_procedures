/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.riallinea;

import java.util.ArrayList;

/**
 *
 * @author srotella
 */
public class OfficeStockPrice_value {
    
    
    
    String id_filiale, de_filiale,currency,decurrency,supporto,qta,medioacq,controvalore,gruppo;
    String localcurrency;
    String data;
    ArrayList dati;

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getGruppo() {
        return gruppo;
    }

    public void setGruppo(String gruppo) {
        this.gruppo = gruppo;
    }

    

    public String getLocalcurrency() {
        return localcurrency;
    }

    public void setLocalcurrency(String localcurrency) {
        this.localcurrency = localcurrency;
    }
    
    

    public String getId_filiale() {
        return id_filiale;
    }

    public void setId_filiale(String id_filiale) {
        this.id_filiale = id_filiale;
    }

    public String getDe_filiale() {
        return de_filiale;
    }

    public void setDe_filiale(String de_filiale) {
        this.de_filiale = de_filiale;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getDecurrency() {
        return decurrency;
    }

    public void setDecurrency(String decurrency) {
        this.decurrency = decurrency;
    }

    public String getSupporto() {
        return supporto;
    }

    public void setSupporto(String supporto) {
        this.supporto = supporto;
    }

    public String getQta() {
        return (qta);
    }

    public String getQtaSenzaFormattazione() {
        return (qta);
    }
    
    public void setQta(String qta) {
        this.qta = qta;
    }

    public String getMedioacq() {
        return (medioacq);
    }

    public void setMedioacq(String medioacq) {
        this.medioacq = medioacq;
    }

    public String getControvalore() {
        return (controvalore);
    }
    public String getControvaloreSenzaFormattazione() {
        return (controvalore.replace(",", "."));
    }

    public void setControvalore(String controvalore) {
        this.controvalore = controvalore;
    }

    public ArrayList getDati() {
        return dati;
    }

    public void setDati(ArrayList dati) {
        this.dati = dati;
    }

    
    
}
