/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 *
 * @author agodino
 */
public class Booking {

    String cod, currency, quantity, rate, comm, euro, filiale, note, cl_email, cl_nome, cl_cognome, 
            cl_telefono, cl_tipologia, canale, agevolazioni, serviziagg, stato, cod_tr, stato_crm;
    String dt_ritiro, dt_tr, timestamp;

      String pan;

    public Booking() {
    }

    public Booking(String cod, String currency, String quantity, String rate, String comm, String euro, String filiale, String note, String cl_email, String cl_nome,
            String cl_cognome, String cl_telefono, String cl_tipologia, String canale, String agevolazioni, String serviziagg, String stato, String cod_tr, String dt_ritiro,
            String dt_tr, String timestamp, String stato_crm) {
        this.cod = cod;
        this.currency = currency;
        this.quantity = quantity;
        this.rate = rate;
        this.comm = comm;
        this.euro = euro;
        this.filiale = filiale;
        this.note = note;
        this.cl_email = cl_email;
        this.cl_nome = cl_nome;
        this.cl_cognome = cl_cognome;
        this.cl_telefono = cl_telefono;
        this.cl_tipologia = cl_tipologia;

        this.canale = canale;
        switch (canale) {
            case "1":
            case "01":
                this.canale = "Website";
                break;
            case "4":
            case "04":
                this.canale = "Chebanca";
            case "5":
            case "05":
                this.canale = "Green Number";
                break;
            case "6":
            case "06":
                this.canale = "Web - Payment on site";
                break;
            case "7":
            case "07":
                this.canale = "Welcome Travel";
                break;
            default:
                break;
        }

        this.agevolazioni = agevolazioni;
        this.serviziagg = serviziagg;
        this.stato = stato;
        this.cod_tr = cod_tr;
        this.dt_ritiro = dt_ritiro;
        this.dt_tr = dt_tr;
        this.timestamp = timestamp;
        this.stato_crm = stato_crm;
    }

    public String getPan() {
        return pan;
    }

    public void setPan(String pan) {
        this.pan = pan;
    }
    
    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public String getEuro() {
        return euro;
    }

    public void setEuro(String euro) {
        this.euro = euro;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getCl_email() {
        return cl_email;
    }

    public void setCl_email(String cl_email) {
        this.cl_email = cl_email;
    }

    public String getCl_nome() {
        return cl_nome;
    }

    public void setCl_nome(String cl_nome) {
        this.cl_nome = cl_nome;
    }

    public String getCl_cognome() {
        return cl_cognome;
    }

    public void setCl_cognome(String cl_cognome) {
        this.cl_cognome = cl_cognome;
    }

    public String getCl_telefono() {
        return cl_telefono;
    }

    public void setCl_telefono(String cl_telefono) {
        this.cl_telefono = cl_telefono;
    }

    public String getCl_tipologia() {
        return cl_tipologia;
    }

    public void setCl_tipologia(String cl_tipologia) {
        this.cl_tipologia = cl_tipologia;
    }

    public String getCanale() {
        return canale;
    }

    public void setCanale(String canale) {
        this.canale = canale;
    }

    public String getAgevolazioni() {
        return agevolazioni;
    }

    public void setAgevolazioni(String agevolazioni) {
        this.agevolazioni = agevolazioni;
    }

    public String getServiziagg() {
        return serviziagg;
    }

    public void setServiziagg(String serviziagg) {
        this.serviziagg = serviziagg;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    public String getCod_tr() {
        return cod_tr;
    }

    public void setCod_tr(String cod_tr) {
        this.cod_tr = cod_tr;
    }

    public String getDt_ritiro() {
        return dt_ritiro;
    }

    public String getDt_tr() {
        return dt_tr;
    }

    public void setDt_tr(String dt_tr) {
        this.dt_tr = dt_tr;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getStato_crm() {
        return stato_crm;
    }

    public void setStato_crm(String stato_crm) {
        this.stato_crm = stato_crm;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
