/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import java.util.List;

/**
 *
 * @author rcosco
 */
public class Atl_dati_fatture {

    String cod, Idreg, branchid, sezionale, datereg, numreg, tipomov, stato, clientcode,dateoper;

    List<Atl_details_dati_fatture> details;
    List<Atl_detailsiva_dati_fatture> detailsiva;

    public Atl_dati_fatture(String cod, String Idreg, String branchid, String sezionale, String datereg, String numreg, String tipomov, String stato, String dateoper, 
            String clientcode,
            List<Atl_details_dati_fatture> details,
            List<Atl_detailsiva_dati_fatture> detailsiva) {
        this.cod = cod;
        this.Idreg = Idreg;
        this.branchid = branchid;
        this.sezionale = sezionale;
        this.datereg = datereg;
        this.numreg = numreg;
        this.tipomov = tipomov;
        this.details = details;
        this.detailsiva = detailsiva;
        this.stato = stato;
        this.dateoper = dateoper;
        this.clientcode = clientcode;
    }

    public String getClientcode() {
        return clientcode;
    }

    public void setClientcode(String clientcode) {
        this.clientcode = clientcode;
    }
    
    public String controlli() {
        return "OK";
    }
    
    
    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    public String getDateoper() {
        return dateoper;
    }

    public void setDateoper(String dateoper) {
        this.dateoper = dateoper;
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getIdreg() {
        return Idreg;
    }

    public void setIdreg(String Idreg) {
        this.Idreg = Idreg;
    }

    public String getBranchid() {
        return branchid;
    }

    public void setBranchid(String branchid) {
        this.branchid = branchid;
    }

    public String getSezionale() {
        return sezionale;
    }

    public void setSezionale(String sezionale) {
        this.sezionale = sezionale;
    }

    public String getDatereg() {
        return datereg;
    }

    public void setDatereg(String datereg) {
        this.datereg = datereg;
    }

    public String getNumreg() {
        return numreg;
    }

    public void setNumreg(String numreg) {
        this.numreg = numreg;
    }

    public String getTipomov() {
        return tipomov;
    }

    public void setTipomov(String tipomov) {
        this.tipomov = tipomov;
    }

    public List<Atl_details_dati_fatture> getDetails() {
        return details;
    }

    public void setDetails(List<Atl_details_dati_fatture> details) {
        this.details = details;
    }

    public List<Atl_detailsiva_dati_fatture> getDetailsiva() {
        return detailsiva;
    }

    public void setDetailsiva(List<Atl_detailsiva_dati_fatture> detailsiva) {
        this.detailsiva = detailsiva;
    }

}
