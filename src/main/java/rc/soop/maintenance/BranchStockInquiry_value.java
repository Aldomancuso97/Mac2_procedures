/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.util.ArrayList;

/**
 *
 * @author srotella
 */
public class BranchStockInquiry_value {
    
    
    String id_filiale, de_filiale,currency,notes,eurotravel,travel,credit,personal,creditcop,bancomatcop;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getNotes() {
        return (notes);
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public String getEurotravel() {
        return (eurotravel);
    }

    public void setEurotravel(String eurotravel) {
        this.eurotravel = eurotravel;
    }

    public String getTravel() {
        return (travel);
    }

    public void setTravel(String travel) {
        this.travel = travel;
    }

    public String getCredit() {
        return (credit);
    }

    public void setCredit(String credic) {
        this.credit = credic;
    }

    public String getPersonal() {
        return (personal);
    }

    public void setPersonal(String personal) {
        this.personal = personal;
    }

    public String getCreditcop() {
        return (creditcop);
    }

    public void setCreditcop(String creditcop) {
        this.creditcop = creditcop;
    }

    public String getBancomatcop() {
        return (bancomatcop);
    }

    public void setBancomatcop(String bancomatcop) {
        this.bancomatcop = bancomatcop;
    }
    ArrayList dati;

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

    public ArrayList getDati() {
        return dati;
    }

    public void setDati(ArrayList dati) {
        this.dati = dati;
    }
    
    
}
