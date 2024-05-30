/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rcosco
 */
public class Openclose_Error_value_stock {
    List<Openclose_Error_value> dati = new ArrayList();
    String operazione = "";
    
    String id_filiale, de_filiale,user;
    
    public Openclose_Error_value_stock(ArrayList da, String operazione) {
        this.dati = da;
        this.operazione = operazione;
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
    
    public String getOperazione() {
        return operazione;
    }

    public void setOperazione(String operazione) {
        this.operazione = operazione;
    }

    public List<Openclose_Error_value> getDati() {
        return dati;
    }

    public void setDati(List<Openclose_Error_value> dati) {
        this.dati = dati;
    }

}
