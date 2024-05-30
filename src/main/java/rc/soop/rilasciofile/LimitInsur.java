/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

/**
 *
 * @author rcosco
 */
public class LimitInsur {
    
    String filiale,giorno,giornoStringa;
    String cop,fx,tot,delta;

    public LimitInsur() {
    }

    public LimitInsur(String filiale, String giorno, String giornoStringa, String cop, String fx, String tot, String delta) {
        this.filiale = filiale;
        this.giorno = giorno;
        this.giornoStringa = giornoStringa;
        this.cop = cop;
        this.fx = fx;
        this.tot = tot;
        this.delta = delta;
    }

    
    
    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getGiorno() {
        return giorno;
    }

    public void setGiorno(String giorno) {
        this.giorno = giorno;
    }

    public String getGiornoStringa() {
        return giornoStringa;
    }

    public void setGiornoStringa(String giornoStringa) {
        this.giornoStringa = giornoStringa;
    }

    public String getCop() {
        return cop;
    }

    public void setCop(String cop) {
        this.cop = cop;
    }

    public String getFx() {
        return fx;
    }

    public void setFx(String fx) {
        this.fx = fx;
    }

    public String getTot() {
        return tot;
    }

    public void setTot(String tot) {
        this.tot = tot;
    }

    public String getDelta() {
        return delta;
    }

    public void setDelta(String delta) {
        this.delta = delta;
    }

    
    
    
    
}
