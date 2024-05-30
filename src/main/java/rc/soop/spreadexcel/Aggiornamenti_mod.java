/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.spreadexcel;

/**
 *
 * @author rcosco
 */
public class Aggiornamenti_mod {
    
    String cod,filiale,dt_start,fg_stato,tipost,action,user,timestamp;

    public Aggiornamenti_mod(String cod, String filiale, String dt_start, String fg_stato, String tipost, String action, String user, String timestamp) {
        this.cod = cod;
        this.filiale = filiale;
        this.dt_start = dt_start;
        this.fg_stato = fg_stato;
        this.tipost = tipost;
        this.action = action;
        this.user = user;
        this.timestamp = timestamp;
    }

    public Aggiornamenti_mod() {
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getDt_start() {
        return dt_start;
    }

    public void setDt_start(String dt_start) {
        this.dt_start = dt_start;
    }

    public String getFg_stato() {
        return fg_stato;
    }

    public void setFg_stato(String fg_stato) {
        this.fg_stato = fg_stato;
    }

    public String getTipost() {
        return tipost;
    }

    public void setTipost(String tipost) {
        this.tipost = tipost;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
}