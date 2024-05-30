/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

/**
 *
 * @author rcosco
 */
public class Rate_history {

    // ENUM('0','1','2') NOT NULL DEFAULT '0' COMMENT '0 -modifica dello spot da parte della sede centrale; 1 - modifica da parte dell’amministratore della percentuale di standard; 2 - modifica da parte dell’operatore del rate standard',
    String codic, filiale, valuta, tipomod, modify, user, dt_mod;

    public Rate_history() {
    }

    public Rate_history(String codic, String filiale, String valuta, String tipomod, String modify, String user, String dt_mod) {
        this.codic = codic;
        this.filiale = filiale;
        this.valuta = valuta;
        this.tipomod = tipomod;
        this.modify = modify;
        this.user = user;
        this.dt_mod = dt_mod;
    }

    public String formatType(String ty) {
        if (ty.equals("0")) {
            return "Modify BCE spot - Central";
        } else if (ty.equals("1")) {
            return "Modify Percent Standard - Admin";
        } else if (ty.equals("2")) {
            return "Modify Rate - Operator";
        }
        return ty;
    }

    public String getCodic() {
        return codic;
    }

    public void setCodic(String codic) {
        this.codic = codic;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getValuta() {
        return valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getTipomod() {
        return tipomod;
    }

    public void setTipomod(String tipomod) {
        this.tipomod = tipomod;
    }

    public String getModify() {
        return modify;
    }

    public void setModify(String modify) {
        this.modify = modify;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDt_mod() {
        return dt_mod;
    }

    public void setDt_mod(String dt_mod) {
        this.dt_mod = dt_mod;
    }

}
