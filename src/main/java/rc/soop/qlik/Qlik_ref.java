/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.qlik;

/**
 *
 * @author rcosco
 */
public class Qlik_ref {
    String cod,id,data,filiale,del_fg,user,quantity,price,fee,gruppo_nc,causale_nc,client,type,fix_com,var_com,round,commission,volume,estimated_CO,estimated_GM;

    public Qlik_ref(String cod, String id, String data, String filiale, String del_fg, String user, String quantity, String price, String fee, String gruppo_nc, String causale_nc, String client, String type, String fix_com, String var_com, String round, String commission, String volume, String estimated_CO, String estimated_GM) {
        this.cod = cod;
        this.id = id;
        this.data = data;
        this.filiale = filiale;
        this.del_fg = del_fg;
        this.user = user;
        this.quantity = quantity;
        this.price = price;
        this.fee = fee;
        this.gruppo_nc = gruppo_nc;
        this.causale_nc = causale_nc;
        this.client = client;
        this.type = type;
        this.fix_com = fix_com;
        this.var_com = var_com;
        this.round = round;
        this.commission = commission;
        this.volume = volume;
        this.estimated_CO = estimated_CO;
        this.estimated_GM = estimated_GM;
    }
    
    
    
    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getDel_fg() {
        return del_fg;
    }

    public void setDel_fg(String del_fg) {
        this.del_fg = del_fg;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getFee() {
        return fee;
    }

    public void setFee(String fee) {
        this.fee = fee;
    }

    public String getGruppo_nc() {
        return gruppo_nc;
    }

    public void setGruppo_nc(String gruppo_nc) {
        this.gruppo_nc = gruppo_nc;
    }

    public String getCausale_nc() {
        return causale_nc;
    }

    public void setCausale_nc(String causale_nc) {
        this.causale_nc = causale_nc;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFix_com() {
        return fix_com;
    }

    public void setFix_com(String fix_com) {
        this.fix_com = fix_com;
    }

    public String getVar_com() {
        return var_com;
    }

    public void setVar_com(String var_com) {
        this.var_com = var_com;
    }

    public String getRound() {
        return round;
    }

    public void setRound(String round) {
        this.round = round;
    }

    public String getCommission() {
        return commission;
    }

    public void setCommission(String commission) {
        this.commission = commission;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getEstimated_CO() {
        return estimated_CO;
    }

    public void setEstimated_CO(String estimated_CO) {
        this.estimated_CO = estimated_CO;
    }

    public String getEstimated_GM() {
        return estimated_GM;
    }

    public void setEstimated_GM(String estimated_GM) {
        this.estimated_GM = estimated_GM;
    }
}
