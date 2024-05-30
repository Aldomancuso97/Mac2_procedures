package rc.soop.esolver;

import java.util.ArrayList;

/**
 *
 * @author rcosco
 */
public class ET_change {

    String cod, id, filiale, user, till_from, fg_tofrom, fg_brba, cod_dest,
            idopen_from, dt_it, fg_annullato, del_dt, del_user, del_motiv,
            note, ip_oneri, filiale_in, id_in, cod_in;

    String valuta, supporto, ip_stock, ip_quantity, ip_rate, ip_total, ip_buyvalue, ip_spread;

    String ip_taglio;
    
    String nc_causal,type;
    
    String fg_stato, tr_dt, tr_user, tr_motiv;
    
    String auto;

    
    public static String typechangeno(String ty){
        if(ty.equals("CH")){
            return "Change";
        }else{
            return "No Change";
        }
    }
    
    public String format_tofrom_brba(String fg_tofrom, String fg_brba) {
        if (fg_tofrom.equals("T")) {
            if (fg_brba.equals("BR")) {
                return "TO BRANCH";
            } else if (fg_brba.equals("BA")) {
                return "TO BANK";
            }
        } else if (fg_tofrom.equals("F")) {
            if (fg_brba.equals("BR")) {
                return "FROM BRANCH";
            } else if (fg_brba.equals("BA")) {
                return "FROM BANK";
            }
        }
        return "";
    }
    
    
    
    public String format_tofrom_brba(String fg_tofrom, String fg_brba, 
            String coddest, ArrayList<String[]> array_credit_card) {
        
        if (fg_tofrom.equals("T")) {
            if (fg_brba.equals("BR")) {
                return "TO BRANCH";
            } else if (fg_brba.equals("BA")) {
                
                for(int i = 0;i<array_credit_card.size();i++){
                    if(array_credit_card.get(i)[0].equals(coddest)){
                        return "TO POS/Bank Account";
                    }
                }
                
                return "TO BANK";
                
                
            }
        } else if (fg_tofrom.equals("F")) {
            if (fg_brba.equals("BR")) {
                return "FROM BRANCH";
            } else if (fg_brba.equals("BA")) {
                return "FROM BANK";
            }
        }
        return "";
    }

    public String format_tofrom(String fg_brba) {
        if (fg_brba.equals("T")) {
            return "TO";
        } else if (fg_brba.equals("F")) {
            return "FROM";
        }
        return "";
    }

    public String formatStatus(String fg_annullato) {
        if (fg_annullato != null) {
            if (fg_annullato.equals("0")) {
                return "<div class='font-green-jungle'>Active <i class='fa fa-check'></i></div>";
            }
            if (fg_annullato.equals("1")) {
                return "<div class='font-red'>Deleted <i class='fa fa-remove'></i></div>";
            }
        }
        return "-";
    }
    
    public String formatStatus_cru(String fg_annullato) {
        if (fg_annullato != null) {
            if (fg_annullato.equals("0")) {
                return "Active ";
            }
            if (fg_annullato.equals("1")) {
                return "Deleted ";
            }
        }
        return "-";
    }
    
    
    
    
    public String getAuto() {
        return auto;
    }

    public void setAuto(String auto) {
        this.auto = auto;
    }
    
    
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    
    public String getNc_causal() {
        return nc_causal;
    }

    public void setNc_causal(String nc_causal) {
        this.nc_causal = nc_causal;
    }
    
    public String getCod_in() {
        return cod_in;
    }

    public void setCod_in(String cod_in) {
        this.cod_in = cod_in;
    }

    public String getFg_stato() {
        return fg_stato;
    }

    public void setFg_stato(String fg_stato) {
        this.fg_stato = fg_stato;
    }

    public String getTr_dt() {
        return tr_dt;
    }

    public void setTr_dt(String tr_dt) {
        this.tr_dt = tr_dt;
    }

    public String getTr_user() {
        return tr_user;
    }

    public void setTr_user(String tr_user) {
        this.tr_user = tr_user;
    }

    public String getTr_motiv() {
        return tr_motiv;
    }

    public void setTr_motiv(String tr_motiv) {
        this.tr_motiv = tr_motiv;
    }

    public String getIp_taglio() {
        return ip_taglio;
    }

    public void setIp_taglio(String ip_taglio) {
        this.ip_taglio = ip_taglio;
    }

    public String getValuta() {
        return valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getSupporto() {
        return supporto;
    }

    public void setSupporto(String supporto) {
        this.supporto = supporto;
    }

    public String getIp_stock() {
        return ip_stock;
    }

    public void setIp_stock(String ip_stock) {
        this.ip_stock = ip_stock;
    }

    public String getIp_quantity() {
        return ip_quantity;
    }

    public void setIp_quantity(String ip_quantity) {
        this.ip_quantity = ip_quantity;
    }

    public String getIp_rate() {
        return ip_rate;
    }

    public void setIp_rate(String ip_rate) {
        this.ip_rate = ip_rate;
    }

    public String getIp_total() {
        return ip_total;
    }

    public void setIp_total(String ip_total) {
        this.ip_total = ip_total;
    }

    public String getIp_buyvalue() {
        return ip_buyvalue;
    }

    public void setIp_buyvalue(String ip_buyvalue) {
        this.ip_buyvalue = ip_buyvalue;
    }

    public String getIp_spread() {
        return ip_spread;
    }

    public void setIp_spread(String ip_spread) {
        this.ip_spread = ip_spread;
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

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTill_from() {
        return till_from;
    }

    public void setTill_from(String till_from) {
        this.till_from = till_from;
    }

    public String getFg_tofrom() {
        return fg_tofrom;
    }

    public void setFg_tofrom(String fg_tofrom) {
        this.fg_tofrom = fg_tofrom;
    }

    public String getFg_brba() {
        return fg_brba;
    }

    public void setFg_brba(String fg_brba) {
        this.fg_brba = fg_brba;
    }

    public String getCod_dest() {
        return cod_dest;
    }

    public void setCod_dest(String cod_dest) {
        this.cod_dest = cod_dest;
    }

    public String getIdopen_from() {
        return idopen_from;
    }

    public void setIdopen_from(String idopen_from) {
        this.idopen_from = idopen_from;
    }

    public String getDt_it() {
        return dt_it;
    }

    public void setDt_it(String dt_it) {
        this.dt_it = dt_it;
    }

    public String getFg_annullato() {
        return fg_annullato;
    }

    public void setFg_annullato(String fg_annullato) {
        this.fg_annullato = fg_annullato;
    }

    public String getDel_dt() {
        return del_dt;
    }

    public void setDel_dt(String del_dt) {
        this.del_dt = del_dt;
    }

    public String getDel_user() {
        return del_user;
    }

    public void setDel_user(String del_user) {
        this.del_user = del_user;
    }

    public String getDel_motiv() {
        return del_motiv;
    }

    public void setDel_motiv(String del_motiv) {
        this.del_motiv = del_motiv;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getIp_oneri() {
        return ip_oneri;
    }

    public void setIp_oneri(String ip_oneri) {
        this.ip_oneri = ip_oneri;
    }

    public String getFiliale_in() {
        return filiale_in;
    }

    public void setFiliale_in(String filiale_in) {
        this.filiale_in = filiale_in;
    }

    public String getId_in() {
        return id_in;
    }

    public void setId_in(String id_in) {
        this.id_in = id_in;
    }

}
