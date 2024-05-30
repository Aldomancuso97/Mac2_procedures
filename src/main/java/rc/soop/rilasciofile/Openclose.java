/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.ArrayList;

/**
 *
 * @author rcosco
 */
public class Openclose {

    String filiale, cod, id, till, user, fg_tipo, data, errors; //generali

    String valuta, kind, value_op, num_kind_op, value_cl, num_kind_cl; //change

    String ip_taglio, ip_quantity, ip_value; //tagli

    String gruppo_nc, quantity_user, quantity_system; //nochange

    String carta_credito,ip_quantity_op,ip_value_op,ip_quantity_sys,ip_value_sys; //pos
    
    String foreign_tr,local_tr,stock_tr,cod_it,cod_itnc;//transfer on safe

    String note, tipo,total_diff,rate; //errors
    
    String fisico;
    
    
    public Openclose() {
    }
    
    
    
    public Openclose(String filiale, String cod, String id, String till, String user, String fg_tipo, String data, String errors) {
        this.filiale = filiale;
        this.cod = cod;
        this.id = id;
        this.till = till;
        this.user = user;
        if (fg_tipo.equals("O")) {
            this.fg_tipo = "OPEN";
        } else if (fg_tipo.equals("C")) {
            this.fg_tipo = "CLOSE";
        }
        this.data = data;
        this.errors = formatErrors_cru(errors);
    }
    
    
    public String formatType(String fg_tipo) {
        if (fg_tipo != null) {
            if (fg_tipo.equals("OPEN")) {
                return "<span class='font-blue'>OPEN <i class='fa fa-level-up'></i></span>";
            } else if (fg_tipo.equals("CLOSE")) {
                return "<span class='font-green-sharp'>CLOSE <i class='fa fa-level-down'></i></span>";
            }
        }
        return "-";
    }
    public String formatType_cru(String fg_tipo) {
        if (fg_tipo != null) {
            if (fg_tipo.equals("OPEN")) {
                return "OPEN ";
            } else if (fg_tipo.equals("CLOSE")) {
                return "CLOSE ";
            }
        }
        return "-";
    }
    public static String formatType_r(String fg_tipo) {
        if (fg_tipo != null) {
            if (fg_tipo.equals("O")) {
                return "OPEN ";
            } else if (fg_tipo.equals("C")) {
                return "CLOSE ";
            }
        }
        return "-";
    }
    
    public String formatErrors(String errors) {
        if (errors != null) {
            if (errors.equals("N")) {
                return "<div class='font-green-jungle'>No Errors <i class='fa fa-check'></i></div>";
            } else if (errors.equals("Y")) {
                return "<div class='font-red'>Errors <i class='fa fa-remove'></i></div>";
            }
        }
        return "-";
    }
    
    public String formatErrors_cru(String errors) {
        if (errors != null) {            
            if (errors.equals("N") || errors.contains("fa-check")) {
                return "No Errors";
            } else  {
                return "Errors ";
            }
        }
        return "-";
    }

    public static String formatTypeErrors(String errors) {
        if (errors != null) {
            if (errors.equals("CH")) {
                return "Figures";
            } else if (errors.equals("NC")) {
                return "No Change";
            } else if (errors.equals("PO")) {
                return "POS / Bank Account";
            }
        }
        return "-";
    }

    public String getFisico() {
        return fisico;
    }

    public void setFisico(String fisico) {
        this.fisico = fisico;
    }
    
    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getForeign_tr() {
        return foreign_tr;
    }

    public void setForeign_tr(String foreign_tr) {
        this.foreign_tr = foreign_tr;
    }

    public String getLocal_tr() {
        return local_tr;
    }

    public void setLocal_tr(String local_tr) {
        this.local_tr = local_tr;
    }

    public String getStock_tr() {
        return stock_tr;
    }

    public void setStock_tr(String stock_tr) {
        this.stock_tr = stock_tr;
    }

    public String getCod_it() {
        return cod_it;
    }

    public void setCod_it(String cod_it) {
        this.cod_it = cod_it;
    }
    
    public String getTotal_diff() {
        return total_diff;
    }

    public void setTotal_diff(String total_diff) {
        this.total_diff = total_diff;
    }
    
    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getCarta_credito() {
        return carta_credito;
    }

    public void setCarta_credito(String carta_credito) {
        this.carta_credito = carta_credito;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getGruppo_nc() {
        return gruppo_nc;
    }

    public void setGruppo_nc(String gruppo_nc) {
        this.gruppo_nc = gruppo_nc;
    }

    public String getQuantity_user() {
        return quantity_user;
    }

    public void setQuantity_user(String quantity_user) {
        this.quantity_user = quantity_user;
    }

    public String getQuantity_system() {
        return quantity_system;
    }

    public void setQuantity_system(String quantity_system) {
        this.quantity_system = quantity_system;
    }

    public String getIp_taglio() {
        return ip_taglio;
    }

    public void setIp_taglio(String ip_taglio) {
        this.ip_taglio = ip_taglio;
    }

    public String getIp_quantity() {
        return ip_quantity;
    }

    public void setIp_quantity(String ip_quantity) {
        this.ip_quantity = ip_quantity;
    }

    public String getIp_value() {
        return ip_value;
    }

    public void setIp_value(String ip_value) {
        this.ip_value = ip_value;
    }

    public String getValuta() {
        return valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getValue_op() {
        return value_op;
    }

    public void setValue_op(String value_op) {
        this.value_op = value_op;
    }

    public String getNum_kind_op() {
        return num_kind_op;
    }

    public void setNum_kind_op(String num_kind_op) {
        this.num_kind_op = num_kind_op;
    }

    public String getValue_cl() {
        return value_cl;
    }

    public void setValue_cl(String value_cl) {
        this.value_cl = value_cl;
    }

    public String getNum_kind_cl() {
        return num_kind_cl;
    }

    public void setNum_kind_cl(String num_kind_cl) {
        this.num_kind_cl = num_kind_cl;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
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

    public String getTill() {
        return till;
    }

    public void setTill(String till) {
        this.till = till;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFg_tipo() {
        return fg_tipo;
    }

    public void setFg_tipo(String fg_tipo) {
        this.fg_tipo = fg_tipo;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getErrors() {
        return errors;
    }

    public void setErrors(String errors) {
        this.errors = errors;
    }

    public String getIp_quantity_op() {
        return ip_quantity_op;
    }

    public void setIp_quantity_op(String ip_quantity_op) {
        this.ip_quantity_op = ip_quantity_op;
    }

    public String getIp_value_op() {
        return ip_value_op;
    }

    public void setIp_value_op(String ip_value_op) {
        this.ip_value_op = ip_value_op;
    }

    public String getIp_quantity_sys() {
        return ip_quantity_sys;
    }

    public void setIp_quantity_sys(String ip_quantity_sys) {
        this.ip_quantity_sys = ip_quantity_sys;
    }

    public String getIp_value_sys() {
        return ip_value_sys;
    }

    public void setIp_value_sys(String ip_value_sys) {
        this.ip_value_sys = ip_value_sys;
    }

    public String getCod_itnc() {
        return cod_itnc;
    }

    public void setCod_itnc(String cod_itnc) {
        this.cod_itnc = cod_itnc;
    }
    

}