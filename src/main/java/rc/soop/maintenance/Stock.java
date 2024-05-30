/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 *
 * @author rcosco
 */
public class Stock {

    String codice, filiale, tipo, till, idoperation, codiceopenclose, tipostock, cod_value, kind, total, controval, rate, user, date;

    String id_op;

    public Stock(String codice, String filiale, String tipo, String till, String idoperation, String codiceopenclose, String tipostock, String cod_value, String kind, String total, String controval, String rate, String user, String date, String id_op) {
        this.codice = codice;
        this.filiale = filiale;
        this.tipo = tipo;
        this.till = till;
        this.idoperation = idoperation;
        this.codiceopenclose = codiceopenclose;
        this.tipostock = tipostock;
        this.cod_value = cod_value;
        this.kind = kind;
        this.total = total;
        this.controval = controval;
        this.rate = rate;
        this.user = user;
        this.date = date;
        this.id_op = id_op;
    }

    public Stock(String cod_value, String kind, String total, String controval) {
        this.cod_value = cod_value;
        this.kind = kind;
        this.total = total;
        this.controval = controval;
    }

    public Stock() {
    }

    public String getId_op() {
        return id_op;
    }

    public void setId_op(String id_op) {
        this.id_op = id_op;
    }

    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getTill() {
        return till;
    }

    public void setTill(String till) {
        this.till = till;
    }

    public String getIdoperation() {
        return idoperation;
    }

    public void setIdoperation(String idoperation) {
        this.idoperation = idoperation;
    }

    public String getCodiceopenclose() {
        return codiceopenclose;
    }

    public void setCodiceopenclose(String codiceopenclose) {
        this.codiceopenclose = codiceopenclose;
    }

    public String getTipostock() {
        return tipostock;
    }

    public void setTipostock(String tipostock) {
        this.tipostock = tipostock;
    }

    public String getCod_value() {
        return cod_value;
    }

    public void setCod_value(String cod_value) {
        this.cod_value = cod_value;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getControval() {
        return controval;
    }

    public void setControval(String controval) {
        this.controval = controval;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
}