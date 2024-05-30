/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.generaId;
import static rc.soop.esolver.Util.parseDoubleR;
import static rc.soop.esolver.Util.roundDoubleandFormat;



/**
 *
 * @author rcosco
 */

public class Ch_transaction_refund {
    
    String cod,cod_tr,from,method,branch_cod,type,value,cod_usaegetta,status,user_refund,dt_refund,idopentill_refund,timestamp;

    public Ch_transaction_refund() {
    }

    public Ch_transaction_refund(String cod, String cod_tr, String from, String method, String branch_cod, String type, String value, String cod_usaegetta, String status, String user_refund, String dt_refund, String idopentill_refund, String timestamp) {
        this.cod = cod;
        this.cod_tr = cod_tr;
        this.from = from;
        this.method = method;
        this.branch_cod = branch_cod;
        this.type = type;
        this.value = value;
        this.cod_usaegetta = cod_usaegetta;
        this.status = status;
        this.user_refund = user_refund;
        this.dt_refund = dt_refund;
        this.idopentill_refund = idopentill_refund;
        this.timestamp = timestamp;
    }
    
    
    public Ch_transaction_refund(Ch_transaction tr) {
        this.cod = "REF"+generaId(47);
        this.cod_tr = tr.getCod();
        this.from = "CE";
        this.method = "BO";
        this.branch_cod = tr.getFiliale();
        this.type = "CO";
        this.value = roundDoubleandFormat(fd(tr.getCommission())+parseDoubleR(tr.getRound())+fd(tr.getSpread_total()),2);
        this.status = "0";
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getCod_tr() {
        return cod_tr;
    }

    public void setCod_tr(String cod_tr) {
        this.cod_tr = cod_tr;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getBranch_cod() {
        return branch_cod;
    }

    public void setBranch_cod(String branch_cod) {
        this.branch_cod = branch_cod;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCod_usaegetta() {
        return cod_usaegetta;
    }

    public void setCod_usaegetta(String cod_usaegetta) {
        this.cod_usaegetta = cod_usaegetta;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getUser_refund() {
        return user_refund;
    }

    public void setUser_refund(String user_refund) {
        this.user_refund = user_refund;
    }

    public String getDt_refund() {
        return dt_refund;
    }

    public void setDt_refund(String dt_refund) {
        this.dt_refund = dt_refund;
    }

    public String getIdopentill_refund() {
        return idopentill_refund;
    }

    public void setIdopentill_refund(String idopentill_refund) {
        this.idopentill_refund = idopentill_refund;
    }
    
    
    
}
