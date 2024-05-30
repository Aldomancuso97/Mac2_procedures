/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

/**
 *
 * @author rcosco
 */
public class Settings {
    String id,value,descr;
    
    String cod,amount,branchid; //VALUTE TOP
    
    
    public Settings(String id, String value, String descr) {
        this.id = id;
        this.value = value;
        this.descr = descr;
    }
    
    public Settings(String cod, String amount, String branchid, boolean topvalue){
        this.cod = cod;
        this.amount = amount;
        this.branchid = branchid;
        
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getBranchid() {
        return branchid;
    }

    public void setBranchid(String branchid) {
        this.branchid = branchid;
    }
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }
    
    
}
