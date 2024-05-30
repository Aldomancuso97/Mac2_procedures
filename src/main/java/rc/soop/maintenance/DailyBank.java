/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.util.ArrayList;


/**
 *
 * @author fplacanica
 */
public class DailyBank {
    
    String bank, codice, ntrans, amount;

    public DailyBank(String bank, String codice) {
        this.bank = bank;
        this.codice = codice;
        this.ntrans = "0";
        this.amount = "0.00";
    }

    public DailyBank() {
    }
    
    public static DailyBank get_obj(ArrayList<DailyBank> list, String codice){
        for(int i = 0;i<list.size();i++){
            if(list.get(i).getCodice().equals(codice)){
                return list.get(i);
            }
        }
        return null;
    }
    
    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getBank() {
        return bank;
    }

    public void setBank(String bank) {
        this.bank = bank;
    }

    public String getNtrans() {
        return ntrans;
    }

    public void setNtrans(String ntrans) {
        this.ntrans = ntrans;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }
    
    
}


