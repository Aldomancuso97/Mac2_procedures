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
public class Riga {
    ArrayList<String> valori = new ArrayList();
    String desc = "";
    String formula = "Y";

    
    
    
    
    
    
    public ArrayList<String> getValori() {
        return valori;
    }

    public void setValori(ArrayList<String> valori) {
        this.valori = valori;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getFormula() {
        return formula;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }
}
