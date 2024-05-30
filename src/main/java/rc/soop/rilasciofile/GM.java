/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.HashMap;

/**
 *
 * @author rcosco
 */
public class GM implements Comparable<GM>{
    
    String area;
    String riga1,riga2,riga3;
    String descr,formula;
    String budget,annoprec_adeguato,annoprec_alldata,ytd_finemese,ytd_budget,annoprec_ytd;

    public GM() {
        this.budget = "0.00";
        this.annoprec_adeguato = "0.00";
        this.annoprec_alldata = "0.00";
        this.ytd_finemese = "0.00";
        this.ytd_budget = "0.00";
        this.annoprec_ytd = "0.00";
    }
    
    
    HashMap<String, String> mappavalori = new HashMap<>();

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getBudget() {
        return budget;
    }

    public void setBudget(String budget) {
        this.budget = budget;
    }

    public String getAnnoprec_adeguato() {
        return annoprec_adeguato;
    }

    public void setAnnoprec_adeguato(String annoprec_adeguato) {
        this.annoprec_adeguato = annoprec_adeguato;
    }

    public String getAnnoprec_alldata() {
        return annoprec_alldata;
    }

    public void setAnnoprec_alldata(String annoprec_alldata) {
        this.annoprec_alldata = annoprec_alldata;
    }

    public String getYtd_finemese() {
        return ytd_finemese;
    }

    public void setYtd_finemese(String ytd_finemese) {
        this.ytd_finemese = ytd_finemese;
    }

    public String getYtd_budget() {
        return ytd_budget;
    }

    public void setYtd_budget(String ytd_budget) {
        this.ytd_budget = ytd_budget;
    }

    public String getAnnoprec_ytd() {
        return annoprec_ytd;
    }

    public void setAnnoprec_ytd(String annoprec_ytd) {
        this.annoprec_ytd = annoprec_ytd;
    }
    
    
    
    
    public String getFormula() {
        return formula;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }
    
    public HashMap<String, String> getMappavalori() {
        return mappavalori;
    }

    public void setMappavalori(HashMap<String, String> mappavalori) {
        this.mappavalori = mappavalori;
    }
    
    public String getRiga1() {
        return riga1;
    }

    public void setRiga1(String riga1) {
        this.riga1 = riga1;
    }

    public String getRiga2() {
        return riga2;
    }

    public void setRiga2(String riga2) {
        this.riga2 = riga2;
    }

    public String getRiga3() {
        return riga3;
    }

    public void setRiga3(String riga3) {
        this.riga3 = riga3;
    }

    @Override
    public int compareTo(GM o) {
        return this.area.compareTo(o.getArea());
    }
    
}