/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class CambioMTD_YTD {
    
    String filiale;
    String descr;
    
    DateTime datastart;
    DateTime dataend;
    
    String mtd_totale,mtd_totale_prevyear;
    String mtd_bdg;
    String mtd_proiezione_finemese,mtd_proiezione_budget_finemese;
    
    String buy,buy_prevyear,cc,cc_prevyear,buycc,buycc_prevyear,sell,sell_prevyear,totvol,totvol_prevyear;
    String trbuy,trbuy_prevyear,trcctr,trcc_prevyear,trbuycc,trbuycc_prevyear,trsell,trsell_prevyear,trtot,trtot_prevyear;
    String cop,fx;

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }

    public DateTime getDatastart() {
        return datastart;
    }

    public void setDatastart(DateTime datastart) {
        this.datastart = datastart;
    }

    public DateTime getDataend() {
        return dataend;
    }

    public void setDataend(DateTime dataend) {
        this.dataend = dataend;
    }

    public String getMtd_totale() {
        return mtd_totale;
    }

    public void setMtd_totale(String mtd_totale) {
        this.mtd_totale = mtd_totale;
    }

    public String getMtd_totale_prevyear() {
        return mtd_totale_prevyear;
    }

    public void setMtd_totale_prevyear(String mtd_totale_prevyear) {
        this.mtd_totale_prevyear = mtd_totale_prevyear;
    }

    public String getMtd_bdg() {
        return mtd_bdg;
    }

    public void setMtd_bdg(String mtd_bdg) {
        this.mtd_bdg = mtd_bdg;
    }

    public String getMtd_proiezione_finemese() {
        return mtd_proiezione_finemese;
    }

    public void setMtd_proiezione_finemese(String mtd_proiezione_finemese) {
        this.mtd_proiezione_finemese = mtd_proiezione_finemese;
    }

    public String getMtd_proiezione_budget_finemese() {
        return mtd_proiezione_budget_finemese;
    }

    public void setMtd_proiezione_budget_finemese(String mtd_proiezione_budget_finemese) {
        this.mtd_proiezione_budget_finemese = mtd_proiezione_budget_finemese;
    }

    public String getBuy() {
        return buy;
    }

    public void setBuy(String buy) {
        this.buy = buy;
    }

    public String getBuy_prevyear() {
        return buy_prevyear;
    }

    public void setBuy_prevyear(String buy_prevyear) {
        this.buy_prevyear = buy_prevyear;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public String getCc_prevyear() {
        return cc_prevyear;
    }

    public void setCc_prevyear(String cc_prevyear) {
        this.cc_prevyear = cc_prevyear;
    }

    public String getBuycc() {
        return buycc;
    }

    public void setBuycc(String buycc) {
        this.buycc = buycc;
    }

    public String getBuycc_prevyear() {
        return buycc_prevyear;
    }

    public void setBuycc_prevyear(String buycc_prevyear) {
        this.buycc_prevyear = buycc_prevyear;
    }

    public String getSell() {
        return sell;
    }

    public void setSell(String sell) {
        this.sell = sell;
    }

    public String getSell_prevyear() {
        return sell_prevyear;
    }

    public void setSell_prevyear(String sell_prevyear) {
        this.sell_prevyear = sell_prevyear;
    }

    public String getTotvol() {
        return totvol;
    }

    public void setTotvol(String totvol) {
        this.totvol = totvol;
    }

    public String getTotvol_prevyear() {
        return totvol_prevyear;
    }

    public void setTotvol_prevyear(String totvol_prevyear) {
        this.totvol_prevyear = totvol_prevyear;
    }

    public String getTrbuy() {
        return trbuy;
    }

    public void setTrbuy(String trbuy) {
        this.trbuy = trbuy;
    }

    public String getTrbuy_prevyear() {
        return trbuy_prevyear;
    }

    public void setTrbuy_prevyear(String trbuy_prevyear) {
        this.trbuy_prevyear = trbuy_prevyear;
    }

    public String getTrcctr() {
        return trcctr;
    }

    public void setTrcctr(String trcctr) {
        this.trcctr = trcctr;
    }

    public String getTrcc_prevyear() {
        return trcc_prevyear;
    }

    public void setTrcc_prevyear(String trcc_prevyear) {
        this.trcc_prevyear = trcc_prevyear;
    }

    public String getTrbuycc() {
        return trbuycc;
    }

    public void setTrbuycc(String trbuycc) {
        this.trbuycc = trbuycc;
    }

    public String getTrbuycc_prevyear() {
        return trbuycc_prevyear;
    }

    public void setTrbuycc_prevyear(String trbuycc_prevyear) {
        this.trbuycc_prevyear = trbuycc_prevyear;
    }

    public String getTrsell() {
        return trsell;
    }

    public void setTrsell(String trsell) {
        this.trsell = trsell;
    }

    public String getTrsell_prevyear() {
        return trsell_prevyear;
    }

    public void setTrsell_prevyear(String trsell_prevyear) {
        this.trsell_prevyear = trsell_prevyear;
    }

    public String getTrtot() {
        return trtot;
    }

    public void setTrtot(String trtot) {
        this.trtot = trtot;
    }

    public String getTrtot_prevyear() {
        return trtot_prevyear;
    }

    public void setTrtot_prevyear(String trtot_prevyear) {
        this.trtot_prevyear = trtot_prevyear;
    }

    public String getCop() {
        return cop;
    }

    public void setCop(String cop) {
        this.cop = cop;
    }

    public String getFx() {
        return fx;
    }

    public void setFx(String fx) {
        this.fx = fx;
    }
    
}