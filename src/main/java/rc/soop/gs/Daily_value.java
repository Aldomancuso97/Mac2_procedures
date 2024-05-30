/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.gs;

import java.util.ArrayList;

/**
 *
 * @author srotella
 */
public class Daily_value {
    
    String id_filiale, de_filiale,user, data;
    
    String purchTotal, purchComm, purchGrossTot, purchSpread, purchProfit;
    String salesTotal, salesComm, salesGrossTot, salesSpread, salesProfit;
    String cashAdNetTot, cashAdComm, cashAdGrossTot, cashAdSpread, cashAdProfit;
    String TotNetTot, TotComm, TotGrossTot, TotSpread, TotProfit;
    
        
    String baPurchTotal, baPurchSpread, baPurchCreditCard, baPurchTransfNotes, baPurchTransfOther;
    String baSalesTotal, baSalesSpread, baSalesCreditCard, baSalesTransfNotes, baSalesTransfOther;
    
    String braPurchTotal, braPurchSpread,braPurchLocalCurr;
    String braSalesTotal, braSalesSpread,braSalesLocalCurr;
    
    String groffTurnover, grossProfit, lastCashOnPrem, cashOnPrem,fx;
    String cashOnPremFromTrans,cashOnPremError, fxClosureErrorDeclared;
    
    String noTransPurch, noTransCC, noTransSales, total;
    
    String totPos, totAcc;
    
    String refund;
    
    ArrayList dati;
    ArrayList datiCOP;
    ArrayList datiBank;

    String officesp;

    public String getOfficesp() {
        return officesp;
    }

    public void setOfficesp(String officesp) {
        this.officesp = officesp;
    }
    
    public String getRefund() {
        return refund;
    }

    public void setRefund(String refund) {
        this.refund = refund;
    }
    
    
    

    public ArrayList getDatiBank() {
        return datiBank;
    }

    public void setDatiBank(ArrayList datiBank) {
        this.datiBank = datiBank;
    }
    
    

    public String getId_filiale() {
        return id_filiale;
    }

    public void setId_filiale(String id_filiale) {
        this.id_filiale = id_filiale;
    }

    public String getDe_filiale() {
        return de_filiale;
    }

    public void setDe_filiale(String de_filiale) {
        this.de_filiale = de_filiale;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getPurchTotal() {
        return purchTotal;
    }

    public void setPurchTotal(String purchTotal) {
        this.purchTotal = purchTotal;
    }

    public String getPurchComm() {
        return purchComm;
    }

    public void setPurchComm(String purchComm) {
        this.purchComm = purchComm;
    }

    public String getPurchGrossTot() {
        return purchGrossTot;
    }

    public void setPurchGrossTot(String purchGrossTot) {
        this.purchGrossTot = purchGrossTot;
    }

    public String getPurchSpread() {
        return purchSpread;
    }

    public void setPurchSpread(String purchSpread) {
        this.purchSpread = purchSpread;
    }

    public String getPurchProfit() {
        return purchProfit;
    }

    public void setPurchProfit(String purchProfit) {
        this.purchProfit = purchProfit;
    }

    public String getSalesTotal() {
        return salesTotal;
    }

    public void setSalesTotal(String salesTotal) {
        this.salesTotal = salesTotal;
    }

    public String getSalesComm() {
        return salesComm;
    }

    public void setSalesComm(String salesComm) {
        this.salesComm = salesComm;
    }

    public String getSalesGrossTot() {
        return salesGrossTot;
    }

    public void setSalesGrossTot(String salesGrossTot) {
        this.salesGrossTot = salesGrossTot;
    }

    public String getSalesSpread() {
        return salesSpread;
    }

    public void setSalesSpread(String salesSpread) {
        this.salesSpread = salesSpread;
    }

    public String getSalesProfit() {
        return salesProfit;
    }

    public void setSalesProfit(String salesProfit) {
        this.salesProfit = salesProfit;
    }

    public String getCashAdNetTot() {
        return cashAdNetTot;
    }

    public void setCashAdNetTot(String cashAdNetTot) {
        this.cashAdNetTot = cashAdNetTot;
    }

    public String getCashAdComm() {
        return cashAdComm;
    }

    public void setCashAdComm(String cashAdComm) {
        this.cashAdComm = cashAdComm;
    }

    public String getCashAdGrossTot() {
        return cashAdGrossTot;
    }

    public void setCashAdGrossTot(String cashAdGrossTot) {
        this.cashAdGrossTot = cashAdGrossTot;
    }

    public String getCashAdSpread() {
        return cashAdSpread;
    }

    public void setCashAdSpread(String cashAdSpread) {
        this.cashAdSpread = cashAdSpread;
    }

    public String getCashAdProfit() {
        return cashAdProfit;
    }

    public void setCashAdProfit(String cashAdProfit) {
        this.cashAdProfit = cashAdProfit;
    }

    public String getTotNetTot() {
        return TotNetTot;
    }

    public void setTotNetTot(String TotNetTot) {
        this.TotNetTot = TotNetTot;
    }

    public String getTotComm() {
        return TotComm;
    }

    public void setTotComm(String TotComm) {
        this.TotComm = TotComm;
    }

    public String getTotGrossTot() {
        return TotGrossTot;
    }

    public void setTotGrossTot(String TotGrossTot) {
        this.TotGrossTot = TotGrossTot;
    }

    public String getTotSpread() {
        return TotSpread;
    }

    public void setTotSpread(String TotSpread) {
        this.TotSpread = TotSpread;
    }

    public String getTotProfit() {
        return TotProfit;
    }

    public void setTotProfit(String TotProfit) {
        this.TotProfit = TotProfit;
    }


    public String getBaPurchTotal() {
        return baPurchTotal;
    }

    public void setBaPurchTotal(String baPurchTotal) {
        this.baPurchTotal = baPurchTotal;
    }

    public String getBaPurchSpread() {
        return baPurchSpread;
    }

    public void setBaPurchSpread(String baPurchSpread) {
        this.baPurchSpread = baPurchSpread;
    }

    public String getBaPurchCreditCard() {
        return baPurchCreditCard;
    }

    public void setBaPurchCreditCard(String baPurchCreditCard) {
        this.baPurchCreditCard = baPurchCreditCard;
    }

    public String getBaPurchTransfNotes() {
        return baPurchTransfNotes;
    }

    public void setBaPurchTransfNotes(String baPurchTransfNotes) {
        this.baPurchTransfNotes = baPurchTransfNotes;
    }

    public String getBaPurchTransfOther() {
        return baPurchTransfOther;
    }

    public void setBaPurchTransfOther(String baPurchTransfOther) {
        this.baPurchTransfOther = baPurchTransfOther;
    }

    public String getBaSalesTotal() {
        return baSalesTotal;
    }

    public void setBaSalesTotal(String baSalesTotal) {
        this.baSalesTotal = baSalesTotal;
    }

    public String getBaSalesSpread() {
        return baSalesSpread;
    }

    public void setBaSalesSpread(String baSalesSpread) {
        this.baSalesSpread = baSalesSpread;
    }

    public String getBaSalesCreditCard() {
        return baSalesCreditCard;
    }

    public void setBaSalesCreditCard(String baSalesCreditCard) {
        this.baSalesCreditCard = baSalesCreditCard;
    }

    public String getBaSalesTransfNotes() {
        return baSalesTransfNotes;
    }

    public void setBaSalesTransfNotes(String baSalesTransfNotes) {
        this.baSalesTransfNotes = baSalesTransfNotes;
    }

    public String getBaSalesTransfOther() {
        return baSalesTransfOther;
    }

    public void setBaSalesTransfOther(String baSalesTransfOther) {
        this.baSalesTransfOther = baSalesTransfOther;
    }

    public String getBraPurchTotal() {
        return braPurchTotal;
    }

    public void setBraPurchTotal(String braPurchTotal) {
        this.braPurchTotal = braPurchTotal;
    }

    public String getBraPurchSpread() {
        return braPurchSpread;
    }

    public void setBraPurchSpread(String braPurchSpread) {
        this.braPurchSpread = braPurchSpread;
    }

    public String getBraPurchLocalCurr() {
        return braPurchLocalCurr;
    }

    public void setBraPurchLocalCurr(String braPurchLocalCurr) {
        this.braPurchLocalCurr = braPurchLocalCurr;
    }

    public String getBraSalesTotal() {
        return braSalesTotal;
    }

    public void setBraSalesTotal(String braSalesTotal) {
        this.braSalesTotal = braSalesTotal;
    }

    public String getBraSalesSpread() {
        return braSalesSpread;
    }

    public void setBraSalesSpread(String braSalesSpread) {
        this.braSalesSpread = braSalesSpread;
    }

    public String getBraSalesLocalCurr() {
        return braSalesLocalCurr;
    }

    public void setBraSalesLocalCurr(String braSalesLocalCurr) {
        this.braSalesLocalCurr = braSalesLocalCurr;
    }

    public String getGroffTurnover() {
        return groffTurnover;
    }

    public void setGroffTurnover(String groffTurnover) {
        this.groffTurnover = groffTurnover;
    }

    public String getGrossProfit() {
        return grossProfit;
    }

    public void setGrossProfit(String grossProfit) {
        this.grossProfit = grossProfit;
    }

    public String getLastCashOnPrem() {
        return lastCashOnPrem;
    }

    public void setLastCashOnPrem(String lastCashOnPrem) {
        this.lastCashOnPrem = lastCashOnPrem;
    }

    public String getCashOnPrem() {
        return cashOnPrem;
    }

    public void setCashOnPrem(String cashOnPrem) {
        this.cashOnPrem = cashOnPrem;
    }

    public String getFx() {
        return fx;
    }

    public void setFx(String fx) {
        this.fx = fx;
    }

    public String getCashOnPremFromTrans() {
        return cashOnPremFromTrans;
    }

    public void setCashOnPremFromTrans(String cashOnPremFromTrans) {
        this.cashOnPremFromTrans = cashOnPremFromTrans;
    }

    public String getCashOnPremError() {
        return cashOnPremError;
    }

    public void setCashOnPremError(String cashOnPremError) {
        this.cashOnPremError = cashOnPremError;
    }

    public String getFxClosureErrorDeclared() {
        return fxClosureErrorDeclared;
    }

    public void setFxClosureErrorDeclared(String fxClosureErrorDeclared) {
        this.fxClosureErrorDeclared = fxClosureErrorDeclared;
    }

    public String getNoTransPurch() {
        return noTransPurch;
    }

    public void setNoTransPurch(String noTransPurch) {
        this.noTransPurch = noTransPurch;
    }

    public String getNoTransCC() {
        return noTransCC;
    }

    public void setNoTransCC(String noTransCC) {
        this.noTransCC = noTransCC;
    }

    public String getNoTransSales() {
        return noTransSales;
    }

    public void setNoTransSales(String noTransSales) {
        this.noTransSales = noTransSales;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getTotPos() {
        return totPos;
    }

    public void setTotPos(String totPos) {
        this.totPos = totPos;
    }

    public String getTotAcc() {
        return totAcc;
    }

    public void setTotAcc(String totAcc) {
        this.totAcc = totAcc;
    }

    public ArrayList getDati() {
        return dati;
    }

    public void setDati(ArrayList dati) {
        this.dati = dati;
    }

    public ArrayList getDatiCOP() {
        return datiCOP;
    }

    public void setDatiCOP(ArrayList datiCOP) {
        this.datiCOP = datiCOP;
    }

    
}
