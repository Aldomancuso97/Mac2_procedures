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
public class DailyKind {
    

    
    String kind, toNrTran,toTotal, toLocalCurr, toCC, toB,fromNrTran, fromTotal, fromLocalCurr, fromCC, fromB;
    boolean to,from;
    
    ArrayList dati;
    
    String etichetta1,etichetta2;

    public String getEtichetta1() {
        return etichetta1;
    }

    public void setEtichetta1(String etichetta1) {
        this.etichetta1 = etichetta1;
    }

    public String getEtichetta2() {
        return etichetta2;
    }

    public void setEtichetta2(String etichetta2) {
        this.etichetta2 = etichetta2;
    }

    public String getKind() {
        return kind;
    }

    public String getToTotal() {
        return toTotal;
    }
    
    

    public void setToTotal(String toTotal) {
        this.toTotal = toTotal;
    }

    public String getFromTotal() {
        return fromTotal;
    }

    public void setFromTotal(String fromTotal) {
        this.fromTotal = fromTotal;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public boolean isTo() {
        return to;
    }

    public void setTo(boolean to) {
        this.to = to;
    }

    public boolean isFrom() {
        return from;
    }

    public void setFrom(boolean from) {
        this.from = from;
    }

   

    public String getToNrTran() {
        return toNrTran;
    }

    public void setToNrTran(String toNrTran) {
        this.toNrTran = toNrTran;
    }

    public String getToLocalCurr() {
        return toLocalCurr;
    }

    public void setToLocalCurr(String toLocalCurr) {
        this.toLocalCurr = toLocalCurr;
    }

    public String getToCC() {
        return toCC;
    }

    public void setToCC(String toCC) {
        this.toCC = toCC;
    }

    public String getToB() {
        return toB;
    }

    public void setToB(String toB) {
        this.toB = toB;
    }

    public String getFromNrTran() {
        return fromNrTran;
    }

    public void setFromNrTran(String fromNrTran) {
        this.fromNrTran = fromNrTran;
    }

    public String getFromLocalCurr() {
        return fromLocalCurr;
    }

    public void setFromLocalCurr(String fromLocalCurr) {
        this.fromLocalCurr = fromLocalCurr;
    }

    public String getFromCC() {
        return fromCC;
    }

    public void setFromCC(String fromCC) {
        this.fromCC = fromCC;
    }

    public String getFromB() {
        return fromB;
    }

    public void setFromB(String fromB) {
        this.fromB = fromB;
    }

    public ArrayList getDati() {
        return dati;
    }

    public void setDati(ArrayList dati) {
        this.dati = dati;
    }

    
    
    
}


