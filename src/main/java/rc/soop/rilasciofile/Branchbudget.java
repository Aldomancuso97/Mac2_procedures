package rc.soop.rilasciofile;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author rcosco
 */
public class Branchbudget {
    
    String codice,meseanno,branch,budg1,budg2,colonna1,colonna2,colonna3,colonna4,colonna5,timestamp;

    public Branchbudget() {
    }
    
    public Branchbudget(String codice, String meseanno, String branch, String budg1, String budg2, String colonna1, String colonna2, String colonna3, String colonna4, String colonna5, String timestamp) {
        this.codice = codice;
        this.meseanno = meseanno;
        this.branch = branch;
        this.budg1 = budg1;
        this.budg2 = budg2;
        this.colonna1 = colonna1;
        this.colonna2 = colonna2;
        this.colonna3 = colonna3;
        this.colonna4 = colonna4;
        this.colonna5 = colonna5;
        this.timestamp = timestamp;
    }
    
    
    
    
    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getMeseanno() {
        return meseanno;
    }

    public void setMeseanno(String meseanno) {
        this.meseanno = meseanno;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getBudg1() {
        return budg1;
    }

    public void setBudg1(String budg1) {
        this.budg1 = budg1;
    }

    public String getBudg2() {
        return budg2;
    }

    public void setBudg2(String budg2) {
        this.budg2 = budg2;
    }

    public String getColonna1() {
        return colonna1;
    }

    public void setColonna1(String colonna1) {
        this.colonna1 = colonna1;
    }

    public String getColonna2() {
        return colonna2;
    }

    public void setColonna2(String colonna2) {
        this.colonna2 = colonna2;
    }

    public String getColonna3() {
        return colonna3;
    }

    public void setColonna3(String colonna3) {
        this.colonna3 = colonna3;
    }

    public String getColonna4() {
        return colonna4;
    }

    public void setColonna4(String colonna4) {
        this.colonna4 = colonna4;
    }

    public String getColonna5() {
        return colonna5;
    }

    public void setColonna5(String colonna5) {
        this.colonna5 = colonna5;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    

}
