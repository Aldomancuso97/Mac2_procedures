/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

/**
 *
 * @author rcosco
 */
public class NC_vatcode {
    
    String nc_gruppo,totalprice,accountingcode1,vatcode1,price1,department1,accountingcode2,vatcode2,price2,department2;

    public NC_vatcode(String nc_gruppo) {
        this.nc_gruppo = nc_gruppo;
        this.totalprice = "";
        this.accountingcode1 = "";
        this.vatcode1 = "";
        this.price1 = "0.00";
        this.department1 = "00";
        this.accountingcode2 = "";
        this.vatcode2 = "";
        this.price2 = "0.00";
        this.department2 = "00";
    }

    public NC_vatcode(String nc_gruppo, String totalprice, String accountingcode1, String vatcode1, String price1, String department1, String accountingcode2, String vatcode2, String price2, String department2) {
        this.nc_gruppo = nc_gruppo;
        this.totalprice = totalprice;
        this.accountingcode1 = accountingcode1;
        this.vatcode1 = vatcode1;
        this.price1 = price1;
        this.department1 = department1;
        this.accountingcode2 = accountingcode2;
        this.vatcode2 = vatcode2;
        this.price2 = price2;
        this.department2 = department2;
    }

    public String getNc_gruppo() {
        return nc_gruppo;
    }

    public void setNc_gruppo(String nc_gruppo) {
        this.nc_gruppo = nc_gruppo;
    }

    public String getTotalprice() {
        return totalprice;
    }

    public void setTotalprice(String totalprice) {
        this.totalprice = totalprice;
    }

    public String getAccountingcode1() {
        return accountingcode1;
    }

    public void setAccountingcode1(String accountingcode1) {
        this.accountingcode1 = accountingcode1;
    }

    public String getVatcode1() {
        return vatcode1;
    }

    public void setVatcode1(String vatcode1) {
        this.vatcode1 = vatcode1;
    }

    public String getPrice1() {
        return price1;
    }

    public void setPrice1(String price1) {
        this.price1 = price1;
    }

    public String getDepartment1() {
        return department1;
    }

    public void setDepartment1(String department1) {
        this.department1 = department1;
    }

    public String getAccountingcode2() {
        return accountingcode2;
    }

    public void setAccountingcode2(String accountingcode2) {
        this.accountingcode2 = accountingcode2;
    }

    public String getVatcode2() {
        return vatcode2;
    }

    public void setVatcode2(String vatcode2) {
        this.vatcode2 = vatcode2;
    }

    public String getPrice2() {
        return price2;
    }

    public void setPrice2(String price2) {
        this.price2 = price2;
    }

    public String getDepartment2() {
        return department2;
    }

    public void setDepartment2(String department2) {
        this.department2 = department2;
    }
    
    
    
    
}
