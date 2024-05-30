/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

/**
 *
 * @author rcosco
 */
public class NC_val{
    
    String cat,year,filiale;
    double quantity,tot;

    public NC_val() {
    }
    
    public NC_val(String cat, String year, String filiale, double quantity, double tot) {
        this.cat = cat;
        this.year = year;
        this.filiale = filiale;
        this.quantity = quantity;
        this.tot = tot;
    }
    
    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }
    
    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public double getTot() {
        return tot;
    }

    public void setTot(double tot) {
        this.tot = tot;
    }

    
    
}
