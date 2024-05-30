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
public class BCE {

    String data, valuta;
    double rif_bce;

    public BCE(String data, String valuta, double rif_bce) {
        this.data = data;
        this.valuta = valuta;
        this.rif_bce = rif_bce;
    }
    
    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getValuta() {
        return valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public double getRif_bce() {
        return rif_bce;
    }

    public void setRif_bce(double rif_bce) {
        this.rif_bce = rif_bce;
    }
    
    
    
}
