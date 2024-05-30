/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.cora;

/**
 *
 * @author rcosco
 */
public class Codici {

    String codice, anno, codicecliente;

    public Codici(String codice, String anno, String codicecliente) {
        this.codice = codice;
        this.anno = anno;
        this.codicecliente = codicecliente;
    }
    
    
    
    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getAnno() {
        return anno;
    }

    public void setAnno(String anno) {
        this.anno = anno;
    }

    public String getCodicecliente() {
        return codicecliente;
    }

    public void setCodicecliente(String codicecliente) {
        this.codicecliente = codicecliente;
    }
    
    
}
