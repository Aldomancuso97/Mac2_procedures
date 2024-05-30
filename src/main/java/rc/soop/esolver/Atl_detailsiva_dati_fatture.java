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
public class Atl_detailsiva_dati_fatture {

    String cod, nrigaiva, contoiva, segnoiva, codeiva, imponibile, iva;

    public Atl_detailsiva_dati_fatture(String cod, String nrigaiva, String contoiva, String segnoiva, String codeiva, String imponibile, String iva) {
        this.cod = cod;
        this.nrigaiva = nrigaiva;
        this.contoiva = contoiva;
        this.segnoiva = segnoiva;
        this.codeiva = codeiva;
        this.imponibile = imponibile;
        this.iva = iva;
    }

    public String controlli_details() {
        return "OK";
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getNrigaiva() {
        return nrigaiva;
    }

    public void setNrigaiva(String nrigaiva) {
        this.nrigaiva = nrigaiva;
    }

    public String getContoiva() {
        return contoiva;
    }

    public void setContoiva(String contoiva) {
        this.contoiva = contoiva;
    }

    public String getSegnoiva() {
        return segnoiva;
    }

    public void setSegnoiva(String segnoiva) {
        this.segnoiva = segnoiva;
    }

    public String getCodeiva() {
        return codeiva;
    }

    public void setCodeiva(String codeiva) {
        this.codeiva = codeiva;
    }

    public String getImponibile() {
        return imponibile;
    }

    public void setImponibile(String imponibile) {
        this.imponibile = imponibile;
    }

    public String getIva() {
        return iva;
    }

    public void setIva(String iva) {
        this.iva = iva;
    }

}
