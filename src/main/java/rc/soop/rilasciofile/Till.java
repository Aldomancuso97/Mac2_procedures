/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.rilasciofile;

/**
 *
 * @author raf
 */
public class Till {
    String cod,de_till,filiale;

    public Till(String cod, String de_till, String filiale) {
        this.cod = cod;
        this.de_till = de_till;
        this.filiale = filiale;
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getDe_till() {
        return de_till;
    }

    public void setDe_till(String de_till) {
        this.de_till = de_till;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }
    
    
}
