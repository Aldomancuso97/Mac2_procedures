/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.gs;

/**
 *
 * @author raf
 */
public class Filiale {

    String cod, contratto;

    public Filiale() {
    }

    
    public Filiale(String cod, String contratto) {
        this.cod = cod;
        this.contratto = contratto;
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getContratto() {
        return contratto;
    }

    public void setContratto(String contratto) {
        this.contratto = contratto;
    }
}
