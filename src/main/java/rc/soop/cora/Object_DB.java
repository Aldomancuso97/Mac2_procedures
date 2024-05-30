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
public class Object_DB {
    String cod,descr;

    public Object_DB(String cod, String descr) {
        this.cod = cod;
        this.descr = descr;
    }

    public Object_DB() {
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }
    
    
}
