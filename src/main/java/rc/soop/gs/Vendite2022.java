/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.gs;

/**
 *
 * @author raf
 */
public class Vendite2022 {

    String storeId, dataVendite, categoria;
    double vendutoNetto, vendutoLordo;
    int numScontrini;

    public Vendite2022() {
    }

    public Vendite2022(String storeId, String dataVendite, String categoria, double vendutoNetto, double vendutoLordo, int numScontrini) {
        this.storeId = storeId;
        this.dataVendite = dataVendite;
        this.categoria = categoria;
        this.vendutoNetto = vendutoNetto;
        this.vendutoLordo = vendutoLordo;
        this.numScontrini = numScontrini;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getDataVendite() {
        return dataVendite;
    }

    public void setDataVendite(String dataVendite) {
        this.dataVendite = dataVendite;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    public double getVendutoNetto() {
        return vendutoNetto;
    }

    public void setVendutoNetto(double vendutoNetto) {
        this.vendutoNetto = vendutoNetto;
    }

    public double getVendutoLordo() {
        return vendutoLordo;
    }

    public void setVendutoLordo(double vendutoLordo) {
        this.vendutoLordo = vendutoLordo;
    }

    public int getNumScontrini() {
        return numScontrini;
    }

    public void setNumScontrini(int numScontrini) {
        this.numScontrini = numScontrini;
    }

}
