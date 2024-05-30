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
public class Transaction {
    String data_inizio, importo3, altre_info;
    
    public Transaction(String data_inizio, String importo3, String altre_info) {
        this.data_inizio = data_inizio;
        this.importo3 = importo3;
        this.altre_info = altre_info;
    }
    
    public String getData_inizio() {
        return data_inizio;
    }

    public void setData_inizio(String data_inizio) {
        this.data_inizio = data_inizio;
    }

    public String getImporto3() {
        return importo3;
    }

    public void setImporto3(String importo3) {
        this.importo3 = importo3;
    }

    public String getAltre_info() {
        return altre_info;
    }

    public void setAltre_info(String altre_info) {
        this.altre_info = altre_info;
    }
    
    
}
