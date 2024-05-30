/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import org.joda.time.DateTime;

/**
 *
 * @author rcosco
 */
public class Client_CZ {

    String cod_cliente, cod_tr,
            showanagVALUE, heavy_pno1,
            heavy_cz_country, heavy_cz_issuingcountry,
            heavy_sanctions, heavy_pep,
            heavy_transactionre,
            heavy_moneysource,
            op_sos, date, occupation, pep_position, pep_country;

    public Client_CZ() {
    }

    public Client_CZ(String cod_cliente, String cod_tr, String showanagVALUE, String heavy_pno1, String heavy_cz_country,
            String heavy_cz_issuingcountry, String heavy_sanctions, String heavy_pep, String heavy_transactionre,
            String heavy_moneysource, String op_sos, String occupation, String pep_position, String pep_country) {
        this.cod_cliente = cod_cliente;
        this.cod_tr = cod_tr;
        if (showanagVALUE.trim().equals("none")) {
            this.showanagVALUE = "N";
        } else {
            this.showanagVALUE = "Y";
        }
        this.heavy_pno1 = heavy_pno1;
        this.heavy_cz_country = heavy_cz_country;
        this.heavy_cz_issuingcountry = heavy_cz_issuingcountry;
        this.heavy_sanctions = heavy_sanctions;

        if (heavy_pep == null || heavy_pep.equals("NO") || heavy_pep.equals("N") || heavy_pep.equals("")) {
            this.heavy_pep = "N";
        } else {
            this.heavy_pep = "Y";
        }

        this.heavy_transactionre = heavy_transactionre;
        this.heavy_moneysource = heavy_moneysource;
        if (Boolean.valueOf(op_sos) || op_sos.equals("Y")) {
            this.op_sos = "Y";
        } else {
            this.op_sos = "N";
        }

        this.occupation = occupation;
        this.pep_position = pep_position;
        this.pep_country = pep_country;
        this.date = new DateTime().toString(patternsqldate);
    }
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static String formatYN(String v1) {
        if (v1.equals("Y")) {
            return "YES";
        } else if (v1.equals("N")) {
            return "NO";
        }
        return v1;
    }

    public String getPep_position() {
        return pep_position;
    }

    public void setPep_position(String pep_position) {
        this.pep_position = pep_position;
    }

    public String getPep_country() {
        return pep_country;
    }

    public void setPep_country(String pep_country) {
        this.pep_country = pep_country;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCod_cliente() {
        return cod_cliente;
    }

    public void setCod_cliente(String cod_cliente) {
        this.cod_cliente = cod_cliente;
    }

    public String getCod_tr() {
        return cod_tr;
    }

    public void setCod_tr(String cod_tr) {
        this.cod_tr = cod_tr;
    }

    public String getShowanagVALUE() {
        return showanagVALUE;
    }

    public void setShowanagVALUE(String showanagVALUE) {
        this.showanagVALUE = showanagVALUE;
    }

    public String getHeavy_pno1() {
        return heavy_pno1;
    }

    public void setHeavy_pno1(String heavy_pno1) {
        this.heavy_pno1 = heavy_pno1;
    }

    public String getHeavy_cz_country() {
        return heavy_cz_country;
    }

    public void setHeavy_cz_country(String heavy_cz_country) {
        this.heavy_cz_country = heavy_cz_country;
    }

    public String getHeavy_cz_issuingcountry() {
        return heavy_cz_issuingcountry;
    }

    public void setHeavy_cz_issuingcountry(String heavy_cz_issuingcountry) {
        this.heavy_cz_issuingcountry = heavy_cz_issuingcountry;
    }

    public String getHeavy_sanctions() {
        return heavy_sanctions;
    }

    public void setHeavy_sanctions(String heavy_sanctions) {
        this.heavy_sanctions = heavy_sanctions;
    }

    public String getHeavy_pep() {
        return heavy_pep;
    }

    public void setHeavy_pep(String heavy_pep) {
        this.heavy_pep = heavy_pep;
    }

    public String getHeavy_transactionre() {
        return heavy_transactionre;
    }

    public void setHeavy_transactionre(String heavy_transactionre) {
        this.heavy_transactionre = heavy_transactionre;
    }

    public String getHeavy_moneysource() {
        return heavy_moneysource;
    }

    public void setHeavy_moneysource(String heavy_moneysource) {
        this.heavy_moneysource = heavy_moneysource;
    }

    public String getOp_sos() {
        return op_sos;
    }

    public void setOp_sos(String op_sos) {
        this.op_sos = op_sos;
    }

}
