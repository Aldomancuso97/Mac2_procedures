/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Items {

    int numpre;
    String cod, descr, visual;

    String value1;
    String value2;

    String codice, descrizione, tipologia, perc_value, euro_value, sogliaminima, valuta, branch;

    public Items(String codice, String descrizione, String tipologia, String perc_value, String euro_value, String sogliaminima, String valuta, String branch) {
        this.codice = codice;
        this.descrizione = descrizione;
        this.tipologia = tipologia;
        this.perc_value = perc_value;
        this.euro_value = euro_value;
        this.sogliaminima = sogliaminima;
        this.valuta = valuta;
        this.branch = branch;
    }

    public Items(String cod, String descr, String visual) {
        this.cod = cod;
        this.descr = descr;
        this.visual = visual;
        this.value1 = "";
    }

    public Items(String cod, String descr, String visual, String value1, String value2) {
        this.cod = cod;
        this.descr = descr;
        this.visual = visual;
        this.value1 = value1;
        this.value2 = value2;
    }

    public static List<Items> all_BR(String crv) {
        Database db = new Database();
        List<Items> out = db.get_branch_list(crv);
        db.closeDB();
        return out;
    }

    public static List<Items> all_CUR() {
        Database db = new Database();
        List<Items> out = db.get_currency_list();
        db.closeDB();
        return out;
    }

    public static List<Items> all_STATUS() {
        List<Items> out = new ArrayList<>();
        out.add(new Items("0", "Pending", "Pending"));
        out.add(new Items("1", "Send", "Send"));
        out.add(new Items("3", "Rejected", "Rejected"));
        out.add(new Items("4", "Cancelled", "Cancelled"));
        out.add(new Items("5", "No Show", "No Show"));
        return out;
    }

    public static List<Items> all_Status_NEW(String selezionato) {
        List<Items> out = new ArrayList<>();
        if (selezionato.equals("0")) {
            out.add(new Items("0", "selected", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--info kt-badge--pill'>DA VERIFICARE</span>"));
        } else {
            out.add(new Items("0", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--info kt-badge--pill'>DA VERIFICARE</span>"));
        }
        out.add(new Items("1", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--purple kt-badge--pill'>VALUTA TOP</span>"));
        out.add(new Items("9", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--crm kt-badge--pill'>CARICO CRM</span>"));
        out.add(new Items("2", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--orange kt-badge--pill'>CONFERMATA</span>"));
        out.add(new Items("3", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--yellow kt-badge--pill'>OK MODIFICA</span>"));
        out.add(new Items("4", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--success kt-badge--pill'>INVIATA</span>"));
        out.add(new Items("5", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--red kt-badge--pill'>RIGETTATA</span>"));
        if (!selezionato.equals("hidden")) {
            out.add(new Items("6", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--brown kt-badge--pill'>CANCELLATA</span>"));
            out.add(new Items("7", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--light kt-badge--pill'>CHIUSA</span>"));
            out.add(new Items("8", "", "<span class='kt-badge kt-badge--inline kt-badge--rounded kt-badge--grey kt-badge--pill'>NO SHOW</span>"));
        }
        return out;
    }

    public static List<Items> all_Status_NOLABEL() {
        List<Items> out = new ArrayList<>();
        out.add(new Items("0", "", "DA VERIFICARE"));
        out.add(new Items("1", "", "VALUTA TOP"));
        out.add(new Items("9", "", "CARICO CRM"));
        out.add(new Items("2", "", "CONFERMATA"));
        out.add(new Items("3", "", "OK MODIFICA"));
        out.add(new Items("4", "", "INVIATA"));
        out.add(new Items("5", "", "RIGETTATA"));
        out.add(new Items("6", "", "CANCELLATA"));
        out.add(new Items("7", "", "CHIUSA"));
        out.add(new Items("8", "", "NO SHOW"));
        return out;
    }

    public static HashMap<String, String> CRV_BRANCH() {
        HashMap<String, String> out = new HashMap();
        out.put("045", "805");//FIRENZE
        out.put("046", "804");//MALPENSA
        out.put("047", "801");//MILANO
        out.put("048", "803");//ROMA
        out.put("049", "802");//VENEZIA
        return out;
    }

    public static HashMap<String, String> User_Status_map() {
        HashMap<String, String> out = new HashMap();
        out.put("0", "<span class='kt-badge kt-badge--danger kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-danger'>Banned</span>");
        out.put("1", "<span class='kt-badge kt-badge--success kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-success'>Active</span>");
        out.put("2", "<span class='kt-badge kt-badge--warning kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-warning'>First Access</span>");
        return out;
    }

    public static HashMap<String, String> statusCrmToMac() {
        HashMap<String, String> out = new HashMap();
        out.put("0", "0");
        out.put("1", "0");
        out.put("9", "0");
        out.put("2", "0");
        out.put("3", "0");
        out.put("4", "0");
        out.put("5", "5");
        out.put("6", "6");
        out.put("7", "7");
        out.put("8", "8");

        return out;
    }

    public static List<Items> User_Status_item() {
        List<Items> out = new ArrayList();
        out.add(new Items("0", "", "<span class='kt-badge kt-badge--danger kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-danger'>Banned</span>"));
        out.add(new Items("1", "", "<span class='kt-badge kt-badge--success kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-success'>Active</span>"));
        out.add(new Items("2", "", "<span class='kt-badge kt-badge--warning kt-badge--dot'></span>&nbsp;<span class='kt-font-bold kt-font-warning'>First Access</span>"));
        return out;
    }

    public static HashMap<String, String> User_Type_map() {
        HashMap<String, String> out = new HashMap();
        out.put("1", "<span class='kt-badge kt-badge--primary kt-badge--inline kt-badge--rounded'>Administration</span>");
        out.put("2", "<span class='kt-badge kt-badge--info kt-badge--inline kt-badge--rounded'>Marketing</span>");
        out.put("3", "<span class='kt-badge kt-badge--dark kt-badge--inline kt-badge--rounded'>CRV</span>");
        out.put("4", "<span class='kt-badge kt-badge--canc kt-badge--inline kt-badge--rounded'>CRM</span>");
        return out;
    }

    public static List<Items> User_Type_item() {
        List<Items> out = new ArrayList<>();
        out.add(new Items("1", "", "<span class='kt-badge kt-badge--primary kt-badge--inline kt-badge--rounded'>Administration</span>"));
        out.add(new Items("2", "", "<span class='kt-badge kt-badge--info kt-badge--inline kt-badge--rounded'>Marketing</span>"));
        out.add(new Items("3", "", "<span class='kt-badge kt-badge--dark kt-badge--inline kt-badge--rounded'>CRV</span>"));
        out.add(new Items("4", "selected", "<span class='kt-badge kt-badge--canc kt-badge--inline kt-badge--rounded'>CRM</span>"));
        return out;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public int getNumpre() {
        return numpre;
    }

    public void setNumpre(int numpre) {
        this.numpre = numpre;
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

    public String getVisual() {
        return visual;
    }

    public void setVisual(String visual) {
        this.visual = visual;
    }

    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getDescrizione() {
        return descrizione;
    }

    public void setDescrizione(String descrizione) {
        this.descrizione = descrizione;
    }

    public String getTipologia() {
        return tipologia;
    }

    public void setTipologia(String tipologia) {
        this.tipologia = tipologia;
    }

    public String getPerc_value() {
        return perc_value;
    }

    public void setPerc_value(String perc_value) {
        this.perc_value = perc_value;
    }

    public String getEuro_value() {
        return euro_value;
    }

    public void setEuro_value(String euro_value) {
        this.euro_value = euro_value;
    }

    public String getSogliaminima() {
        return sogliaminima;
    }

    public void setSogliaminima(String sogliaminima) {
        this.sogliaminima = sogliaminima;
    }

    public String getValuta() {
        return valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

}
