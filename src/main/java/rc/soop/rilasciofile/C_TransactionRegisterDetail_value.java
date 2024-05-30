/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.ArrayList;
import org.joda.time.DateTime;

/**
 *
 * @author srotella
 */
public class C_TransactionRegisterDetail_value implements Comparable<C_TransactionRegisterDetail_value> {

    private String filiale, till, user, date, cur, kind, amountqty, rate, total, perc, commfee, refundoff, payinout;

    String id_filiale, de_filiale, dataDa, dataA;

    ArrayList<C_TransactionRegisterDetail_value> dati;
    DateTime dt;

    /**
     *
     * @param filiale
     * @param till
     * @param user
     * @param date
     * @param cur
     * @param kind
     * @param amountqty
     * @param rate
     * @param total
     * @param perc
     * @param commfee
     * @param refundoff
     * @param payinout
     */
    public C_TransactionRegisterDetail_value(String filiale, String till, String user, String date, String cur, String kind, String amountqty, String rate, String total, String perc, String commfee, String refundoff, String payinout) {
        this.filiale = filiale;
        this.till = till;
        this.user = user;
        this.date = date;
        this.cur = cur;
        this.kind = kind;
        this.amountqty = amountqty;
        this.rate = rate;
        this.total = total;
        this.perc = perc;
        this.commfee = commfee;
        this.refundoff = refundoff;
        this.payinout = payinout;
    }

    /**
     ** Constructor
     */
    public C_TransactionRegisterDetail_value() {
    }

    /**
     *
     * @return
     */
    public ArrayList<C_TransactionRegisterDetail_value> getDati() {
        return dati;
    }

    /**
     *
     * @param dati
     */
    public void setDati(ArrayList<C_TransactionRegisterDetail_value> dati) {
        this.dati = dati;
    }

    /**
     *
     * @return
     */
    public String getId_filiale() {
        return id_filiale;
    }

    /**
     *
     * @param id_filiale
     */
    public void setId_filiale(String id_filiale) {
        this.id_filiale = id_filiale;
    }

    /**
     *
     * @return
     */
    public String getDe_filiale() {
        return de_filiale;
    }

    /**
     *
     * @param de_filiale
     */
    public void setDe_filiale(String de_filiale) {
        this.de_filiale = de_filiale;
    }

    /**
     *
     * @return
     */
    public String getDataDa() {
        return dataDa;
    }

    /**
     *
     * @param dataDa
     */
    public void setDataDa(String dataDa) {
        this.dataDa = dataDa;
    }

    /**
     *
     * @return
     */
    public String getDataA() {
        return dataA;
    }

    /**
     *
     * @param dataA
     */
    public void setDataA(String dataA) {
        this.dataA = dataA;
    }

    /**
     *
     * @return
     */
    public String getFiliale() {
        return filiale;
    }

    /**
     *
     * @param filiale
     */
    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    /**
     *
     * @return
     */
    public String getTill() {
        return till;
    }

    /**
     *
     * @param till
     */
    public void setTill(String till) {
        this.till = till;
    }

    /**
     *
     * @return
     */
    public String getUser() {
        return user;
    }

    /**
     *
     * @param user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     *
     * @return
     */
    public String getDate() {
        return date;
    }

    /**
     *
     * @param date
     */
    public void setDate(String date) {
        this.date = date;
    }

    /**
     *
     * @return
     */
    public String getCur() {
        return cur;
    }

    /**
     *
     * @param cur
     */
    public void setCur(String cur) {
        this.cur = cur;
    }

    /**
     *
     * @return
     */
    public String getKind() {
        return kind;
    }

    /**
     *
     * @param kind
     */
    public void setKind(String kind) {
        this.kind = kind;
    }

    /**
     *
     * @return
     */
    public String getAmountqty() {
        return amountqty;
    }

    /**
     *
     * @param amountqty
     */
    public void setAmountqty(String amountqty) {
        this.amountqty = amountqty;
    }

    /**
     *
     * @return
     */
    public String getRate() {
        return rate;
    }

    /**
     *
     * @param rate
     */
    public void setRate(String rate) {
        this.rate = rate;
    }

    /**
     *
     * @return
     */
    public String getTotal() {
        return total;
    }

    /**
     *
     * @param total
     */
    public void setTotal(String total) {
        this.total = total;
    }

    /**
     *
     * @return
     */
    public String getPerc() {
        return perc;
    }

    /**
     *
     * @param perc
     */
    public void setPerc(String perc) {
        this.perc = perc;
    }

    /**
     *
     * @return
     */
    public String getCommfee() {
        return commfee;
    }

    /**
     *
     * @param commfee
     */
    public void setCommfee(String commfee) {
        this.commfee = commfee;
    }

    /**
     *
     * @return
     */
    public String getRefundoff() {
        return refundoff;
    }

    /**
     *
     * @param refundoff
     */
    public void setRefundoff(String refundoff) {
        this.refundoff = refundoff;
    }

    /**
     *
     * @return
     */
    public String getPayinout() {
        return payinout;
    }

    /**
     *
     * @param payinout
     */
    public void setPayinout(String payinout) {
        this.payinout = payinout;
    }

    public DateTime getDt() {
        return dt;
    }

    public void setDt(DateTime dt) {
        this.dt = dt;
    }

    @Override
    public int compareTo(C_TransactionRegisterDetail_value u) {
        if (getDt() == null || u.getDt() == null) {
            return 0;
        }
        return getDt().compareTo(u.getDt());
    }
}
