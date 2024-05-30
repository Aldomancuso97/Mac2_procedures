/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import java.util.ArrayList;

/**
 *
 * @author srotella
 */
public class TillTransactionListBB_value {
    
    ArrayList dati;   
    
    String id_filiale, de_filiale,till,user,notr,time,cur,kind,amount,rate,total,perc,comfree,payinpayout,customer,spread,fig,pos;
    
    String transvalueresidentbuy,transvaluenonresidentbuy,commisionvaluetresidentbuy,commisionvaluenonresidentbuy,transactionnumberresidentbuy,transactionnumbernonresidentbuy,internetbookingamountyes,internetbookingnumberyes;
    String transvalueresidentsell,transvaluenonresidentsell,commisionvaluetresidentsell,commisionvaluenonresidentsell,transactionnumberresidentsell,transactionnumbernonresidentsell,internetbookingamountno,internetbookingnumberno;
    
    ArrayList footerdati;
    
    String residentnonresident;
    
    String posbuyamount,posbuynumber,possellamount,possellnumber;
    
    String internetbooking;
    
    String type="";
    
    String round="";
    
    String delete1="";
    String delete2="";
    
    String bankbuyamount,bankbuynumber,banksellamount,banksellnumber;

    public String getDelete1() {
        return delete1;
    }

    public void setDelete1(String delete1) {
        this.delete1 = delete1;
    }

    public String getDelete2() {
        return delete2;
    }

    public void setDelete2(String delete2) {
        this.delete2 = delete2;
    }

    
    
    
    public String getRound() {
        return round;
    }

    public void setRound(String round) {
        this.round = round;
    }

    public String getBankbuyamount() {
        return bankbuyamount;
    }

    public void setBankbuyamount(String bankbuyamount) {
        this.bankbuyamount = bankbuyamount;
    }

    public String getBankbuynumber() {
        return bankbuynumber;
    }

    public void setBankbuynumber(String bankbuynumber) {
        this.bankbuynumber = bankbuynumber;
    }

    public String getBanksellamount() {
        return banksellamount;
    }

    public void setBanksellamount(String banksellamount) {
        this.banksellamount = banksellamount;
    }

    public String getBanksellnumber() {
        return banksellnumber;
    }

    public void setBanksellnumber(String banksellnumber) {
        this.banksellnumber = banksellnumber;
    }
    
    

    public String getInternetbooking() {
        return internetbooking;
    }

    public void setInternetbooking(String internetbooking) {
        this.internetbooking = internetbooking;
    }

    public String getPosbuyamount() {
        return posbuyamount;
    }

    public void setPosbuyamount(String posbuyamount) {
        this.posbuyamount = posbuyamount;
    }

    public String getPosbuynumber() {
        return posbuynumber;
    }

    public void setPosbuynumber(String posbuynumber) {
        this.posbuynumber = posbuynumber;
    }

    public String getPossellamount() {
        return possellamount;
    }

    public void setPossellamount(String possellamount) {
        this.possellamount = possellamount;
    }

    public String getPossellnumber() {
        return possellnumber;
    }

    public void setPossellnumber(String possellnumber) {
        this.possellnumber = possellnumber;
    }
    
    
    

    public String getPayinpayout() {
        return (payinpayout);
    }
    
    public String getPayinpayoutSenzaFormattazione() {
        return payinpayout;
    }

    public void setPayinpayout(String payinpayout) {
        this.payinpayout = payinpayout;
    }
    
    

    public String getResidentnonresident() {
        return residentnonresident;
    }

    public void setResidentnonresident(String residentnonresident) {
        this.residentnonresident = residentnonresident;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    
    
    

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }
    
    

    public ArrayList getFooterdati() {
        return footerdati;
    }

    public void setFooterdati(ArrayList footerdati) {
        this.footerdati = footerdati;
    }
    
    

    public String getTransvalueresidentbuy() {
        return transvalueresidentbuy;
    }

    public void setTransvalueresidentbuy(String transvalueresidentbuy) {
        this.transvalueresidentbuy = transvalueresidentbuy;
    }

    public String getTransvaluenonresidentbuy() {
        return transvaluenonresidentbuy;
    }

    public void setTransvaluenonresidentbuy(String transvaluenonresidentbuy) {
        this.transvaluenonresidentbuy = transvaluenonresidentbuy;
    }

    public String getCommisionvaluetresidentbuy() {
        return commisionvaluetresidentbuy;
    }

    public void setCommisionvaluetresidentbuy(String commisionvaluetresidentbuy) {
        this.commisionvaluetresidentbuy = commisionvaluetresidentbuy;
    }

    public String getCommisionvaluenonresidentbuy() {
        return commisionvaluenonresidentbuy;
    }

    public void setCommisionvaluenonresidentbuy(String commisionvaluenonresidentbuy) {
        this.commisionvaluenonresidentbuy = commisionvaluenonresidentbuy;
    }

    public String getTransactionnumberresidentbuy() {
        return transactionnumberresidentbuy;
    }

    public void setTransactionnumberresidentbuy(String transactionnumberresidentbuy) {
        this.transactionnumberresidentbuy = transactionnumberresidentbuy;
    }

    public String getTransactionnumbernonresidentbuy() {
        return transactionnumbernonresidentbuy;
    }

    public void setTransactionnumbernonresidentbuy(String transactionnumbernonresidentbuy) {
        this.transactionnumbernonresidentbuy = transactionnumbernonresidentbuy;
    }

    public String getInternetbookingamountyes() {
        return internetbookingamountyes;
    }

    public void setInternetbookingamountyes(String internetbookingamountyes) {
        this.internetbookingamountyes = internetbookingamountyes;
    }

    public String getInternetbookingnumberyes() {
        return internetbookingnumberyes;
    }

    public void setInternetbookingnumberyes(String internetbookingnumberyes) {
        this.internetbookingnumberyes = internetbookingnumberyes;
    }

    public String getTransvalueresidentsell() {
        return transvalueresidentsell;
    }

    public void setTransvalueresidentsell(String transvalueresidentsell) {
        this.transvalueresidentsell = transvalueresidentsell;
    }

    public String getTransvaluenonresidentsell() {
        return transvaluenonresidentsell;
    }

    public void setTransvaluenonresidentsell(String transvaluenonresidentsell) {
        this.transvaluenonresidentsell = transvaluenonresidentsell;
    }

    public String getCommisionvaluetresidentsell() {
        return commisionvaluetresidentsell;
    }

    public void setCommisionvaluetresidentsell(String commisionvaluetresidentsell) {
        this.commisionvaluetresidentsell = commisionvaluetresidentsell;
    }

    public String getCommisionvaluenonresidentsell() {
        return commisionvaluenonresidentsell;
    }

    public void setCommisionvaluenonresidentsell(String commisionvaluenonresidentsell) {
        this.commisionvaluenonresidentsell = commisionvaluenonresidentsell;
    }

    public String getTransactionnumberresidentsell() {
        return transactionnumberresidentsell;
    }

    public void setTransactionnumberresidentsell(String transactionnumberresidentsell) {
        this.transactionnumberresidentsell = transactionnumberresidentsell;
    }

    public String getTransactionnumbernonresidentsell() {
        return transactionnumbernonresidentsell;
    }

    public void setTransactionnumbernonresidentsell(String transactionnumbernonresidentsell) {
        this.transactionnumbernonresidentsell = transactionnumbernonresidentsell;
    }

    public String getInternetbookingamountno() {
        return internetbookingamountno;
    }

    public void setInternetbookingamountno(String internetbookingamountno) {
        this.internetbookingamountno = internetbookingamountno;
    }

    public String getInternetbookingnumberno() {
        return internetbookingnumberno;
    }

    public void setInternetbookingnumberno(String internetbookingnumberno) {
        this.internetbookingnumberno = internetbookingnumberno;
    }
    
    
    
    
    
    
     public ArrayList getDati() {
        return dati;
    }

    public void setDati(ArrayList dati) {
        this.dati = dati;
    }

    public String getId_filiale() {
        return id_filiale;
    }

    public void setId_filiale(String id_filiale) {
        this.id_filiale = id_filiale;
    }

    public String getDe_filiale() {
        return de_filiale;
    }

    public void setDe_filiale(String de_filiale) {
        this.de_filiale = de_filiale;
    }

    public String getTill() {
        return till;
    }

    public void setTill(String till) {
        this.till = till;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getNotr() {
        return notr;
    }

    public void setNotr(String notr) {
        this.notr = notr;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getCur() {
        return cur;
    }

    public void setCur(String cur) {
        this.cur = cur;
    }

    public String getKind() {
        return kind;
    }
    
    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getAmount() {
        return (amount);
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getRate() {
        return (rate);
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTotal() {
        return (total);
    }
    
    public String getTotalSenzaFormattazione() {
        return (total);
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getPerc() {
        return (perc);
    }

    public void setPerc(String perc) {
        this.perc = perc;
    }

    public String getComfree() {
        return (comfree);
    }
    
      public String getComfreeSenzaFormattazione() {
        return (comfree);
    }

    public void setComfree(String comfree) {
        this.comfree = comfree;
    }

   

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getSpread() {
        return (spread);
    }

    public void setSpread(String spread) {
        this.spread = spread;
    }

    public String getFig() {
        return fig;
    }

    public void setFig(String fig) {
        this.fig = fig;
    }

    
}
