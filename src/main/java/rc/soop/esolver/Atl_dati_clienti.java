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
public class Atl_dati_clienti {

    String cod, clientcode, ragsoc1, ragsoc2, address, city, country, clientnumber;

    String zipcode, district, fatelet;

    public Atl_dati_clienti(String cod, String clientcode, String ragsoc1, String ragsoc2, String address, String city, String country, String clientnumber, String zipcode, String district, String fatelet) {
        this.cod = cod;
        this.clientcode = clientcode;
        this.ragsoc1 = ragsoc1;
        this.ragsoc2 = ragsoc2;
        this.address = address;
        this.city = city;
        this.country = country;
        this.clientnumber = clientnumber;
        this.zipcode = zipcode;
        this.district = district;
        this.fatelet = fatelet;
    }

    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getFatelet() {
        return fatelet;
    }

    public void setFatelet(String fatelet) {
        this.fatelet = fatelet;
    }

    public String controlli_dati_clienti() {

        return "OK";
    }

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getClientcode() {
        return clientcode;
    }

    public void setClientcode(String clientcode) {
        this.clientcode = clientcode;
    }

    public String getRagsoc1() {
        return ragsoc1;
    }

    public void setRagsoc1(String ragsoc1) {
        this.ragsoc1 = ragsoc1;
    }

    public String getRagsoc2() {
        return ragsoc2;
    }

    public void setRagsoc2(String ragsoc2) {
        this.ragsoc2 = ragsoc2;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getClientnumber() {
        return clientnumber;
    }

    public void setClientnumber(String clientnumber) {
        this.clientnumber = clientnumber;
    }

}
