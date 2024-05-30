/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import static rc.soop.crm.Action.generaId;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 *
 * @author rcosco
 */
public class Preauth {

    String codice, prenotazione, request, request_date, response_code, response, response_message, response_date, stato;

    public Preauth(String prenotazione) {
        this.codice = generaId(50);
        this.prenotazione = prenotazione;
        this.request = "NO REQUEST";
        this.request_date = "1901-01-01 00:00;00";
        this.response_code = "-";
        this.response = "NO RESPONSE";
        this.response_message = "NO RESPONSE";
        this.response_date = "1901-01-01 00:00;00";
        this.stato = "WA";
    }

    public Preauth(String codice, String prenotazione, String request, String request_date, String response_code, String response, String response_message, String response_date, String stato) {
        this.codice = codice;
        this.prenotazione = prenotazione;
        this.request = request;
        this.request_date = request_date;
        this.response_code = response_code;
        this.response = response;
        this.response_message = response_message;
        this.response_date = response_date;
        this.stato = stato;
    }

    public String getResponse_code() {
        return response_code;
    }

    public void setResponse_code(String response_code) {
        this.response_code = response_code;
    }

    public String getResponse_message() {
        return response_message;
    }

    public void setResponse_message(String response_message) {
        this.response_message = response_message;
    }

    public String getCodice() {
        return codice;
    }

    public void setCodice(String codice) {
        this.codice = codice;
    }

    public String getPrenotazione() {
        return prenotazione;
    }

    public void setPrenotazione(String prenotazione) {
        this.prenotazione = prenotazione;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getRequest_date() {
        return request_date;
    }

    public void setRequest_date(String request_date) {
        this.request_date = request_date;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getResponse_date() {
        return response_date;
    }

    public void setResponse_date(String response_date) {
        this.response_date = response_date;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
