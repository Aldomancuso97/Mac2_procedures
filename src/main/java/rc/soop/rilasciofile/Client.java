package rc.soop.rilasciofile;

import java.util.HashMap;

public class Client {

    String code;
    String cognome;
    String nome;
    String sesso;
    String codfisc;
    String nazione;
    String citta;
    String indirizzo;
    String cap;
    String provincia;
    String citta_nascita;
    String provincia_nascita;
    String nazione_nascita;
    String dt_nascita;
    String tipo_documento;
    String pep;
    String datatr;
    
    Client_CZ repceca;
    
    public Client() {
        this.repceca = null;
        this.cognome = "";
        this.nome = "";
        this.codfisc = "";
        this.code = "---";
        this.nazione = "";
        this.nazione_nascita = "";
        this.indirizzo = "";
        this.citta = "";
        this.sesso = "";
        
    }
    
    public Client_CZ getRepceca() {
        return repceca;
    }

    public void setRepceca(Client_CZ repceca) {
        this.repceca = repceca;
    }
    
    public String getDatatr() {
        return datatr;
    }

    public void setDatatr(String datatr) {
        this.datatr = datatr;
    }

    public String getPep() {
        return pep;
    }

    public void setPep(String pep) {
        this.pep = pep;
    }

    public String getCode() {
        return this.code;
    }

    String numero_documento;

    public void setCode(String code) {
        this.code = code;
    }

    String dt_rilascio_documento;

    public String getCognome() {
        return this.cognome;
    }

    String dt_scadenza_documento;
    String rilasciato_da_documento;
    String luogo_rilascio_documento;

    public void setCognome(String cognome) {
        this.cognome = cognome;
    }

    String email;

    public String getNome() {
        return this.nome;
    }

    String telefono;

    public void setNome(String nome) {
        this.nome = nome;
    }

    String perc_sell;

    public String getSesso() {
        return this.sesso;
    }

    String perc_buy;

    public void setSesso(String sesso) {
        this.sesso = sesso;
    }

    String timestamp;

    String codicemodifica;
    String tipomodifica;
    String usermodifica;
    String datemodifica;

    public String getTipomodifica() {
        return tipomodifica;
    }

    public void setTipomodifica(String tipomodifica) {
        this.tipomodifica = tipomodifica;
    }

    public String getUsermodifica() {
        return usermodifica;
    }

    public void setUsermodifica(String usermodifica) {
        this.usermodifica = usermodifica;
    }

    public String getDatemodifica() {
        return datemodifica;
    }

    public void setDatemodifica(String datemodifica) {
        this.datemodifica = datemodifica;
    }

    public String getCodicemodifica() {
        return codicemodifica;
    }

    public void setCodicemodifica(String codicemodifica) {
        this.codicemodifica = codicemodifica;
    }

    public String getCodfisc() {
        return this.codfisc;
    }

    public void setCodfisc(String codfisc) {
        this.codfisc = codfisc;
    }

    public String getNazione() {
        return this.nazione;
    }

    public void setNazione(String nazione) {
        this.nazione = nazione;
    }

    public String getCitta() {
        return this.citta;
    }

    public void setCitta(String citta) {
        this.citta = citta;
    }

    public String getIndirizzo() {
        return this.indirizzo;
    }

    public void setIndirizzo(String indirizzo) {
        this.indirizzo = indirizzo;
    }

    public String getCap() {
        return this.cap;
    }

    public void setCap(String cap) {
        this.cap = cap;
    }

    public String getProvincia() {
        return this.provincia;
    }

    public void setProvincia(String provincia) {
        this.provincia = provincia;
    }

    public String getCitta_nascita() {
        return this.citta_nascita;
    }

    public void setCitta_nascita(String citta_nascita) {
        this.citta_nascita = citta_nascita;
    }

    public String getProvincia_nascita() {
        return this.provincia_nascita;
    }

    public void setProvincia_nascita(String provincia_nascita) {
        this.provincia_nascita = provincia_nascita;
    }

    public String getNazione_nascita() {
        return this.nazione_nascita;
    }

    public void setNazione_nascita(String nazione_nascita) {
        this.nazione_nascita = nazione_nascita;
    }

    public String getDt_nascita() {
        return this.dt_nascita;
    }

    public void setDt_nascita(String dt_nascita) {
        this.dt_nascita = dt_nascita;
    }

    public String getTipo_documento() {
        return this.tipo_documento;
    }

    public void setTipo_documento(String tipo_documento) {
        this.tipo_documento = tipo_documento;
    }

    public String getNumero_documento() {
        return this.numero_documento;
    }

    public void setNumero_documento(String numero_documento) {
        this.numero_documento = numero_documento;
    }

    public String getDt_rilascio_documento() {
        return this.dt_rilascio_documento;
    }

    public void setDt_rilascio_documento(String dt_rilascio_documento) {
        this.dt_rilascio_documento = dt_rilascio_documento;
    }

    public String getDt_scadenza_documento() {
        return this.dt_scadenza_documento;
    }

    public void setDt_scadenza_documento(String dt_scadenza_documento) {
        this.dt_scadenza_documento = dt_scadenza_documento;
    }

    public String getRilasciato_da_documento() {
        return this.rilasciato_da_documento;
    }

    public void setRilasciato_da_documento(String rilasciato_da_documento) {
        this.rilasciato_da_documento = rilasciato_da_documento;
    }

    public String getLuogo_rilascio_documento() {
        return this.luogo_rilascio_documento;
    }

    public void setLuogo_rilascio_documento(String luogo_rilascio_documento) {
        this.luogo_rilascio_documento = luogo_rilascio_documento;
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getTelefono() {
        return this.telefono;
    }

    public void setTelefono(String telefono) {
        this.telefono = telefono;
    }

    public String getPerc_sell() {
        return this.perc_sell;
    }

    public void setPerc_sell(String perc_sell) {
        this.perc_sell = perc_sell;
    }

    public String getPerc_buy() {
        return this.perc_buy;
    }

    public void setPerc_buy(String perc_buy) {
        this.perc_buy = perc_buy;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    
    public HashMap<String, String[]> modifiche;

    public HashMap<String, String[]> getModifiche() {
        return modifiche;
    }

    public void setModifiche(HashMap<String, String[]> modifiche) {
        this.modifiche = modifiche;
    }
    
    
    
    
    
    
}