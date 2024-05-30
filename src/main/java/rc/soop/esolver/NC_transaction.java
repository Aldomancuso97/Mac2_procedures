package rc.soop.esolver;

public class NC_transaction {

    String cod;
    String id;
    String filiale;
    String gruppo_nc;
    String causale_nc;
    String valuta;
    String supporto;
    String pos;
    String user;
    String till;
    String id_open_till;
    String data;
    String total;
    String commissione;
    String netto;
    String prezzo;
    String quantita;
    String fg_inout;
    String ricevuta;
    String mtcn;

    String del_fg;
    String del_dt;
    String del_user;
    String del_motiv;
    String fg_tipo_transazione_nc;

    String fg_dogana;
    String ass_idcode;
    String ass_startdate;
    String ass_enddate;
    String cl_cognome;
    String cl_nome;

    String cl_indirizzo;

    String posnum;

    String cl_citta;
    String cl_nazione;

    String percentiva;
    String bonus;
    
    String ch_transaction;
    
    String docrico;
    
    public NC_transaction() {
        this.valuta = "EUR";
        this.supporto = "01";
        this.pos = "000";
        this.total = "0.00";
        this.commissione = "0.00";
        this.netto = "0.00";
        this.prezzo = "0.00";
        this.quantita = "0.00";
        this.fg_inout = "1";
        this.ricevuta = "-";
        this.mtcn = "-";
        this.del_fg = "0";
        this.del_user = "-";
        this.del_motiv = "-";
        this.fg_tipo_transazione_nc = "0";
        this.fg_dogana = "00";
        this.ass_idcode = "-";
        this.ass_startdate = "-";
        this.ass_enddate = "-";
        this.cl_cognome = "-";
        this.cl_nome = "-";
        this.cl_indirizzo = "-";
        this.cl_citta = "-";
        this.cl_nazione = "-";
        this.cl_cap = "-";
        this.cl_provincia = "-";
        this.cl_email = "-";
        this.cl_telefono = "-";
        this.note = "-";
        this.ti_diritti = "0.00";
        this.ti_ticket_fee = "0.00";
        this.posnum = "-";
        this.percentiva="-";
        this.bonus="0";
        this.ch_transaction="-";
        this.docrico = "-";
    }

//  public NC_transaction(String filiale)
//  {
//    this.filiale = filiale;
//    this.cod = Utility.generaIdMAC(this.filiale);
//    this.id = setId_db();
//    this.gruppo_nc = "-";
//    this.causale_nc = "-";
//    this.valuta = "EUR";
//    this.supporto = "01";
//    this.pos = "000";
//    this.user = "-";
//    this.till = "-";
//    this.id_open_till = "-";
//    this.total = "0.00";
//    this.commissione = "0.00";
//    this.netto = "0.00";
//    this.prezzo = "0.00";
//    this.quantita = "0.00";
//    this.fg_inout = "1";
//    this.ricevuta = "-";
//    this.mtcn = "-";
//    this.del_fg = "0";
//    this.del_user = "-";
//    this.del_motiv = "-";
//    this.fg_tipo_transazione_nc = "0";
//    this.fg_dogana = "00";
//    this.ass_idcode = "-";
//    this.ass_startdate = "-";
//    this.ass_enddate = "-";
//    this.cl_cognome = "-";
//    this.cl_nome = "-";
//    this.cl_indirizzo = "-";
//    this.cl_citta = "-";
//    this.cl_nazione = "-";
//    this.cl_cap = "-";
//    this.cl_provincia = "-";
//    this.cl_email = "-";
//    this.cl_telefono = "-";
//    this.note = "-";
//    this.ti_diritti = "0.00";
//    this.ti_ticket_fee = "0.00";
//  }
//  
//  
    public NC_transaction(String cod, String id, String filiale, String gruppo_nc, String causale_nc,
            String valuta, String supporto, String pos, String user, String till, String data,
            String total, String commissione, String netto, String prezzo, String quantita, String fg_inout, String ricevuta,
            String mtcn, String del_fg, String del_dt, String del_user, String del_motiv, String fg_tipo_transazione_nc,
            String fg_dogana, String ass_idcode, String ass_startdate, String ass_enddate, String cl_cognome, String cl_nome,
            String cl_indirizzo, String cl_citta, String cl_nazione, String cl_cap, String cl_provincia, String cl_email,
            String cl_telefono, String note, String ti_diritti, String ti_ticket_fee, String id_open_till, String posnum, String percentiva,String bonus,String ch_transaction,String docrico) {
        this.cod = cod;
        this.id = id;
        this.filiale = filiale;
        this.gruppo_nc = gruppo_nc;
        this.causale_nc = causale_nc;
        this.valuta = valuta;
        this.supporto = supporto;
        this.pos = pos;
        this.user = user;
        this.till = till;
        this.id_open_till = id_open_till;
        this.data = data;
        this.total = total;
        this.commissione = commissione;
        this.netto = netto;
        this.prezzo = prezzo;
        this.quantita = quantita;
        this.fg_inout = fg_inout;
        this.ricevuta = ricevuta;
        this.mtcn = mtcn;
        this.del_fg = del_fg;
        this.del_dt = del_dt;
        this.del_user = del_user;
        this.del_motiv = del_motiv;
        this.fg_tipo_transazione_nc = fg_tipo_transazione_nc;
        this.fg_dogana = fg_dogana;
        this.ass_idcode = ass_idcode;
        this.ass_startdate = ass_startdate;
        this.ass_enddate = ass_enddate;
        this.cl_cognome = cl_cognome;
        this.cl_nome = cl_nome;
        this.cl_indirizzo = cl_indirizzo;
        this.cl_citta = cl_citta;
        this.cl_nazione = cl_nazione;
        this.cl_cap = cl_cap;
        this.cl_provincia = cl_provincia;
        this.cl_email = cl_email;
        this.cl_telefono = cl_telefono;
        this.note = note;
        this.ti_diritti = ti_diritti;
        this.ti_ticket_fee = ti_ticket_fee;
        this.posnum = posnum;
        this.percentiva = percentiva;
        this.bonus = bonus;
        this.ch_transaction = ch_transaction;
        this.docrico = docrico;
    }

    public String getDocrico() {
        return docrico;
    }

    public void setDocrico(String docrico) {
        this.docrico = docrico;
    }

    public String getCh_transaction() {
        return ch_transaction;
    }

    public void setCh_transaction(String ch_transaction) {
        this.ch_transaction = ch_transaction;
    }

    public String getBonus() {
        return bonus;
    }

    public void setBonus(String bonus) {
        this.bonus = bonus;
    }
    
    public String getPercentiva() {
        return percentiva;
    }

//  private String setId_db()
//  {
//    Db_Master db = new Db_Master();
//    int value = db.getlastId_nc_trans();
//    db.closeDB();
//    if (value > -1) {
//      value++;
//      return Utility.fillLeftInt(value, 15, "0");
//    }
//    return "ERROR";
//  }
    
    
    public String formatStatus(String del_fg) {
        if (del_fg != null) {
            if (del_fg.equals("0")) {
                return "<div class='font-green-jungle'>Active <i class='fa fa-check'></i></div>";
            }
            if (del_fg.equals("1")) {
                return "<div class='font-red'>Deleted <i class='fa fa-remove'></i></div>";
            }
        }
        return "-";
    }
    public String formatStatus_cru(String del_fg) {
        if (del_fg != null) {
            if (del_fg.equals("0")) {
                return "Active";
            }
            if (del_fg.equals("1")) {
                return "Deleted";
            }
        }
        return "-";
    }
    public void setPercentiva(String percentiva) {
        this.percentiva = percentiva;
    }

    public String getPosnum() {
        return posnum;
    }

    public void setPosnum(String posnum) {
        this.posnum = posnum;
    }

    public String getId_open_till() {
        return this.id_open_till;
    }

    String cl_cap;

    public void setId_open_till(String id_open_till) {
        this.id_open_till = id_open_till;
    }

    String cl_provincia;

    public String getCod() {
        return this.cod;
    }

    String cl_email;
    String cl_telefono;
    String note;

    public void setCod(String cod) {
        this.cod = cod;
    }

    String ti_diritti;

    public String getId() {
        return this.id;
    }

    String ti_ticket_fee;

    public void setId(String id) {
        this.id = id;
    }

    public String getFiliale() {
        return this.filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getGruppo_nc() {
        return this.gruppo_nc;
    }

    public void setGruppo_nc(String gruppo_nc) {
        this.gruppo_nc = gruppo_nc;
    }

    public String getCausale_nc() {
        return this.causale_nc;
    }

    public void setCausale_nc(String causale_nc) {
        this.causale_nc = causale_nc;
    }

    public String getValuta() {
        return this.valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getSupporto() {
        return this.supporto;
    }

    public void setSupporto(String supporto) {
        this.supporto = supporto;
    }

    public String getPos() {
        return this.pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTill() {
        return this.till;
    }

    public void setTill(String till) {
        this.till = till;
    }

    public String getData() {
        return this.data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getTotal() {
        return this.total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getCommissione() {
        return this.commissione;
    }

    public void setCommissione(String commissione) {
        this.commissione = commissione;
    }

    public String getNetto() {
        return this.netto;
    }

    public void setNetto(String netto) {
        this.netto = netto;
    }

    public String getPrezzo() {
        return this.prezzo;
    }

    public void setPrezzo(String prezzo) {
        this.prezzo = prezzo;
    }

    public String getQuantita() {
        return this.quantita;
    }

    public void setQuantita(String quantita) {
        this.quantita = quantita;
    }

    public String getFg_inout() {
        return this.fg_inout;
    }

    public void setFg_inout(String fg_inout) {
        this.fg_inout = fg_inout;
    }

    public String getRicevuta() {
        return this.ricevuta;
    }

    public void setRicevuta(String ricevuta) {
        this.ricevuta = ricevuta;
    }

    public String getMtcn() {
        return this.mtcn;
    }

    public void setMtcn(String mtcn) {
        this.mtcn = mtcn;
    }

    public String getDel_fg() {
        return this.del_fg;
    }

    public void setDel_fg(String del_fg) {
        this.del_fg = del_fg;
    }

    public String getDel_dt() {
        return this.del_dt;
    }

    public void setDel_dt(String del_dt) {
        this.del_dt = del_dt;
    }

    public String getDel_user() {
        return this.del_user;
    }

    public void setDel_user(String del_user) {
        this.del_user = del_user;
    }

    public String getDel_motiv() {
        return this.del_motiv;
    }

    public void setDel_motiv(String del_motiv) {
        this.del_motiv = del_motiv;
    }

    public String getFg_tipo_transazione_nc() {
        return this.fg_tipo_transazione_nc;
    }

    public void setFg_tipo_transazione_nc(String fg_tipo_transazione_nc) {
        this.fg_tipo_transazione_nc = fg_tipo_transazione_nc;
    }

    public String getFg_dogana() {
        return this.fg_dogana;
    }

    public void setFg_dogana(String fg_dogana) {
        this.fg_dogana = fg_dogana;
    }

    public String getAss_idcode() {
        return this.ass_idcode;
    }

    public void setAss_idcode(String ass_idcode) {
        this.ass_idcode = ass_idcode;
    }

    public String getAss_startdate() {
        return this.ass_startdate;
    }

    public void setAss_startdate(String ass_startdate) {
        this.ass_startdate = ass_startdate;
    }

    public String getAss_enddate() {
        return this.ass_enddate;
    }

    public void setAss_enddate(String ass_enddate) {
        this.ass_enddate = ass_enddate;
    }

    public String getCl_cognome() {
        return this.cl_cognome;
    }

    public void setCl_cognome(String cl_cognome) {
        this.cl_cognome = cl_cognome;
    }

    public String getCl_nome() {
        return this.cl_nome;
    }

    public void setCl_nome(String cl_nome) {
        this.cl_nome = cl_nome;
    }

    public String getCl_indirizzo() {
        return this.cl_indirizzo;
    }

    public void setCl_indirizzo(String cl_indirizzo) {
        this.cl_indirizzo = cl_indirizzo;
    }

    public String getCl_citta() {
        return this.cl_citta;
    }

    public void setCl_citta(String cl_citta) {
        this.cl_citta = cl_citta;
    }

    public String getCl_nazione() {
        return this.cl_nazione;
    }

    public void setCl_nazione(String cl_nazione) {
        this.cl_nazione = cl_nazione;
    }

    public String getCl_cap() {
        return this.cl_cap;
    }

    public void setCl_cap(String cl_cap) {
        this.cl_cap = cl_cap;
    }

    public String getCl_provincia() {
        return this.cl_provincia;
    }

    public void setCl_provincia(String cl_provincia) {
        this.cl_provincia = cl_provincia;
    }

    public String getCl_email() {
        return this.cl_email;
    }

    public void setCl_email(String cl_email) {
        this.cl_email = cl_email;
    }

    public String getCl_telefono() {
        return this.cl_telefono;
    }

    public void setCl_telefono(String cl_telefono) {
        this.cl_telefono = cl_telefono;
    }

    public String getNote() {
        return this.note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getTi_diritti() {
        return this.ti_diritti;
    }

    public void setTi_diritti(String ti_diritti) {
        this.ti_diritti = ti_diritti;
    }

    public String getTi_ticket_fee() {
        return this.ti_ticket_fee;
    }

    public void setTi_ticket_fee(String ti_ticket_fee) {
        this.ti_ticket_fee = ti_ticket_fee;
    }
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\NC_transaction.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */
