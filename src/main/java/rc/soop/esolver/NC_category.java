package rc.soop.esolver;

public class NC_category {

    String gruppo_nc;
    String de_gruppo_nc;
    String fg_tipo_transazione_nc;
    String ip_prezzo_nc;
    String annullato;
    String conto_coge_01;
    String conto_coge_02;
    String conto_coge_03;
    String de_scontrino;
    String de_riga;
    String pc_iva_corrispettivo;
    String ticket_fee;
    String max_ticket;
    String ticket_fee_type;
    String ticket_enabled;
    String timestamp;
    String filiale;

    String int_corrisp;
    String int_iva;
    String int_code;

    String fg_registratore;
    String department;

    public String formatStatus(String annullato) {
        if (annullato == null) {
            annullato = "";
        }
        if (annullato.equals("0")) {
            return "<div class='font-green-jungle'>Enabled <i class='fa fa-check'></i></div>";
        }
        if (annullato.equals("1")) {
            return "<div class='font-red'>Disabled <i class='fa fa-close'></i></div>";
        }
        return annullato;
    }

    public String formatStatus_cru(String annullato) {
        if (annullato == null) {
            annullato = "";
        }
        if (annullato.equals("0")) {
            return "Enabled ";
        }
        if (annullato.equals("1")) {
            return "Disabled ";
        }
        return annullato;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getFg_registratore() {
        return fg_registratore;
    }

    public void setFg_registratore(String fg_registratore) {
        if (fg_registratore.equals("1")) {
            this.department = get_department_NC(this.gruppo_nc);
        } else {
            this.department = "01";
        }

        this.fg_registratore = fg_registratore;
    }

    public String getInt_corrisp() {
        return int_corrisp;
    }

    public void setInt_corrisp(String int_corrisp) {
        this.int_corrisp = int_corrisp;
    }

    public String getInt_iva() {
        return int_iva;
    }

    public void setInt_iva(String int_iva) {
        this.int_iva = int_iva;
    }

    public String getInt_code() {
        return int_code;
    }

    public void setInt_code(String int_code) {
        this.int_code = int_code;
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getTicket_enabled() {
        return ticket_enabled;
    }

    public void setTicket_enabled(String ticket_enabled) {
        this.ticket_enabled = ticket_enabled;
    }

    public String getTicket_fee_type() {
        return ticket_fee_type;
    }

    public void setTicket_fee_type(String ticket_fee_type) {
        this.ticket_fee_type = ticket_fee_type;
    }

    public String getGruppo_nc() {
        return this.gruppo_nc;
    }

    public void setGruppo_nc(String gruppo_nc) {
        this.gruppo_nc = gruppo_nc;
    }

    public String getDe_gruppo_nc() {
        return this.de_gruppo_nc;
    }

    public void setDe_gruppo_nc(String de_gruppo_nc) {
        this.de_gruppo_nc = de_gruppo_nc;
    }

    public String getFg_tipo_transazione_nc() {
        return this.fg_tipo_transazione_nc;
    }

    public void setFg_tipo_transazione_nc(String fg_tipo_transazione_nc) {
        this.fg_tipo_transazione_nc = fg_tipo_transazione_nc;
    }

    public String getIp_prezzo_nc() {
        return this.ip_prezzo_nc;
    }

    public void setIp_prezzo_nc(String ip_prezzo_nc) {
        this.ip_prezzo_nc = ip_prezzo_nc;
    }

    public String getAnnullato() {
        return this.annullato;
    }

    public void setAnnullato(String annullato) {
        this.annullato = annullato;
    }

    public String getConto_coge_01() {
        return this.conto_coge_01;
    }

    public void setConto_coge_01(String conto_coge_01) {
        this.conto_coge_01 = conto_coge_01;
    }

    public String getConto_coge_02() {
        return this.conto_coge_02;
    }

    public void setConto_coge_02(String conto_coge_02) {
        this.conto_coge_02 = conto_coge_02;
    }

    public String getConto_coge_03() {
        return this.conto_coge_03;
    }

    public void setConto_coge_03(String conto_coge_03) {
        this.conto_coge_03 = conto_coge_03;
    }

    public String getDe_scontrino() {
        return this.de_scontrino;
    }

    public void setDe_scontrino(String de_scontrino) {
        this.de_scontrino = de_scontrino;
    }

    public String getDe_riga() {
        return this.de_riga;
    }

    public void setDe_riga(String de_riga) {
        this.de_riga = de_riga;
    }

    public String getPc_iva_corrispettivo() {
        return this.pc_iva_corrispettivo;
    }

    public void setPc_iva_corrispettivo(String pc_iva_corrispettivo) {
        this.pc_iva_corrispettivo = pc_iva_corrispettivo;
    }

    public String getTicket_fee() {
        return this.ticket_fee;
    }

    public void setTicket_fee(String ticket_fee) {
        this.ticket_fee = ticket_fee;
    }

    public String getMax_ticket() {
        return this.max_ticket;
    }

    public void setMax_ticket(String max_ticket) {
        this.max_ticket = max_ticket;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    
    public static String get_department_NC(String cod) {
        Db_Master dbl = new Db_Master();
        String dep = dbl.get_department_NC(cod);
        dbl.closeDB();
        return dep;
    }
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\NC_category.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */
