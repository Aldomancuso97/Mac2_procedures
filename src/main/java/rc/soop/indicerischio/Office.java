package rc.soop.indicerischio;


public class Office {

    String cod;
    String de_office;
    String add_city;
    String add_cap;
    String add_via;
    String vat;
    String reg_impr;
    String rea;
    String changetype;
    String decimalround;
    String url_bl;
    String txt_ricev_1;
    String txt_ricev_2;
    String txt_alert_threshold_1;
    String txt_alert_threshold_2;
    String txt_ricev_bb;
    String txt_nopep;
    
    String scadenza_bb;
    String showagency;
    String minutes;
    String kyc_mesi;
    String kyc_soglia;
    
    String risk_days;
    String risk_ntr;
    String risk_soglia;

//    public String getTxt_nopep() {
//        return HtmlEncoder.getBase64HTML(this.txt_nopep);
//    }
    public String n_getTxt_nopep() {
        return this.txt_nopep;
    }

    public void setTxt_nopep(String txt_nopep) {
        this.txt_nopep = txt_nopep;
    }

    public String getRisk_days() {
        return risk_days;
    }

    public void setRisk_days(String risk_days) {
        this.risk_days = risk_days;
    }

    public String getRisk_ntr() {
        return risk_ntr;
    }

    public void setRisk_ntr(String risk_ntr) {
        this.risk_ntr = risk_ntr;
    }

    public String getRisk_soglia() {
        return risk_soglia;
    }

    public void setRisk_soglia(String risk_soglia) {
        this.risk_soglia = risk_soglia;
    }
    
    public String getChangeOperator() {
        if (this.changetype.equals("*")) {
            return "/";
        }
        return "*";
    }

    public String getKyc_mesi() {
        return kyc_mesi;
    }

    public void setKyc_mesi(String kyc_mesi) {
        this.kyc_mesi = kyc_mesi;
    }

    public String getKyc_soglia() {
        return kyc_soglia;
    }

    public void setKyc_soglia(String kyc_soglia) {
        this.kyc_soglia = kyc_soglia;
    }

    public String getMinutes() {
        return minutes;
    }

    public void setMinutes(String minutes) {
        this.minutes = minutes;
    }

    public String getShowagency() {
        return this.showagency;
    }

    public void setShowagency(String showagency) {
        this.showagency = showagency;
    }

//    public String getTxt_ricev_bb() {
//        return HtmlEncoder.getBase64HTML(this.txt_ricev_bb);
//    }
    public String n_getTxt_ricev_bb() {
        return this.txt_ricev_bb;
    }

    public void setTxt_ricev_bb(String txt_ricev_bb) {
        this.txt_ricev_bb = txt_ricev_bb;
    }

    public String getScadenza_bb() {
        return this.scadenza_bb;
    }

    public void setScadenza_bb(String scadenza_bb) {
        this.scadenza_bb = scadenza_bb;
    }

    public String getCod() {
        return this.cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getDe_office() {
        return this.de_office;
    }

    public void setDe_office(String de_office) {
        this.de_office = de_office;
    }

    public String getAdd_city() {
        return this.add_city;
    }

    public void setAdd_city(String add_city) {
        this.add_city = add_city;
    }

    public String getAdd_cap() {
        return this.add_cap;
    }

    public void setAdd_cap(String add_cap) {
        this.add_cap = add_cap;
    }

    public String getAdd_via() {
        return this.add_via;
    }

    public void setAdd_via(String add_via) {
        this.add_via = add_via;
    }

    public String getVat() {
        return this.vat;
    }

    public void setVat(String vat) {
        this.vat = vat;
    }

    public String getReg_impr() {
        return this.reg_impr;
    }

    public void setReg_impr(String reg_impr) {
        this.reg_impr = reg_impr;
    }

    public String getRea() {
        return this.rea;
    }

    public void setRea(String rea) {
        this.rea = rea;
    }

    public String getChangetype() {
        return this.changetype;
    }

    public void setChangetype(String changetype) {
        this.changetype = changetype;
    }

    public String getDecimalround() {
        return this.decimalround;
    }

    public void setDecimalround(String decimalround) {
        this.decimalround = decimalround;
    }

    public String getUrl_bl() {
        return this.url_bl;
    }

    public void setUrl_bl(String url_bl) {
        this.url_bl = url_bl;
    }

//    public String getTxt_ricev_1() {
//        return HtmlEncoder.getBase64HTML(this.txt_ricev_1);
//    }
    
    public String n_getTxt_ricev_1() {
        return this.txt_ricev_1;
    }
    
    public void setTxt_ricev_1(String txt_ricev_1) {
        this.txt_ricev_1 = txt_ricev_1;
    }

//    public String getTxt_ricev_2() {
//        return HtmlEncoder.getBase64HTML(this.txt_ricev_2);
//    }

    public String n_getTxt_ricev_2() {
        return this.txt_ricev_2;
    }

    public void setTxt_ricev_2(String txt_ricev_2) {
        this.txt_ricev_2 = txt_ricev_2;
    }

//    public String getTxt_alert_threshold_1() {
//        return HtmlEncoder.getBase64HTML(this.txt_alert_threshold_1);
//    }
    public String n_getTxt_alert_threshold_1() {
        return this.txt_alert_threshold_1;
    }

    public void setTxt_alert_threshold_1(String txt_alert_threshold_1) {
        this.txt_alert_threshold_1 = txt_alert_threshold_1;
    }

//    public String getTxt_alert_threshold_2() {
//        return HtmlEncoder.getBase64HTML(this.txt_alert_threshold_2);
//    }
    public String n_getTxt_alert_threshold_2() {
        return this.txt_alert_threshold_2;
    }

    public void setTxt_alert_threshold_2(String txt_alert_threshold_2) {
        this.txt_alert_threshold_2 = txt_alert_threshold_2;
    }
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\Office.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */
