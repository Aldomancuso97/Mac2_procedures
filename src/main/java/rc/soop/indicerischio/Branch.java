package rc.soop.indicerischio;

import java.util.HashMap;

public class Branch {

    String filiale;

    String cod;
    String de_branch;
    String add_city;
    String add_cap;
    String add_via;
    String fg_persgiur;
    String prov_raccval;
    String fg_annullato;
    String da_annull;
    String g01;
    String g02;
    String g03;
    String fg_modrate;
    String fg_agency;
    
    String fg_crm;
    
    String olta_user;
    String olta_pass;
    
    String pay_nomeazienda,pay_idazienda,pay_skin,pay_user,pay_password,pay_token,pay_terminale;
    
    String fg_pad;
    
    //evo
    String dt_start;
    String max_ass;
    String target;
    
    String brgr_01,brgr_02,brgr_03,brgr_04,brgr_05,brgr_06,brgr_07,brgr_08,brgr_09,brgr_10;
    HashMap<String, String> listagruppi = new HashMap<>();
    
    
    public HashMap<String, String> getListagruppi() {
        return listagruppi;
    }

    public void setListagruppi() {
        this.listagruppi = new HashMap<>();
        this.listagruppi.put("01", this.brgr_01);
        this.listagruppi.put("02", this.brgr_02);
        this.listagruppi.put("03", this.brgr_03);
        this.listagruppi.put("04", this.brgr_04);
        this.listagruppi.put("05", this.brgr_05);
        this.listagruppi.put("06", this.brgr_06);
        this.listagruppi.put("07", this.brgr_07);
        this.listagruppi.put("08", this.brgr_08);
        this.listagruppi.put("09", this.brgr_09);
        this.listagruppi.put("10", this.brgr_10);
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
    
    public String getBrgr_01() {
        return brgr_01;
    }

    public void setBrgr_01(String brgr_01) {
        this.brgr_01 = brgr_01;
    }

    public String getBrgr_02() {
        return brgr_02;
    }

    public void setBrgr_02(String brgr_02) {
        this.brgr_02 = brgr_02;
    }

    public String getBrgr_03() {
        return brgr_03;
    }

    public void setBrgr_03(String brgr_03) {
        this.brgr_03 = brgr_03;
    }

    public String getBrgr_04() {
        return brgr_04;
    }

    public void setBrgr_04(String brgr_04) {
        this.brgr_04 = brgr_04;
    }

    public String getBrgr_05() {
        return brgr_05;
    }

    public void setBrgr_05(String brgr_05) {
        this.brgr_05 = brgr_05;
    }

    public String getBrgr_06() {
        return brgr_06;
    }

    public void setBrgr_06(String brgr_06) {
        this.brgr_06 = brgr_06;
    }

    public String getBrgr_07() {
        return brgr_07;
    }

    public void setBrgr_07(String brgr_07) {
        this.brgr_07 = brgr_07;
    }

    public String getBrgr_08() {
        return brgr_08;
    }

    public void setBrgr_08(String brgr_08) {
        this.brgr_08 = brgr_08;
    }

    public String getBrgr_09() {
        return brgr_09;
    }

    public void setBrgr_09(String brgr_09) {
        this.brgr_09 = brgr_09;
    }

    public String getBrgr_10() {
        return brgr_10;
    }

    public void setBrgr_10(String brgr_10) {
        this.brgr_10 = brgr_10;
    }
    
    
    
    
    public String getMax_ass() {
        return max_ass;
    }

    public void setMax_ass(String max_ass) {
        this.max_ass = max_ass;
    }
    
    public String getDt_start() {
        return dt_start;
    }

    public void setDt_start(String dt_start) {
        this.dt_start = dt_start;
    }
    
    public String getFg_pad() {
        return fg_pad;
    }

    public void setFg_pad(String fg_pad) {
        this.fg_pad = fg_pad;
    }
    
    public String getFg_agency() {
        return fg_agency;
    }

    public void setFg_agency(String fg_agency) {
        this.fg_agency = fg_agency;
    }

    public String getPay_nomeazienda() {
        return pay_nomeazienda;
    }

    public void setPay_nomeazienda(String pay_nomeazienda) {
        this.pay_nomeazienda = pay_nomeazienda;
    }

    public String getPay_idazienda() {
        return pay_idazienda;
    }

    public void setPay_idazienda(String pay_idazienda) {
        this.pay_idazienda = pay_idazienda;
    }

    public String getPay_skin() {
        return pay_skin;
    }

    public void setPay_skin(String pay_skin) {
        this.pay_skin = pay_skin;
    }

    public String getPay_user() {
        return pay_user;
    }

    public void setPay_user(String pay_user) {
        this.pay_user = pay_user;
    }

    public String getPay_password() {
        return pay_password;
    }

    public void setPay_password(String pay_password) {
        this.pay_password = pay_password;
    }

    public String getPay_token() {
        return pay_token;
    }

    public void setPay_token(String pay_token) {
        this.pay_token = pay_token;
    }

    public String getPay_terminale() {
        return pay_terminale;
    }

    public void setPay_terminale(String pay_terminale) {
        this.pay_terminale = pay_terminale;
    }
    
    
    public String getOlta_user() {
        return olta_user;
    }

    public void setOlta_user(String olta_user) {
        this.olta_user = olta_user;
    }

    public String getOlta_pass() {
        return olta_pass;
    }

    public void setOlta_pass(String olta_pass) {
        this.olta_pass = olta_pass;
    }
    
    public String getFg_crm() {
        return fg_crm;
    }

    public void setFg_crm(String fg_crm) {
        this.fg_crm = fg_crm;
    }

    public String getFg_modrate() {
        return fg_modrate;
    }

    public void setFg_modrate(String fg_modrate) {
        this.fg_modrate = fg_modrate;
    }
    
    public String getG01() {
        return this.g01;
    }

    public void setG01(String g01) {
        this.g01 = g01;
    }

    public String getG02() {
        return this.g02;
    }

    public void setG02(String g02) {
        this.g02 = g02;
    }

    public String getG03() {
        return this.g03;
    }

    public void setG03(String g03) {
        this.g03 = g03;
    }

    public String getFiliale() {
        return this.filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getCod() {
        return this.cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getDe_branch() {
        return this.de_branch;
    }

    public void setDe_branch(String de_branch) {
        this.de_branch = de_branch;
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

    public String getFg_persgiur() {
        return this.fg_persgiur;
    }

    public void setFg_persgiur(String fg_persgiur) {
        this.fg_persgiur = fg_persgiur;
    }

    public String getProv_raccval() {
        return this.prov_raccval;
    }

    public void setProv_raccval(String prov_raccval) {
        this.prov_raccval = prov_raccval;
    }

    public String getFg_annullato() {
        return this.fg_annullato;
    }

    public void setFg_annullato(String fg_annullato) {
        this.fg_annullato = fg_annullato;
    }

    public String getDa_annull() {
        return this.da_annull;
    }

    public void setDa_annull(String da_annull) {
        this.da_annull = da_annull;
    }
    
    
    
    
    
}
