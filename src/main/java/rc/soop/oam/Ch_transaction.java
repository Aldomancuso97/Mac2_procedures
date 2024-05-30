package rc.soop.oam;

public class Ch_transaction {

    String credccard_number;
    String cod;
    String id;
    String filiale;
    String tipotr;
    String user;
    String till;
    String data;
    String tipocliente;
    String id_open_till;
    String pay;
    String total;
    String fix;
    String com;
    String round;
    String commission;
    String spread_total;
    String note;
    String agency;
    String agency_cod;
    String localfigures;
    String pos;
    String intbook;
    String intbook_type;
    String intbook_1_tf;
    String intbook_2_val;
    String intbook_1_mod;
    String intbook_1_val;
    String bb;
    String refund;

    String fa_number, cn_number;

    public String getCredccard_number() {
        return credccard_number;
    }

    public void setCredccard_number(String credccard_number) {
        this.credccard_number = credccard_number;
    }

    public static String formatBB(String bb) {
        if (bb.equals("1") || bb.equals("2")) {
            return "BB";
        }
        return "";
    }

    public static String formatTilltr(String del,String bb1,String fatnum,String supporto) {
        String start = "";
        
        if(fatnum.trim().equals("-")){
            start = "C ";
        }else{
            start = "F ";
        }
        
        if (bb1.equals("1") || bb1.equals("2") || bb1.equals("Y")) {
            start += "BB ";
        }
        
        if(supporto.equals("04")){
            start += "CA ";
        }
        
        
        if (del.equals("1")) {
            
            start += "D";
            
//            if (start.equals("")) {
//                 start += "D";
//            } else {
//                 start += "BB D";
//            }
        }
        
        
        
        
        return start;
    }

    public static String formatType(String tipotr) {
        if (tipotr != null) {
            if (tipotr.equals("B")) {
                return "Buy";
            }
            if (tipotr.equals("S")) {
                return "Sell";
            }
        }
        return "-";
    }

    public String formatType_CZ(String tipotr) {
        if (tipotr != null) {
            if (tipotr.equals("B")) {
                return "NAKUPUJEME / WE BUY";
            }
            if (tipotr.equals("S")) {
                return "PROVADAME / WE SELL";
            }
        }
        return "-";
    }

    public String formatType_pdf(String tipotr) {
        if (tipotr != null) {
            if (tipotr.equals("B")) {
                return "Acquisto / Buy";
            }
            if (tipotr.equals("S")) {
                return "Vendita / Sell";
            }
        }
        return "-";
    }

    String intbook_2_tf;
    String intbook_2_mod;
    String intbook_3_tf;
    String intbook_3_mod;
    String heavy_pepI;

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

    String intbook_3_val;

//    public String setId_db() {
//        Db_Master db = new Db_Master();
//        int value = db.getlastId_nc_trans();
//        db.closeDB();
//        if (value > -1) {
//            value++;
//            return Utility.fillLeftInt(value, 15, "0");
//        }
//        return "ERROR";
//    }
    String intbook_mac;
    String intbook_cli;

    public String getBl_status() {
        return this.bl_status;
    }

    String cl_cf;

    public void setBl_status(String bl_status) {
        this.bl_status = bl_status;
    }

    String cl_cod;

    public String getBl_motiv() {
        return this.bl_motiv;
    }

    String del_fg;
    String del_dt;

    public void setBl_motiv(String bl_motiv) {
        this.bl_motiv = bl_motiv;
    }

    String del_user;

    public String getCod() {
        return this.cod;
    }

    String del_motiv;
    String bl_status;
    String bl_motiv;

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFiliale() {
        return this.filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getTipotr() {
        return this.tipotr;
    }

    public void setTipotr(String tipotr) {
        this.tipotr = tipotr;
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

    public String getTipocliente() {
        return this.tipocliente;
    }

    public void setTipocliente(String tipocliente) {
        this.tipocliente = tipocliente;
    }

    public String getId_open_till() {
        return this.id_open_till;
    }

    public void setId_open_till(String id_open_till) {
        this.id_open_till = id_open_till;
    }

    public String getPay() {
        return this.pay;
    }

    public void setPay(String pay) {
        this.pay = pay;
    }

    public String getTotal() {
        return this.total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getFix() {
        return this.fix;
    }

    public void setFix(String fix) {
        this.fix = fix;
    }

    public String getCom() {
        return this.com;
    }

    public void setCom(String com) {
        this.com = com;
    }

    public String getRound() {
        return this.round;
    }

    public void setRound(String round) {
        this.round = round;
    }

    public String getCommission() {
        return this.commission;
    }

    public void setCommission(String commission) {
        this.commission = commission;
    }

    public String getSpread_total() {
        return this.spread_total;
    }

    public void setSpread_total(String spread_total) {
        this.spread_total = spread_total;
    }

    public String getNote() {
        return this.note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getAgency() {
        return this.agency;
    }

    public void setAgency(String agency) {
        this.agency = agency;
    }

    public String getAgency_cod() {
        return this.agency_cod;
    }

    public void setAgency_cod(String agency_cod) {
        this.agency_cod = agency_cod;
    }

    public String getLocalfigures() {
        return this.localfigures;
    }

    public void setLocalfigures(String localfigures) {
        this.localfigures = localfigures;
    }

    public String getPos() {
        return this.pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public String getIntbook() {
        return this.intbook;
    }

    public void setIntbook(String intbook) {
        this.intbook = intbook;
    }

    public String getIntbook_type() {
        return this.intbook_type;
    }

    public void setIntbook_type(String intbook_type) {
        this.intbook_type = intbook_type;
    }

    public String getIntbook_1_tf() {
        return this.intbook_1_tf;
    }

    public void setIntbook_1_tf(String intbook_1_tf) {
        this.intbook_1_tf = intbook_1_tf;
    }

    public String getIntbook_2_val() {
        return this.intbook_2_val;
    }

    public void setIntbook_2_val(String intbook_2_val) {
        this.intbook_2_val = intbook_2_val;
    }

    public String getIntbook_1_mod() {
        return this.intbook_1_mod;
    }

    public void setIntbook_1_mod(String intbook_1_mod) {
        this.intbook_1_mod = intbook_1_mod;
    }

    public String getIntbook_1_val() {
        return this.intbook_1_val;
    }

    public void setIntbook_1_val(String intbook_1_val) {
        this.intbook_1_val = intbook_1_val;
    }

    public String getIntbook_2_tf() {
        return this.intbook_2_tf;
    }

    public void setIntbook_2_tf(String intbook_2_tf) {
        this.intbook_2_tf = intbook_2_tf;
    }

    public String getIntbook_2_mod() {
        return this.intbook_2_mod;
    }

    public void setIntbook_2_mod(String intbook_2_mod) {
        this.intbook_2_mod = intbook_2_mod;
    }

    public String getIntbook_3_tf() {
        return this.intbook_3_tf;
    }

    public void setIntbook_3_tf(String intbook_3_tf) {
        this.intbook_3_tf = intbook_3_tf;
    }

    public String getIntbook_3_mod() {
        return this.intbook_3_mod;
    }

    public void setIntbook_3_mod(String intbook_3_mod) {
        this.intbook_3_mod = intbook_3_mod;
    }

    public String getIntbook_3_val() {
        return this.intbook_3_val;
    }

    public void setIntbook_3_val(String intbook_3_val) {
        this.intbook_3_val = intbook_3_val;
    }

    public String getIntbook_mac() {
        return this.intbook_mac;
    }

    public void setIntbook_mac(String intbook_mac) {
        this.intbook_mac = intbook_mac;
    }

    public String getIntbook_cli() {
        return this.intbook_cli;
    }

    public void setIntbook_cli(String intbook_cli) {
        this.intbook_cli = intbook_cli;
    }

    public String getCl_cf() {
        return this.cl_cf;
    }

    public void setCl_cf(String cl_cf) {
        this.cl_cf = cl_cf;
    }

    public String getCl_cod() {
        return this.cl_cod;
    }

    public void setCl_cod(String cl_cod) {
        this.cl_cod = cl_cod;
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

    public String getHeavy_pepI() {
        return heavy_pepI;
    }

    public void setHeavy_pepI(String heavy_pepI) {
        this.heavy_pepI = heavy_pepI;
    }

    public String getBb() {
        return bb;
    }

    public void setBb(String bb) {
        this.bb = bb;
    }

    public String getRefund() {
        return refund;
    }

    public void setRefund(String refund) {
        this.refund = refund;
    }

    public String getFa_number() {
        return fa_number;
    }

    public void setFa_number(String fa_number) {
        this.fa_number = fa_number;
    }

    public String getCn_number() {
        return cn_number;
    }

    public void setCn_number(String cn_number) {
        this.cn_number = cn_number;
    }

}
