package rc.soop.rilasciofile;

public class Ch_transaction_value implements Comparable<Ch_transaction_value>{
    Ch_transaction trorig;
    
    String id;
    String cod_tr;
    String numeroriga;
    String supporto;
    String pos;
    String posnum;
    String valuta;
    String quantita;
    String rate;
    String com_perc;
    String com_perc_tot;
    String fx_com;
    String tot_com;
    String net;
    String spread;
    String total;
    String roundvalue;
    
    String branch,type,operator,note;

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
    
    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    
    
    
    public Ch_transaction getTrorig() {
        return trorig;
    }

    public String getRoundvalue() {
        return roundvalue;
    }

    public void setRoundvalue(String roundvalue) {
        this.roundvalue = roundvalue;
    }

    public void setTrorig(Ch_transaction trorig) {
        this.trorig = trorig;
    }
    
    
    public String getFx_com() {
        return this.fx_com;
    }

    String kind_fix_comm;

    public String getPosnum() {
        return this.posnum;
    }

    String low_com_ju;

    public void setPosnum(String posnum) {
        this.posnum = posnum;
    }

    String bb;
    String bb_fidcode;
    String contr_valuta;

    public void setFx_com(String fx_com) {
        this.fx_com = fx_com;
    }

    String contr_supporto;

    public String getId() {
        return this.id;
    }

    String contr_quantita;

    public void setId(String id) {
        this.id = id;
    }

    String dt_tr;

    public String getCod_tr() {
        return this.cod_tr;
    }

    String del_fg;

    public void setCod_tr(String cod_tr) {
        this.cod_tr = cod_tr;
    }

    String del_dt;

    public String getNumeroriga() {
        return this.numeroriga;
    }

    public void setNumeroriga(String numeroriga) {
        this.numeroriga = numeroriga;
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

    public String getValuta() {
        return this.valuta;
    }

    public void setValuta(String valuta) {
        this.valuta = valuta;
    }

    public String getQuantita() {
        return this.quantita;
    }

    public void setQuantita(String quantita) {
        this.quantita = quantita;
    }

    public String getRate() {
        return this.rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getCom_perc() {
        return this.com_perc;
    }

    public void setCom_perc(String com_perc) {
        this.com_perc = com_perc;
    }

    public String getCom_perc_tot() {
        return this.com_perc_tot;
    }

    public void setCom_perc_tot(String com_perc_tot) {
        this.com_perc_tot = com_perc_tot;
    }

    public String getTot_com() {
        return this.tot_com;
    }

    public void setTot_com(String tot_com) {
        this.tot_com = tot_com;
    }

    public String getNet() {
        return this.net;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public String getSpread() {
        return this.spread;
    }

    public void setSpread(String spread) {
        this.spread = spread;
    }

    public String getTotal() {
        return this.total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getKind_fix_comm() {
        return this.kind_fix_comm;
    }

    public void setKind_fix_comm(String kind_fix_comm) {
        this.kind_fix_comm = kind_fix_comm;
    }

    public String getLow_com_ju() {
        return this.low_com_ju;
    }

    public void setLow_com_ju(String low_com_ju) {
        this.low_com_ju = low_com_ju;
    }

    public String getBb() {
        return this.bb;
    }

    public void setBb(String bb) {
        this.bb = bb;
    }

    public String getBb_fidcode() {
        return this.bb_fidcode;
    }

    public void setBb_fidcode(String bb_fidcode) {
        this.bb_fidcode = bb_fidcode;
    }

    public String getContr_valuta() {
        return this.contr_valuta;
    }

    public void setContr_valuta(String contr_valuta) {
        this.contr_valuta = contr_valuta;
    }

    public String getContr_supporto() {
        return this.contr_supporto;
    }

    public void setContr_supporto(String contr_supporto) {
        this.contr_supporto = contr_supporto;
    }

    public String getContr_quantita() {
        return this.contr_quantita;
    }

    public void setContr_quantita(String contr_quantita) {
        this.contr_quantita = contr_quantita;
    }

    public String getDt_tr() {
        return this.dt_tr;
    }

    public void setDt_tr(String dt_tr) {
        this.dt_tr = dt_tr;
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
    
    
    @Override
    public int compareTo(Ch_transaction_value o) {
        if (this.getValuta().compareTo(o.getValuta()) > 0) {
            return 1;
        } else if (this.getValuta().compareTo(o.getValuta()) < 0) {
            return -1;
        } else {
            return this.getSupporto().compareTo(o.getSupporto());
        }
    }
    
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\Ch_transaction_value.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */
