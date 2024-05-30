/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.aggiornamenti;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 *
 * @author rcosco
 */
public class StatusBranch {
    String cod, ip;
    int aggfrom, aggto;
    boolean ragg;

    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getAggfrom() {
        return aggfrom;
    }

    public void setAggfrom(int aggfrom) {
        this.aggfrom = aggfrom;
    }

    public int getAggto() {
        return aggto;
    }

    public void setAggto(int aggto) {
        this.aggto = aggto;
    }

    public boolean isRagg() {
        return ragg;
    }

    public void setRagg(boolean ragg) {
        this.ragg = ragg;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
}
