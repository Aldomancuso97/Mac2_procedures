/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

/**
 *
 * @author rcosco
 */
public class DailyChange {

    String filiale, descr, data, VOLUMEAC, VOLUMECA, VOLUMETC, TRANSAC, TRANSCA, TRANSTC, COMMAC, COMMCA, COMMTC, SPREADBR, SPREADBA, SPREADAC, SPREADCA;
    String TOTTRANSACQ, TOTVOLACQ, TOTGMACQ, PERCACQ;
    String VOLUMEVENDOFF, VOLUMEONL, VOLUMERIVA, TRANSVENDOFF, TRANSONL, TRANSRIVA, COMMVENDOFF, COMMONL, COMMRIVA, SPREADVEND;
    String TOTTRANSVEN, TOTVOLVEN, TOTGMVEN, PERCVEN;
    String TOTVOL, TOTTRANS, TOTGM, PERCVEND;
    String COP, TOBANKCOP, FRBANKCOP, TOBRCOP, FRBRCOP, OCERRCOP;
    String FX, TOBANKFX, FRBANKFX, TOBRFX, FRBRFX, OCERRFX;

    public DailyChange() {
    }

    public String getFiliale() {
        return filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getDescr() {
        return descr;
    }

    public void setDescr(String descr) {
        this.descr = descr;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getVOLUMEAC() {
        return VOLUMEAC;
    }

    public void setVOLUMEAC(String VOLUMEAC) {
        this.VOLUMEAC = VOLUMEAC;
    }

    public String getVOLUMECA() {
        return VOLUMECA;
    }

    public String getSPREADAC() {
        return SPREADAC;
    }

    public void setSPREADAC(String SPREADAC) {
        this.SPREADAC = SPREADAC;
    }

    public String getSPREADCA() {
        return SPREADCA;
    }

    public void setSPREADCA(String SPREADCA) {
        this.SPREADCA = SPREADCA;
    }

    public void setVOLUMECA(String VOLUMECA) {
        this.VOLUMECA = VOLUMECA;
    }

    public String getVOLUMETC() {
        return VOLUMETC;
    }

    public void setVOLUMETC(String VOLUMETC) {
        this.VOLUMETC = VOLUMETC;
    }

    public String getTRANSAC() {
        return TRANSAC;
    }

    public void setTRANSAC(String TRANSAC) {
        this.TRANSAC = TRANSAC;
    }

    public String getTRANSCA() {
        return TRANSCA;
    }

    public void setTRANSCA(String TRANSCA) {
        this.TRANSCA = TRANSCA;
    }

    public String getTRANSTC() {
        return TRANSTC;
    }

    public void setTRANSTC(String TRANSTC) {
        this.TRANSTC = TRANSTC;
    }

    public String getCOMMAC() {
        return COMMAC;
    }

    public void setCOMMAC(String COMMAC) {
        this.COMMAC = COMMAC;
    }

    public String getCOMMCA() {
        return COMMCA;
    }

    public void setCOMMCA(String COMMCA) {
        this.COMMCA = COMMCA;
    }

    public String getCOMMTC() {
        return COMMTC;
    }

    public void setCOMMTC(String COMMTC) {
        this.COMMTC = COMMTC;
    }

    public String getSPREADBR() {
        return SPREADBR;
    }

    public void setSPREADBR(String SPREADBR) {
        this.SPREADBR = SPREADBR;
    }

    public String getSPREADBA() {
        return SPREADBA;
    }

    public void setSPREADBA(String SPREADBA) {
        this.SPREADBA = SPREADBA;
    }

    public String getTOTTRANSACQ() {
        return TOTTRANSACQ;
    }

    public void setTOTTRANSACQ(String TOTTRANSACQ) {
        this.TOTTRANSACQ = TOTTRANSACQ;
    }

    public String getTOTVOLACQ() {
        return TOTVOLACQ;
    }

    public void setTOTVOLACQ(String TOTVOLACQ) {
        this.TOTVOLACQ = TOTVOLACQ;
    }

    public String getTOTGMACQ() {
        return TOTGMACQ;
    }

    public void setTOTGMACQ(String TOTGMACQ) {
        this.TOTGMACQ = TOTGMACQ;
    }

    public String getPERCACQ() {
        return PERCACQ;
    }

    public void setPERCACQ(String PERCACQ) {
        this.PERCACQ = PERCACQ;
    }

    public String getVOLUMEVENDOFF() {
        return VOLUMEVENDOFF;
    }

    public void setVOLUMEVENDOFF(String VOLUMEVENDOFF) {
        this.VOLUMEVENDOFF = VOLUMEVENDOFF;
    }

    public String getVOLUMEONL() {
        return VOLUMEONL;
    }

    public void setVOLUMEONL(String VOLUMEONL) {
        this.VOLUMEONL = VOLUMEONL;
    }

    public String getVOLUMERIVA() {
        return VOLUMERIVA;
    }

    public void setVOLUMERIVA(String VOLUMERIVA) {
        this.VOLUMERIVA = VOLUMERIVA;
    }

    public String getTRANSVENDOFF() {
        return TRANSVENDOFF;
    }

    public void setTRANSVENDOFF(String TRANSVENDOFF) {
        this.TRANSVENDOFF = TRANSVENDOFF;
    }

    public String getTRANSONL() {
        return TRANSONL;
    }

    public void setTRANSONL(String TRANSONL) {
        this.TRANSONL = TRANSONL;
    }

    public String getTRANSRIVA() {
        return TRANSRIVA;
    }

    public void setTRANSRIVA(String TRANSRIVA) {
        this.TRANSRIVA = TRANSRIVA;
    }

    public String getCOMMVENDOFF() {
        return COMMVENDOFF;
    }

    public void setCOMMVENDOFF(String COMMVENDOFF) {
        this.COMMVENDOFF = COMMVENDOFF;
    }

    public String getCOMMONL() {
        return COMMONL;
    }

    public void setCOMMONL(String COMMONL) {
        this.COMMONL = COMMONL;
    }

    public String getCOMMRIVA() {
        return COMMRIVA;
    }

    public void setCOMMRIVA(String COMMRIVA) {
        this.COMMRIVA = COMMRIVA;
    }

    public String getSPREADVEND() {
        return SPREADVEND;
    }

    public void setSPREADVEND(String SPREADVEND) {
        this.SPREADVEND = SPREADVEND;
    }

    public String getTOTTRANSVEN() {
        return TOTTRANSVEN;
    }

    public void setTOTTRANSVEN(String TOTTRANSVEN) {
        this.TOTTRANSVEN = TOTTRANSVEN;
    }

    public String getTOTVOLVEN() {
        return TOTVOLVEN;
    }

    public void setTOTVOLVEN(String TOTVOLVEN) {
        this.TOTVOLVEN = TOTVOLVEN;
    }

    public String getTOTGMVEN() {
        return TOTGMVEN;
    }

    public void setTOTGMVEN(String TOTGMVEN) {
        this.TOTGMVEN = TOTGMVEN;
    }

    public String getPERCVEN() {
        return PERCVEN;
    }

    public void setPERCVEN(String PERCVEN) {
        this.PERCVEN = PERCVEN;
    }

    public String getTOTVOL() {
        return TOTVOL;
    }

    public void setTOTVOL(String TOTVOL) {
        this.TOTVOL = TOTVOL;
    }

    public String getTOTTRANS() {
        return TOTTRANS;
    }

    public void setTOTTRANS(String TOTTRANS) {
        this.TOTTRANS = TOTTRANS;
    }

    public String getTOTGM() {
        return TOTGM;
    }

    public void setTOTGM(String TOTGM) {
        this.TOTGM = TOTGM;
    }

    public String getPERCVEND() {
        return PERCVEND;
    }

    public void setPERCVEND(String PERCVEND) {
        this.PERCVEND = PERCVEND;
    }

    public String getCOP() {
        return COP;
    }

    public void setCOP(String COP) {
        this.COP = COP;
    }

    public String getTOBANKCOP() {
        return TOBANKCOP;
    }

    public void setTOBANKCOP(String TOBANKCOP) {
        this.TOBANKCOP = TOBANKCOP;
    }

    public String getFRBANKCOP() {
        return FRBANKCOP;
    }

    public void setFRBANKCOP(String FRBANKCOP) {
        this.FRBANKCOP = FRBANKCOP;
    }

    public String getTOBRCOP() {
        return TOBRCOP;
    }

    public void setTOBRCOP(String TOBRCOP) {
        this.TOBRCOP = TOBRCOP;
    }

    public String getFRBRCOP() {
        return FRBRCOP;
    }

    public void setFRBRCOP(String FRBRCOP) {
        this.FRBRCOP = FRBRCOP;
    }

    public String getOCERRCOP() {
        return OCERRCOP;
    }

    public void setOCERRCOP(String OCERRCOP) {
        this.OCERRCOP = OCERRCOP;
    }

    public String getFX() {
        return FX;
    }

    public void setFX(String FX) {
        this.FX = FX;
    }

    public String getTOBANKFX() {
        return TOBANKFX;
    }

    public void setTOBANKFX(String TOBANKFX) {
        this.TOBANKFX = TOBANKFX;
    }

    public String getFRBANKFX() {
        return FRBANKFX;
    }

    public void setFRBANKFX(String FRBANKFX) {
        this.FRBANKFX = FRBANKFX;
    }

    public String getTOBRFX() {
        return TOBRFX;
    }

    public void setTOBRFX(String TOBRFX) {
        this.TOBRFX = TOBRFX;
    }

    public String getFRBRFX() {
        return FRBRFX;
    }

    public void setFRBRFX(String FRBRFX) {
        this.FRBRFX = FRBRFX;
    }

    public String getOCERRFX() {
        return OCERRFX;
    }

    public void setOCERRFX(String OCERRFX) {
        this.OCERRFX = OCERRFX;
    }

}
