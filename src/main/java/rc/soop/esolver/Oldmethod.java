package rc.soop.esolver;

///*
// * To change this license header, choose License Headers in Project Properties.
// * To change this template file, choose Tools | Templates
// * and open the template in the editor.
// */
//package it.refill.esolver;
//
//import static esolver.ESolver.separator;
//import static esolver.ESolver.tag_TES;
//import static esolver.Util.calcolaIva;
//import static esolver.Util.fd;
//import static esolver.Util.formatAL;
//import static esolver.Util.formatBankBranchReport;
//import static esolver.Util.formatDoubleforMysql;
//import static esolver.Util.getCity_apm;
//import static esolver.Util.getNC_category;
//import static esolver.Util.getNC_causal;
//import static esolver.Util.getValueDiff;
//import static esolver.Util.get_ET_change_value;
//import static esolver.Util.get_customerKind;
//import static esolver.Util.get_user;
//import static esolver.Util.get_vat;
//import static esolver.Util.list_oc_errors;
//import static esolver.Util.log;
//import static esolver.Util.parseDoubleR;
//import static esolver.Util.query_transaction_ch;
//import static esolver.Util.query_transaction_value;
//import static esolver.Util.removeDuplicatesAL;
//import static esolver.Util.removeDuplicatesALAr;
//import static esolver.Util.roundDoubleandFormat;
//import java.io.File;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.ArrayList;
//import java.util.logging.Level;
//import org.apache.commons.lang3.StringUtils;
//
///**
// *
// * @author rcosco
// */
//public class Oldmethod {
////    public File FILEP5(String path, String data, String anno,
////            ArrayList<String[]> fatt_note,
////            Branch filiale,
////            ArrayList<String[]> contabilita_codici,
////            ArrayList<String[]> bank,
////            ArrayList<String[]> country,
////            String valuta_locale,
////            ArrayList<Branch> branch) {
////
////        try {
////
////            ArrayList<String> cod_cliente = new ArrayList<>();
////            ArrayList<Client> lista_client = new ArrayList<>();
////
////            for (int i = 0; i < fatt_note.size(); i++) {
////                String valori[] = fatt_note.get(i);
////                if (valori[0].equals(filiale.getCod())) {
////                    ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
////                    boolean ca = false;
////                    if (livalue.size() > 0) {
////                        for (int x = 0; x < livalue.size(); x++) {
////                            if (livalue.get(x).getSupporto().equals("04")) {
////                                ca = true;
////                                break;
////                            }
////                        }
////                        Db_Master db1 = new Db_Master();
////                        Client cl = db1.query_Client_transaction(valori[1], valori[2]);
////                        db1.closeDB();
////                        if (cl != null) {
////                            lista_client.add(cl);
////                            if (!ca) {
////                                if (valori[7].equals("B")) {
////                                    cod_cliente.add(cl.getCode());
////                                } else if (valori[7].equals("S")) {
////                                    cod_cliente.add(cl.getCode());
////                                }
////                            } else {
////                                cod_cliente.add(cl.getCode());
////                            }
////                        }
////                    }
////                }
////            }
////            removeDuplicatesAL(cod_cliente);
////            if (cod_cliente.size() > 0) {
////                File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P5_eSol.txt");
////                PrintWriter writer = new PrintWriter(f);
////                for (int i = 0; i < cod_cliente.size(); i++) {
////                    String cc = cod_cliente.get(i);
////                    Client cl = getCL(lista_client, cc);
////                    if (cl != null) {
////                        Db_Master db1 = new Db_Master();
////                        String naz = db1.codnaz_esolv(cl.getNazione());
////                        db1.closeDB();
////
////                        String city[] = getCity_apm(cl.getCitta());
////                        crea_anagrafica_clienti(cl.getCognome().toUpperCase(),
////                                cl.getNome().toUpperCase(),
////                                cl.getIndirizzo().toUpperCase(),
////                                city[1].toUpperCase(),
////                                naz, cl.getNumero_documento(),
////                                cc, writer);
////                    }
////                }
////                writer.close();
////                return f;
////            }
////        } catch (IOException ex) {
////            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
////        }
////        return null;
////    }
////
////    public File FILEP4(String path, String data, String anno,
////            ArrayList<String[]> fatt_note,
////            Branch filiale,
////            ArrayList<String[]> contabilita_codici,
////            ArrayList<String[]> bank,
////            ArrayList<String[]> country,
////            String valuta_locale,
////            ArrayList<Branch> branch) {
////
////        try {
////            
////            String conto_RVE = formatAL("RVE", contabilita_codici, 2);
////            
////            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P4_eSol.txt");
////            PrintWriter writer = new PrintWriter(f);
////            int n_reg = 1;
////
////            for (int i = 0; i < fatt_note.size(); i++) {
////                String valori[] = fatt_note.get(i);
////                if (valori[0].equals(filiale.getCod())) {
////                    String tip = valori[6];
////                    if (tip.equals("F")) {
////                        tip = "FAT";
////                    } else {
////                        tip = "NDC";
////                    }
////
////                    Ch_transaction ch = query_transaction_ch(valori[1]);
////                    CustomerKind ck = get_customerKind(ch.getTipocliente());
////                    ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
////                    boolean ca = false;
////                    if (livalue.size() > 0) {
////                        String cadval = "";
////                        for (int x = 0; x < livalue.size(); x++) {
////                            if (livalue.get(x).getSupporto().equals("04")) {
////                                ca = true;
////                                cadval = livalue.get(x).getPos();
////                                break;
////                            }
////                        }
////                        Db_Master db1 = new Db_Master();
////                        Client cl = db1.query_Client_transaction(valori[1], valori[2]);
////                        db1.closeDB();
////
////                        if (cl != null) {
////
////                            String contocassa = null;
////                            String total = StringUtils.replace(valori[4], ".", ",");
////                            String pay = StringUtils.replace(valori[3], ".", ",");
////
////                            
////                            
////                            double commission = fd(valori[5]) + parseDoubleR(valori[8]);
////                            String comm = StringUtils.replace(roundDoubleandFormat((fd(valori[5]) + parseDoubleR(valori[8])), 2), ".", ",");
////
////                            if (valori[12].trim().equals("0")) {
////
////                                String pos;
////                                if (valori[10].equals("01")) {
////                                    pos = "00";
////                                } else if (valori[10].equals("08")) {
////                                    pos = valori[11];
////                                } else {
////                                    if (valori[10].equals("-")) {
////                                        if (cadval.equals("")) {
////                                            pos = "00";
////                                        } else {
////                                            pos = cadval;
////                                        }
////                                    } else {
////                                        pos = valori[11];
////                                    }
////                                }
////                                if (!ca) {
////                                    if (valori[7].equals("B")) {
////                                        String startdescr = "Acquisto valuta";
////                                        if (pos.equals("00")) {
////                                            if (tip.equals("FAT")) {
////                                                tofrombank_fattura("FBB", anno, data,
////                                                        String.valueOf(n_reg),
////                                                        conto_RVE,
////                                                        this.codiceNegozi, filiale.getCod(),
////                                                        total, filiale.getCod(), valori[9],
////                                                        "", startdescr, writer,null,null,null);
////                                                n_reg++;
////                                            }
////                                            contocassa = this.codiceNegozi;
////                                        } else if (pos.equals("99")) {
////                                            if (tip.equals("FAT")) {
////                                                tofrombank_fattura("FBB", anno, data,
////                                                        String.valueOf(n_reg),
////                                                        conto_RVE,
////                                                        conto_RVE, filiale.getCod(),
////                                                        total, filiale.getCod(), valori[9], "", startdescr, writer,null,null,null);
////
////                                                n_reg++;
////                                            }
////                                            contocassa = conto_RVE;
////                                        } else {
////                                            if (tip.equals("FAT")) {
////                                                tofrombank_fattura("FBB", anno, data,
////                                                        String.valueOf(n_reg),
////                                                        conto_RVE,
////                                                        formatAL(pos, bank, 2), pos,
////                                                        total, filiale.getCod(), valori[9], "", startdescr, writer,null,null,null);
////                                                n_reg++;
////
////                                            }
////                                            contocassa = formatAL(pos, bank, 2);
////                                        }
////                                        if (tip.equals("FAT")) {
////                                            if (commission >= fd(ck.getIp_soglia_bollo())) {
////                                                impostabollo("FIB", anno, data, String.valueOf(n_reg),
////                                                        "630520", //CHIEDERE
////                                                        "390509", //CHEIDERE
////                                                        StringUtils.replace(ck.getIp_value_bollo(), ".", ","),
////                                                        filiale.getCod(), valori[9],
////                                                        cl.getCognome(), cl.getNome(),
////                                                        writer);
////                                                n_reg++;
////                                            }
////                                        }
////                                    } else if (valori[7].equals("S")) {
////                                        
////                                        String spread = StringUtils.replace(valori[13], ".", ",");
////                                        String net = StringUtils.replace(roundDoubleandFormat((fd(valori[4]) - fd(valori[13])), 2), ".", ",");
////                                        
////                                        if (tip.equals("FAT")) {
////                                            String startdescr = "Vendita valuta";
////                                            if (pos.equals("00")) {
////
////                                                if (tip.equals("FAT")) {
////                                                    tofrombank_fattura("N_FBS", anno, data,
////                                                            String.valueOf(n_reg),
////                                                            conto_RVE,
////                                                            this.codiceNegozi, filiale.getCod(),
////                                                            total, filiale.getCod(), valori[9], valori[2], startdescr, writer,
////                                                    formatAL("SVVEC", contabilita_codici, 2),
////                                                    net,spread);
////                                                    n_reg++;
////
////                                                }
////                                                contocassa = this.codiceNegozi;
////                                            } else if (pos.equals("99")) {
////                                                if (tip.equals("FAT")) {
////                                                    tofrombank_fattura("N_FBS", anno, data,
////                                                            String.valueOf(n_reg),
////                                                            conto_RVE,
////                                                            conto_RVE, filiale.getCod(),
////                                                            total, filiale.getCod(), valori[9], valori[2], startdescr, writer,
////                                                    formatAL("SVVEC", contabilita_codici, 2),
////                                                    net,spread);
////                                                    n_reg++;
////
////                                                }
////                                                contocassa = conto_RVE;
////                                            } else {
////
////                                                if (tip.equals("FAT")) {
////                                                    tofrombank_fattura("N_FBS", anno, data,
////                                                            String.valueOf(n_reg),
////                                                            conto_RVE,
////                                                            formatAL(pos, bank, 2), pos,
////                                                            total, filiale.getCod(), valori[9], valori[2], startdescr, writer,
////                                                    formatAL("SVVEC", contabilita_codici, 2),
////                                                    net,spread);
////                                                    n_reg++;
////
////                                                }
////                                                contocassa = formatAL(pos, bank, 2);
////                                            }
////                                            if (tip.equals("FAT")) {
////                                                if (commission >= fd(ck.getIp_soglia_bollo())) {
////                                                    impostabollo("FIB", anno, data, String.valueOf(n_reg),
////                                                            "630520", //CHIEDERE
////                                                            "390509", //CHEIDERE
////                                                            StringUtils.replace(ck.getIp_value_bollo(), ".", ","),
////                                                            filiale.getCod(), valori[9],
////                                                            cl.getCognome(), cl.getNome(),
////                                                            writer);
////                                                    n_reg++;
////                                                }
////                                            }
////                                        }
////                                    }
////                                } else {
////                                    //NUOVO ASPETTARE RAGGI
////                                    String startdescr = "Cash advance";
////                                    if (pos.equals("00")) {
////                                        if (tip.equals("FAT")) {
////                                            tofrombank_fattura("FBB", anno, data,
////                                                    String.valueOf(n_reg),
////                                                    formatAL("CCCA", contabilita_codici, 2),
////                                                    this.codiceNegozi,
////                                                    filiale.getCod(),
////                                                    pay, filiale.getCod(), valori[9],
////                                                    "", startdescr, writer,null,null,null);
////                                            n_reg++;
////                                        }
////                                        contocassa = this.codiceNegozi;
////                                    } else if (pos.equals("99")) {
////                                        if (tip.equals("FAT")) {
////                                            tofrombank_fattura("FBB", anno, data,
////                                                    String.valueOf(n_reg),
////                                                    formatAL("CCCA", contabilita_codici, 2),
////                                                    conto_RVE,
////                                                    filiale.getCod(),
////                                                    pay, filiale.getCod(), valori[9], "", startdescr, writer,null,null,null);
////                                            n_reg++;
////                                        }
////                                        contocassa = conto_RVE;
////                                    } else {
////
////                                        if (tip.equals("FAT")) {
////                                            tofrombank_fattura("FBB", anno, data,
////                                                    String.valueOf(n_reg),
////                                                    formatAL(pos, bank, 2),
////                                                    formatAL("CANE", contabilita_codici, 2),
////                                                    filiale.getCod(),
////                                                    pay, filiale.getCod(), valori[9],
////                                                    pos, startdescr, writer,null,null,null);
////                                            n_reg++;
////                                        }
////                                        contocassa = formatAL(pos, bank, 2);
////                                    }
////                                }
////
////                                if (contocassa != null) {
////                                    if (tip.equals("FAT")) {
////                                        incasso_fattura("IFA", anno, data,
////                                                String.valueOf(n_reg),
////                                                contocassa, this.incassofattura, comm,
////                                                filiale.getCod(), valori[9],
////                                                cl.getCognome(), cl.getNome(),
////                                                valori[2], writer);
////                                        n_reg++;
////                                    }
////                                }
////                            }
////
////                        }
////
////                    }
////
////                }
////            }
////
////            writer.close();
////            if (n_reg > 1) {
////                return f;
////            }
////
////        } catch (IOException ex) {
////            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
////        }
////        return null;
////    }
////
////    public File FILEP3(String path, String data, String anno,
////            ArrayList<String[]> fatt_note,
////            Branch filiale,
////            ArrayList<String[]> contabilita_codici,
////            ArrayList<String[]> bank,
////            ArrayList<String[]> country,
////            String valuta_locale,
////            ArrayList<Branch> branch) {
////
////        try {
////
////            boolean pr = false;
////
////            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P3_eSol.txt");
////            PrintWriter writer = new PrintWriter(f);
////            for (int i = 0; i < fatt_note.size(); i++) {
////                String valori[] = fatt_note.get(i);
////                if (valori[0].equals(filiale.getCod())) {
////                    String tip = valori[6];
////                    if (tip.equals("F")) {
////                        tip = "FAT";
////                    } else {
////                        tip = "NDC";
////                    }
////
////                    ArrayList<Ch_transaction_value> livalue = query_transaction_value(valori[1]);
////                    boolean ca = false;
////                    if (livalue.size() > 0) {
////                        for (int x = 0; x < livalue.size(); x++) {
////                            if (livalue.get(x).getSupporto().equals("04")) {
////                                ca = true;
////                                break;
////                            }
////                        }
////                        Db_Master db1 = new Db_Master();
////                        Client cl = db1.query_Client_transaction(valori[1], valori[2]);
////                        CustomerKind ck = db1.get_customerKind(db1.query_transaction_ch(valori[1]).getTipocliente());
////                        //String naz = db1.codnaz_esolv(cl.getNazione());
////                        db1.closeDB();
////
////                        if (cl != null) {
////                            String incassato = "";
////                            String comm = StringUtils.replace(roundDoubleandFormat((fd(valori[5]) + parseDoubleR(valori[8])), 2), ".", ",");
////                            if (!ca) {
////                                if (valori[7].equals("B")) {
////                                    fattura_new(tip, data, cl.getCode(),
////                                            formatAL("CAV", contabilita_codici, 2),
////                                            incassato, comm,
////                                            filiale.getCod(),
////                                            valori[9],
////                                            cl.getCognome(),
////                                            cl.getNome(), ck.getVatcode(), writer);
////                                    pr = true;
////                                } else if (valori[7].equals("S")) {
////                                    fattura_new(tip, data, cl.getCode(),
////                                            formatAL("CIVV", contabilita_codici, 2),
////                                            incassato, comm,
////                                            filiale.getCod(),
////                                            valori[9],
////                                            cl.getCognome(),
////                                            cl.getNome(), ck.getVatcode(), writer);
////                                    pr = true;
////                                }
////                            } else {
////
////                                //NUOVO ASPETTARE RAGGI
////                                fattura_new(tip, data, cl.getCode(),
////                                        //Utility.formatAL(pos, bank, 2),
////                                        formatAL("CCCA", contabilita_codici, 2),
////                                        incassato, comm,
////                                        filiale.getCod(),
////                                        valori[9],
////                                        cl.getCognome(),
////                                        cl.getNome(), ck.getVatcode(), writer);
////                                pr = true;
////                            }
////                        }
////                    }
////                }
////            }
////            writer.close();
////            if (pr) {
////                return f;
////            }
////            f.delete();
////        } catch (IOException ex) {
////            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
////        }
////        return null;
////    }
////
////    public File FILEP2(String path, String data, String anno,
////            ArrayList<Ch_transaction> ch_list,
////            ArrayList<NC_transaction> nc_list,
////            ArrayList<NC_category> listcategory,
////            ArrayList<NC_causal> listcausal,
////            Branch filiale,
////            ArrayList<String[]> contabilita_codici,
////            ArrayList<String[]> bank,
////            String valuta_locale,
////            ArrayList<Branch> branch,
////            ArrayList<VATcode> vat,
////            CustomerKind ck) {
////        try {
////            String conto_RVE = formatAL("RVE", contabilita_codici, 2);
////            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P2_eSol.txt");
////            PrintWriter writer = new PrintWriter(f);
////            int nreg = 1;
////
////            //SELL
////            ArrayList<String> se = new ArrayList<>();
////            ArrayList<String[]> se_value_temp = new ArrayList<>();
////            ArrayList<String[]> se_value = new ArrayList<>();
////
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("S")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////
////                    String pos;
////                    if (ch_list.get(i).getLocalfigures().equals("01")) {
////                        pos = "00";
////                    } else if (ch_list.get(i).getLocalfigures().equals("08")) {
//////                        pos = "99";
////                        pos = ch_list.get(i).getPos();
////                    } else {
////                        pos = ch_list.get(i).getPos();
////                    }
////                    if (!valori.isEmpty()) {
////                        se.add(pos);
////                        for (int x = 0; x < valori.size(); x++) {
////
////                            String[] tmp = {pos, valori.get(x).getNet(), valori.get(x).getTotal(), valori.get(x).getTot_com(), valori.get(x).getRoundvalue(), valori.get(x).getSpread()};
////
////                            se_value_temp.add(tmp);
////                        }
////                    }
////                }
////            }
////
////            removeDuplicatesAL(se);
////            for (int x = 0; x < se.size(); x++) {
////
////                double importo_comm = 0.00;
////                double importo_net = 0.00;
////                double importo_t1 = 0.00;
////                double importo_sp = 0.00;
////
////                for (int i = 0; i < se_value_temp.size(); i++) {
////                    if (se.get(x).equals(se_value_temp.get(i)[0])) {
////                        importo_comm = importo_comm + parseDoubleR(se_value_temp.get(i)[3]) + parseDoubleR(se_value_temp.get(i)[4]);
////                        importo_net = importo_net + parseDoubleR(se_value_temp.get(i)[2]);
////                        importo_t1 = importo_t1 + parseDoubleR(se_value_temp.get(i)[1]);
////                        importo_sp = importo_sp + fd(se_value_temp.get(i)[5]);
////                    }
////                }
////
////                String[] tmp = {
////                    se.get(x),
////                    roundDoubleandFormat(importo_comm, 2),
////                    roundDoubleandFormat(importo_net, 2),
////                    roundDoubleandFormat(importo_t1, 2),
////                    roundDoubleandFormat(importo_sp, 2)
////                };
////
////                se_value.add(tmp);
////            }
////
////            for (int x = 0; x < se_value.size(); x++) {
////
////                String[] valore = se_value.get(x);
////
////                double corrisp = fd(valore[1]);
////
//////                double nuovoimporto = fd(valore[2]) + fd(valore[1]);
////                double nuovoimporto = fd(valore[3]);
////                double com1 = fd(valore[1]);
////
////                double rig1 = nuovoimporto - com1;
////
////                double spr_val = fd(valore[4]);
////                double net_val = rig1 - spr_val;
////
////                if (corrisp != 0) {
////
////                    String newrig1 = StringUtils.replace(roundDoubleandFormat(rig1, 2), ".", ",");
////                    String newimp = StringUtils.replace(roundDoubleandFormat(nuovoimporto, 2), ".", ",");
////                    String commissioni = StringUtils.replace(valore[1], ".", ",");
////
////                    String net = StringUtils.replace(roundDoubleandFormat(net_val, 2), ".", ",");
////                    String spread = StringUtils.replace(roundDoubleandFormat(spr_val, 2), ".", ",");
////
////                    if (valore[0].equals("00")) {
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////                        sell("N_SEL", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                newrig1,
////                                commissioni,
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                null, null, ck.getVatcode(), writer,
////                                newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),
////                                net, spread);
////                        nreg++;
////
////                    } else if (valore[0].equals("99")) {
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////                        sell("N_SEL", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                newrig1,
////                                commissioni,
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                conto_RVE,
////                                valore[0], ck.getVatcode(), writer,
////                                newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),
////                                net, spread);
////                        nreg++;
////                    } else {
////
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////
////                        sell("N_SEL", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                newrig1,
////                                commissioni,
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                formatAL(valore[0], bank, 2),
////                                valore[0],
////                                ck.getVatcode(), writer, newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),
////                                net, spread);
////                        nreg++;
////                    }
////                }
////            }
////            //BUY
////            double tot1 = 0.00;
////            double comm1 = 0.00;
////            double net1 = 0.00;
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("B")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////                    if (!valori.isEmpty()) {
////                        for (int x = 0; x < valori.size(); x++) {
////                            Ch_transaction_value val = valori.get(x);
////                            if (!val.getSupporto().equals("04")) {
////                                tot1 = tot1 + parseDoubleR(val.getTotal());
////                                comm1 = comm1 + parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue());
////                                net1 = net1 + parseDoubleR(val.getNet());
////                            }
////                        }
////                    }
////                }
////            }
////            if (tot1 > 0 && comm1 != 0) {
////                writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                        + separator + separator + separator + separator + separator + separator
////                        + separator + separator + separator + separator + separator + separator + separator
////                        + "Corrispettivi " + filiale.getDe_branch() + separator);
////                buy("BUY", anno, data,
////                        String.valueOf(nreg),
////                        filiale.getCod(),
////                        StringUtils.replace(roundDoubleandFormat(net1, 2), ".", ","),
////                        formatAL("CAV", contabilita_codici, 2),
////                        StringUtils.replace(roundDoubleandFormat(comm1, 2), ".", ","),
////                        conto_RVE,
////                        StringUtils.replace(roundDoubleandFormat(tot1, 2), ".", ","), ck.getVatcode(), writer, scontrino_NORM);
////                nreg++;
////            }
////
////            //BUY  - CASH ADVANCE - CON COMMISSIONI
////            ArrayList<String> b_ca_c = new ArrayList<>();
////            ArrayList<String[]> b_ca_c_value_temp = new ArrayList<>();
////            ArrayList<String[]> b_ca_c_value = new ArrayList<>();
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("B")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////                    if (!valori.isEmpty()) {
////                        for (int x = 0; x < valori.size(); x++) {
////                            Ch_transaction_value val = valori.get(x);
////                            if (val.getSupporto().equals("04")) {
////                                if (parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue()) > 0.0D) {
////                                    b_ca_c.add(val.getPos());
////                                    String[] tmp = {val.getPos(), val.getNet(), val.getTotal(), val.getTot_com(), val.getRoundvalue()};
////                                    b_ca_c_value_temp.add(tmp);
////                                }
////                            }
////                        }
////                    }
////                }
////            }
////
////            removeDuplicatesAL(b_ca_c);
////            for (int x = 0; x < b_ca_c.size(); x++) {
////                double importo_b_ca_sc_net = 0.00;
////                double importo_b_ca_sc_total = 0.00;
////                double importo_b_ca_sc_comm = 0.00;
////                for (int i = 0; i < b_ca_c_value_temp.size(); i++) {
////                    if (b_ca_c.get(x).equals(b_ca_c_value_temp.get(i)[0])) {
////                        importo_b_ca_sc_net = importo_b_ca_sc_net + parseDoubleR(b_ca_c_value_temp.get(i)[1]);
////                        importo_b_ca_sc_total = importo_b_ca_sc_total + parseDoubleR(b_ca_c_value_temp.get(i)[2]);
////                        importo_b_ca_sc_comm = importo_b_ca_sc_comm + parseDoubleR(b_ca_c_value_temp.get(i)[3]) + parseDoubleR(b_ca_c_value_temp.get(i)[4]);
////                    }
////                }
////                String[] tmp = {b_ca_c.get(x), roundDoubleandFormat(importo_b_ca_sc_net, 2), roundDoubleandFormat(importo_b_ca_sc_total, 2),
////                    roundDoubleandFormat(importo_b_ca_sc_comm, 2)};
////                b_ca_c_value.add(tmp);
////            }
////
////            for (int x = 0; x < b_ca_c_value.size(); x++) {
////                String[] valore = b_ca_c_value.get(x);
////                writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                        + separator + separator + separator + separator + separator + separator
////                        + separator + separator + separator + separator + separator + separator + separator
////                        + "Corrispettivi " + filiale.getDe_branch() + separator);
////                cashAdvance("CAC", anno, data,
////                        String.valueOf(nreg),
////                        formatAL("CCCA", contabilita_codici, 2),
////                        StringUtils.replace(valore[1], ".", ","),
////                        StringUtils.replace(valore[2], ".", ","),
////                        StringUtils.replace(valore[3], ".", ","),
////                        filiale.getDe_branch(),
////                        filiale.getCod(),
////                        formatAL(valore[0], bank, 2),
////                        formatAL("CANE", contabilita_codici, 2), //CASSE NEGOZI //14/11
////                        ck.getVatcode(), writer, scontrino_NORM);
////                nreg++;
////
////            }
////            ArrayList<String[]> nochangecorr = new ArrayList<>();
////            ArrayList<String[]> nochangecorr_value = new ArrayList<>();
////
////            ArrayList<String[]> ti = new ArrayList<>();
////            ArrayList<String[]> ti_value = new ArrayList<>();
////
////            for (int i = 0; i < nc_list.size(); i++) {
////                NC_category nc0 = getNC_category(listcategory, nc_list.get(i).getGruppo_nc());
////                NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc());
////
////                if (nc0 != null && nc1 != null && !nc1.getNc_de().equals("14")) {
////
////                    if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getFg_tipo_transazione_nc().equals("21")) {
////                        String comm;
////                        if (fd(nc_list.get(i).getCommissione()) > 0) {
////                            comm = nc_list.get(i).getCommissione();
////                        } else {
////                            comm = nc_list.get(i).getTi_ticket_fee();
////                        }
////
////                        comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
////
////                        if (parseDoubleR(comm) > 0.0D) {
////                            String pos;
////                            if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                                pos = "00";
////                            } else if (nc_list.get(i).getSupporto().equals("08")) {
//////                                pos = "99";
////
////                                pos = nc_list.get(i).getPos();
////                            } else {
////                                pos = nc_list.get(i).getPos();
////                            }
////                            String[] va = {nc_list.get(i).getGruppo_nc(), pos};
////                            ti.add(va);
////                        }
//////                    } else if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getGruppo_nc().equals("SHU01")) { //TEST
////                    } else if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && !nc0.getConto_coge_02().trim().equals("")) { //PROD
////
////                        String pos;
////                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                            pos = "00";
////                        } else if (nc_list.get(i).getSupporto().equals("08")) {
////                            //                          pos = "99";
////                            pos = nc_list.get(i).getPos();
//////                            System.out.println(nc_list.get(i).getPos());
////                        } else {
////                            pos = nc_list.get(i).getPos();
////                        }
////                        String[] va = {nc_list.get(i).getGruppo_nc(), pos};
////                        nochangecorr.add(va);
////
////                    }
////
////                }
////            }
////
////            removeDuplicatesALAr(ti);
////            removeDuplicatesALAr(nochangecorr);
////
////            for (int x = 0; x < nochangecorr.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
//////                        pos = "99";
////                        pos = nc_list.get(i).getPos();
//////                        System.out.println(nc_list.get(i).getPos());
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////
////                    if (nc_list.get(i).getGruppo_nc().equals(nochangecorr.get(x)[0])
////                            & nc_list.get(i).getFiliale().equals(filiale.getCod())
////                            && pos.equals(nochangecorr.get(x)[1])) {
////                        NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc());
////                        if (nc1 != null) {
////                            if (!nc1.getDe_causale_nc().toUpperCase().contains("ACQUISTO")) {
////                                importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                            }
////                        }
////                    }
////
////                }
////                String[] va = {nochangecorr.get(x)[0], nochangecorr.get(x)[1], roundDoubleandFormat(importo, 2)};
////                nochangecorr_value.add(va);
////            }
////
////            //NUOVO ASPETTARE RAGGI
////            for (int x = 0; x < nochangecorr_value.size(); x++) {
////                String valori[] = nochangecorr_value.get(x);
////                NC_category nc0 = getNC_category(listcategory, valori[0]);
////                if (nc0 != null) {
////                    VATcode va1 = get_vat(nc0.getConto_coge_02(), vat);
////                    if (va1 != null) {
////                        double total1 = fd(valori[2]);
////                        double imposta1 = calcolaIva(total1, fd(va1.getAliquota()));
////                        double imponibile1 = total1 - fd(roundDoubleandFormat(imposta1, 2));
////                        String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "").trim();
////                        String imposta = StringUtils.replace(StringUtils.replace(roundDoubleandFormat(imposta1, 2), ".", ","), "-", "").trim();
////                        String imponibile = StringUtils.replace(StringUtils.replace(roundDoubleandFormat(imponibile1, 2), ".", ","), "-", "").trim();
////                        String contocassa;
////                        String codicecassa = "";
////                        if (valori[1].equals("00")) {
////                            contocassa = this.codiceNegozi;
////                            codicecassa = filiale.getCod();
////                        } else if (valori[1].equals("99")) {
////                            contocassa = conto_RVE;
////                        } else {
////                            contocassa = formatAL(valori[1], bank, 2);
////                            codicecassa = valori[1];
////                        }
////                        ticket_sc_corrispettivo("CTI", anno, data, String.valueOf(nreg),
////                                nc0.getConto_coge_01(), importo, imponibile, imposta, contocassa, codicecassa,
////                                filiale.getCod(), filiale.getDe_branch(), nc0.getDe_gruppo_nc(), nc0.getConto_coge_02(), writer);
////                        nreg++;
////                    }
////
////                }
////
////            }
////
////            for (int x = 0; x < ti.size(); x++) {
////                double importo = 0.00;
////
////                double fee = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
////                        pos = nc_list.get(i).getPos();
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////
////                    String comm;
////                    if (fd(nc_list.get(i).getCommissione()) > 0) {
////                        comm = nc_list.get(i).getCommissione();
////                    } else {
////                        comm = nc_list.get(i).getTi_ticket_fee();
////                    }
////
////                    boolean add = fd(nc_list.get(i).getTotal()) >= 0;
////
////                    if (nc_list.get(i).getGruppo_nc().equals(ti.get(x)[0]) & nc_list.get(i).getFiliale().equals(filiale.getCod()) && pos.equals(ti.get(x)[1])) {
////
////                        if (add) {
////                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                            fee = fee + parseDoubleR(comm);
////                        } else {
////                            importo = importo - parseDoubleR(nc_list.get(i).getTotal());
////                            fee = fee - parseDoubleR(comm);
////                        }
////
////                    }
////                }
////
////                VATcode va1 = get_vat("22", vat);
////
////                double valueiva = fd(va1.getAliquota());
////                double d1 = calcolaIva(fee, valueiva);
////                double d2 = fee - fd(roundDoubleandFormat(d1, 2));
////
////                String[] va = {ti.get(x)[0], ti.get(x)[1],
////                    roundDoubleandFormat(importo, 2),
////                    roundDoubleandFormat(fee, 2),
////                    roundDoubleandFormat(importo - fee, 2),
////                    roundDoubleandFormat(d1, 2),
////                    roundDoubleandFormat(d2, 2),
////                    roundDoubleandFormat(valueiva, 0)
////                };
////                ti_value.add(va);
////            }
////
////            for (int x = 0; x < ti_value.size(); x++) {
////                String valori[] = ti_value.get(x);
////                String importo = StringUtils.replace(valori[2], ".", ",").trim();
////                String fee = StringUtils.replace(valori[3], ".", ",").trim();
////                String net = StringUtils.replace(valori[4], ".", ",").trim();
////                String imp1 = StringUtils.replace(valori[6], ".", ",").trim();
////                String netimposta = StringUtils.replace(valori[5], ".", ",").trim();
//////                String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "").trim();
//////                String fee = StringUtils.replace(StringUtils.replace(valori[3], ".", ","), "-", "").trim();
//////                String net = StringUtils.replace(StringUtils.replace(valori[4], ".", ","), "-", "").trim();
//////                String imp1 = StringUtils.replace(StringUtils.replace(valori[6], ".", ","), "-", "").trim();
//////                String netimposta = StringUtils.replace(StringUtils.replace(valori[5], ".", ","), "-", "").trim();
////
////                NC_category nc0 = getNC_category(listcategory, valori[0]);
////                if (nc0 != null) {
////                    if (valori[1].equals("00")) {
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////                        ticket_new("TCC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                fee, valori[7],
////                                imp1, netimposta, importo,
////                                net,
////                                nc0.getConto_coge_02(),
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                this.codiceNegozi, filiale.getCod(), writer, scontrino_NORM);
////                        nreg++;
////                    } else if (valori[1].equals("99")) {
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////                        ticket_new("TCC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                fee, valori[7],
////                                imp1, netimposta, importo,
////                                net,
////                                nc0.getConto_coge_02(),
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                conto_RVE,
////                                "",
////                                writer, scontrino_NORM);
////                        nreg++;
////                    } else {
////                        writer.println(tag_TES + separator + scontrino_NORM + separator + anno + separator + data + separator + nreg
////                                + separator + separator + separator + separator + separator + separator
////                                + separator + separator + separator + separator + separator + separator + separator
////                                + "Corrispettivi " + filiale.getDe_branch() + separator);
////                        ticket_new("TCC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                fee, valori[7],
////                                imp1, netimposta, importo,
////                                net,
////                                nc0.getConto_coge_02(),
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                formatAL(valori[1], bank, 2),
////                                valori[1],
////                                writer, scontrino_NORM);
////                        nreg++;
////                    }
////                }
////            }
////            writer.close();
////            if (nreg > 1) {
////                return f;
////            }
////            f.delete();
////        } catch (IOException ex) {
////            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
////        }
////        return null;
////    }
////
////    public File FILEP1(String path, String data, String anno,
////            ArrayList<Ch_transaction> ch_list,
////            ArrayList<NC_transaction> nc_list,
////            ArrayList<Ch_transaction_refund> list_esolver_refund,
////            ArrayList<ET_change> et_list,
////            ArrayList<Openclose> oc_list,
////            ArrayList<NC_category> listcategory,
////            ArrayList<NC_causal> listcausal,
////            ArrayList<Users> listusers,
////            Branch filiale,
////            ArrayList<String[]> contabilita_codici,
////            ArrayList<String[]> bank,
////            String valuta_locale,
////            ArrayList<Branch> branch, boolean dividi,
////            CustomerKind ck) {
////
////        try {
////            String conto_RVE = formatAL("RVE", contabilita_codici, 2);
////            File f = new File(path + filiale.getCod() + "_" + StringUtils.replace(data, "/", "") + "_P1_eSol.txt");
////            PrintWriter writer = new PrintWriter(f);
////            int nreg = 1;
////
////            //SELL NO COMM
////            ArrayList<String> se = new ArrayList<>();
////            ArrayList<String[]> se_value_temp = new ArrayList<>();
////            ArrayList<String[]> se_value = new ArrayList<>();
////
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("S")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////                    String pos;
////                    if (ch_list.get(i).getLocalfigures().equals("01")) {
////                        pos = "00";
////                    } else if (ch_list.get(i).getLocalfigures().equals("08")) {
////                        pos = ch_list.get(i).getPos();
////                    } else {
////                        pos = ch_list.get(i).getPos();
////                    }
////                    if (!valori.isEmpty()) {
////                        se.add(pos);
////                        for (int x = 0; x < valori.size(); x++) {
////                            String[] tmp = {pos, valori.get(x).getNet(), valori.get(x).getTotal(), valori.get(x).getTot_com(), valori.get(x).getRoundvalue(),
////                                valori.get(x).getSpread()};
////                            se_value_temp.add(tmp);
////                        }
////                    }
////                }
////            }
////
////            removeDuplicatesAL(se);
////            for (int x = 0; x < se.size(); x++) {
////
////                double importo_spread = 0.00;
////
////                double importo_comm = 0.00;
////                double importo_net = 0.00;
////
////                for (int i = 0; i < se_value_temp.size(); i++) {
////                    if (se.get(x).equals(se_value_temp.get(i)[0])) {
////                        importo_comm = importo_comm + parseDoubleR(se_value_temp.get(i)[3]) + parseDoubleR(se_value_temp.get(i)[4]);
////                        importo_net = importo_net + parseDoubleR(se_value_temp.get(i)[1]);
////
////                        importo_spread = importo_spread + fd(se_value_temp.get(i)[5]);
////                    }
////                }
////                String[] tmp = {
////                    se.get(x),
////                    roundDoubleandFormat(importo_comm, 2),
////                    roundDoubleandFormat(importo_net, 2),
////                    roundDoubleandFormat(importo_spread, 2)
////
////                };
////                se_value.add(tmp);
////            }
////
////            for (int x = 0; x < se_value.size(); x++) {
////                String[] valore = se_value.get(x);
////
////                double corrisp = fd(valore[1]);
////
////                double spr_dbl = fd(valore[3]);
////
////                double nuovoimporto = fd(valore[2]) + fd(valore[1]);
////                if (corrisp == 0) {
////                    String newimp = StringUtils.replace(roundDoubleandFormat(nuovoimporto, 2), ".", ",");
////                    String net = StringUtils.replace(roundDoubleandFormat(nuovoimporto - spr_dbl, 2), ".", ",");
////                    String spread = StringUtils.replace(roundDoubleandFormat(spr_dbl, 2), ".", ",");
////
////                    if (valore[0].equals("00")) {
////                        sell("N_SSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                StringUtils.replace(valore[2], ".", ","),
////                                StringUtils.replace(valore[1], ".", ","),
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                this.codiceNegozi, filiale.getCod(), ck.getVatcode(), writer,
////                                newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),
////                                net, spread
////                        );
////                        nreg++;
////                    } else if (valore[0].equals("99")) {
////                        sell("N_SSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                StringUtils.replace(valore[2], ".", ","), StringUtils.replace(valore[1], ".", ","),
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                conto_RVE,
////                                valore[0], ck.getVatcode(), writer, newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),//SOSTITUIRE
////                                net, spread);
////
////                        nreg++;
////                    } else {
////                        sell("N_SSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                StringUtils.replace(valore[2], ".", ","), StringUtils.replace(valore[1], ".", ","),
////                                filiale.getDe_branch(),
////                                formatAL("CIVV", contabilita_codici, 2),
////                                conto_RVE,
////                                formatAL(valore[0], bank, 2),
////                                valore[0],
////                                ck.getVatcode(), writer, newimp, scontrino_NORM,
////                                formatAL("SVVEC", contabilita_codici, 2),//SOSTITUIRE
////                                net, spread);
////                        nreg++;
////                    }
////                }
////            }
////
////            //BUY
////            double tot1 = 0.00;
////            double comm1 = 0.00;
////            double net1 = 0.00;
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("B")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////                    if (!valori.isEmpty()) {
////                        for (int x = 0; x < valori.size(); x++) {
////                            Ch_transaction_value val = valori.get(x);
////                            if (!val.getSupporto().equals("04")) {
////                                tot1 = tot1 + parseDoubleR(val.getTotal());
////                                comm1 = comm1 + parseDoubleR(val.getTot_com()) + parseDoubleR(val.getRoundvalue());
////                                net1 = net1 + parseDoubleR(val.getNet());
////                            }
////                        }
////                    }
////                }
////            }
////
////            if (tot1 > 0 && comm1 == 0) {
////                buy("BSC", anno, data,
////                        String.valueOf(nreg),
////                        filiale.getCod(),
////                        StringUtils.replace(roundDoubleandFormat(net1, 2), ".", ","),
////                        formatAL("CAV", contabilita_codici, 2),
////                        StringUtils.replace(roundDoubleandFormat(comm1, 2), ".", ","),
////                        conto_RVE,
////                        StringUtils.replace(roundDoubleandFormat(tot1, 2), ".", ","), ck.getVatcode(), writer, scontrino_NORM);
////                nreg++;
////            }
////
////            //writer.println("TES;;" + anno + ";" + data + ";" + nreg + ";;;;;;;");
////            //ERRORI CASSA
////            double errori_cassa_pos = 0.00;
////            double errori_cassa_neg = 0.00;
////
////            for (int i = 0; i < oc_list.size(); i++) {
////                if (oc_list.get(i).getFiliale().equals(filiale.getCod())) {
////                    ArrayList<String[]> list_oc_errors = list_oc_errors(oc_list.get(i).getCod());
////
////                    for (int c = 0; c < list_oc_errors.size(); c++) {
////                        String[] value = list_oc_errors.get(c);
////
////                        if (value[1].equals("CH") && value[2].equals(valuta_locale)) {
////                            String diff = formatDoubleforMysql(getValueDiff(value[11], value[13], value[7], value[8], dividi));
////                            if (fd(diff) > 0) {
////                                errori_cassa_pos = errori_cassa_pos + parseDoubleR(diff);
////                            } else {
////                                errori_cassa_neg = errori_cassa_neg + parseDoubleR(diff);
////                            }
////                        }
////                    }
////                }
////            }
////
////            boolean erpos = false;
////            boolean erneg = false;
////
////            if (errori_cassa_pos > errori_cassa_neg) {
////                errori_cassa_pos = errori_cassa_pos - errori_cassa_neg;
////                erpos = true;
////            } else if (errori_cassa_pos < errori_cassa_neg) {
////                errori_cassa_neg = errori_cassa_neg - errori_cassa_pos;
////                erneg = true;
////            }
////
////            if (erpos) {
////                erroriCassa("ERA", anno, data,
////                        String.valueOf(nreg),
////                        formatAL("EPT", contabilita_codici, 2),
////                        StringUtils.replace(roundDoubleandFormat(errori_cassa_pos, 2), ".", ","),
////                        filiale.getDe_branch(), filiale.getCod(), writer);
////
////                nreg++;
////            } else if (erneg) {
////                erroriCassa("ERR", anno, data,
////                        String.valueOf(nreg),
////                        formatAL("ENT", contabilita_codici, 2),
////                        StringUtils.replace(roundDoubleandFormat(errori_cassa_neg, 2), ".", ","),
////                        filiale.getDe_branch(), filiale.getCod(), writer);
////                nreg++;
////            }
////
////            //RIMBORSI
////            for (int i = 0; i < list_esolver_refund.size(); i++) {
////                if (list_esolver_refund.get(i).getBranch_cod().equals(filiale.getCod())) {
////                    String importo = StringUtils.replace(list_esolver_refund.get(i).getValue(), "-", "");
////                    rimborso("RIB", anno, data,
////                            String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(importo, ".", ","),
////                            filiale.getDe_branch(),
////                            formatAL("CORI", contabilita_codici, 2), writer);
////                    nreg++;
////                }
////            }
////
////            //TO BRANCH
////            ArrayList<String> to_br = new ArrayList<>();
////            ArrayList<String[]> to_br_value = new ArrayList<>();
////
////            for (int i = 0; i < et_list.size(); i++) {
////                if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                        && et_list.get(i).getFg_tofrom().equals("T")
////                        && et_list.get(i).getFg_brba().equals("BR")) {
////                    to_br.add(et_list.get(i).getCod_dest());
////                }
////            }
////            removeDuplicatesAL(to_br);
////
////            for (int x = 0; x < to_br.size(); x++) {
////                double importo_val_loc = 0.00;
////                for (int i = 0; i < et_list.size(); i++) {
////                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                            && et_list.get(i).getFg_tofrom().equals("T")
////                            && et_list.get(i).getFg_brba().equals("BR")
////                            && et_list.get(i).getCod_dest().equals(to_br.get(x))) {
////                        ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
////                        if (!valori.isEmpty()) {
////                            for (int z = 0; z < valori.size(); z++) {
////                                if (valori.get(z).getValuta().equals(valuta_locale) && valori.get(z).getSupporto().equals("01")) {
////                                    importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
////                                }
////                            }
////                        }
////                    }
////                }
////
////                String[] val = {to_br.get(x), roundDoubleandFormat(importo_val_loc, 2)};
////                to_br_value.add(val);
////            }
////
////            for (int i = 0; i < to_br_value.size(); i++) {
////                String valore[] = to_br_value.get(i);
////
////                if (parseDoubleR(valore[1]) > 0) { //LOCALE
////                    extBranch("TBR", anno, data,
////                            String.valueOf(nreg), valore[0],
////                            filiale.getCod(),
////                            StringUtils.replace(valore[1], ".", ","),
////                            filiale.getDe_branch(),
////                            formatBankBranchReport(valore[0], "BR", null, branch), writer);
////                    nreg++;
////                }
////            }
////
////            //TO BANK
////            ArrayList<String> to_ba = new ArrayList<>();
////            ArrayList<String[]> to_ba_value = new ArrayList<>();
////
////            for (int i = 0; i < et_list.size(); i++) {
////                if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                        && et_list.get(i).getFg_tofrom().equals("T")
////                        && et_list.get(i).getFg_brba().equals("BA")) {
////                    to_ba.add(et_list.get(i).getCod_dest());
////                }
////            }
////
////            removeDuplicatesAL(to_ba);
////
////            for (int x = 0; x < to_ba.size(); x++) {
////
////                double importo_val_loc = 0.00;
////                double importo_val_est = 0.00;
////                double importo_spread = 0.00;
////
////                for (int i = 0; i < et_list.size(); i++) {
////                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                            && et_list.get(i).getFg_tofrom().equals("T")
////                            && et_list.get(i).getFg_brba().equals("BA")
////                            && et_list.get(i).getCod_dest().equals(to_ba.get(x))) {
////                        ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
////                        if (!valori.isEmpty()) {
////                            for (int z = 0; z < valori.size(); z++) {
////                                if (valori.get(z).getValuta().equals(valuta_locale) && valori.get(z).getSupporto().equals("01")) {
////                                    importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
////                                } else {
////                                    importo_val_est = importo_val_est + parseDoubleR(valori.get(z).getIp_total());
////                                }
////
////                                importo_spread = importo_spread + fd(valori.get(z).getIp_spread());
////
////                            }
////                        }
////                    }
////                }
////                String[] val = {to_ba.get(x), roundDoubleandFormat(importo_val_loc, 2), roundDoubleandFormat(importo_val_est, 2), roundDoubleandFormat(importo_spread, 2)};
////                to_ba_value.add(val);
////            }
////
////            for (int i = 0; i < to_ba_value.size(); i++) {
////                String valore[] = to_ba_value.get(i);
////
////                if (parseDoubleR(valore[1]) > 0) { //LOCALE
////
////                    extBank("TBL", anno, data,
////                            String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(valore[1], ".", ","),
////                            formatAL(valore[0], bank, 1),
////                            formatAL(valore[0], bank, 2),
////                            valore[0],
////                            this.codiceNegozi,
////                            writer, true, null, null, null);
////                    nreg++;
////                }
////                if (parseDoubleR(valore[2]) > 0) { //ESTERA
////
////                    double spr_dbl = fd(valore[3]);
////                    double net_dbl = fd(valore[2]) - spr_dbl;
////
////                    extBank("N_TBE", anno, data,
////                            String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(valore[2], ".", ","),
////                            formatAL(valore[0], bank, 1),
////                            formatAL(valore[0], bank, 2),
////                            valore[0],
////                            conto_RVE,
////                            writer, false,
////                            formatAL("SVVEB", contabilita_codici, 2),//SOSTITUIRE
////                            StringUtils.replace(roundDoubleandFormat(net_dbl, 2), ".", ","),
////                            StringUtils.replace(roundDoubleandFormat(spr_dbl, 2), ".", ",")
////                    );
////                    nreg++;
////                }
////            }
////
////            //FROM BANK
////            ArrayList<String> fr_ba = new ArrayList<>();
////            ArrayList<String[]> fr_ba_value = new ArrayList<>();
////
////            for (int i = 0; i < et_list.size(); i++) {
////                if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                        && et_list.get(i).getFg_tofrom().equals("F")
////                        && et_list.get(i).getFg_brba().equals("BA")) {
////                    fr_ba.add(et_list.get(i).getCod_dest());
////                }
////            }
////
////            removeDuplicatesAL(fr_ba);
////
////            for (int x = 0; x < fr_ba.size(); x++) {
////
////                double importo_val_loc = 0.00;
////                double importo_val_est = 0.00;
////
////                for (int i = 0; i < et_list.size(); i++) {
////                    if (et_list.get(i).getFiliale().equals(filiale.getCod())
////                            && et_list.get(i).getFg_tofrom().equals("F")
////                            && et_list.get(i).getFg_brba().equals("BA")
////                            && et_list.get(i).getCod_dest().equals(fr_ba.get(x))) {
////                        ArrayList<ET_change> valori = get_ET_change_value(et_list.get(i).getCod());
////                        if (!valori.isEmpty()) {
////                            for (int z = 0; z < valori.size(); z++) {
////                                if (valori.get(z).getValuta().equals(valuta_locale) && valori.get(z).getSupporto().equals("01")) {
////                                    importo_val_loc = importo_val_loc + parseDoubleR(valori.get(z).getIp_total());
////                                } else {
////                                    importo_val_est = importo_val_est + parseDoubleR(valori.get(z).getIp_total());
////                                }
////                            }
////                        }
////                    }
////                }
////                String[] val = {fr_ba.get(x), roundDoubleandFormat(importo_val_loc, 2), roundDoubleandFormat(importo_val_est, 2)};
////                fr_ba_value.add(val);
////            }
////
////            for (int i = 0; i < fr_ba_value.size(); i++) {
////                String valore[] = fr_ba_value.get(i);
////
////                if (parseDoubleR(valore[1]) > 0) { //LOCALE
////
////                    extBank("FBL", anno, data,
////                            String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(valore[1], ".", ","),
////                            formatAL(valore[0], bank, 1),
////                            formatAL(valore[0], bank, 2),
////                            //                            Utility.formatAL("BANC", contabilita_codici, 2),
////                            valore[0],
////                            this.codiceNegozi,
////                            writer, true, null, null, null);
////                    nreg++;
////                }
////                if (parseDoubleR(valore[2]) > 0) { //ESTERA
////
////                    extBank("FBE", anno, data,
////                            String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(valore[2], ".", ","),
////                            formatAL(valore[0], bank, 1),
////                            formatAL(valore[0], bank, 2),
////                            valore[0],
////                            conto_RVE,
////                            writer, false, null, null, null);
////                    nreg++;
////                }
////            }
////
////            //BUY  - CASH ADVANCE - SENZA COMMISSIONI
////            ArrayList<String> b_ca_sc = new ArrayList<>();
////            ArrayList<String[]> b_ca_sc_value_temp = new ArrayList<>();
////            ArrayList<String[]> b_ca_sc_value = new ArrayList<>();
////            for (int i = 0; i < ch_list.size(); i++) {
////                if (ch_list.get(i).getFiliale().equals(filiale.getCod()) && ch_list.get(i).getTipotr().equals("B")) {
////                    ArrayList<Ch_transaction_value> valori = query_transaction_value(ch_list.get(i).getCod());
////                    if (!valori.isEmpty()) {
////                        for (int x = 0; x < valori.size(); x++) {
////                            Ch_transaction_value val = valori.get(x);
////                            if (val.getSupporto().equals("04")) {
////                                if (parseDoubleR(val.getTot_com())
////                                        + parseDoubleR(val.getRoundvalue())
////                                        == 0.0D) {
////                                    b_ca_sc.add(val.getPos());
////                                    String[] tmp = {val.getPos(), val.getNet()};
////                                    b_ca_sc_value_temp.add(tmp);
////                                }
////                            }
////                        }
////                    }
////                }
////            }
////
////            removeDuplicatesAL(b_ca_sc);
////            for (int x = 0; x < b_ca_sc.size(); x++) {
////                double importo_b_ca_sc = 0.00;
////                for (int i = 0; i < b_ca_sc_value_temp.size(); i++) {
////                    if (b_ca_sc.get(x).equals(b_ca_sc_value_temp.get(i)[0])) {
////                        importo_b_ca_sc = importo_b_ca_sc + parseDoubleR(b_ca_sc_value_temp.get(i)[1]);
////                    }
////                }
////                String[] tmp = {b_ca_sc.get(x), roundDoubleandFormat(importo_b_ca_sc, 2)};
////                b_ca_sc_value.add(tmp);
////            }
////
////            for (int x = 0; x < b_ca_sc_value.size(); x++) {
////                String[] valore = b_ca_sc_value.get(x);
////
////                cashAdvance("CAD", anno, data,
////                        String.valueOf(nreg),
////                        formatAL(valore[0], bank, 2),
////                        StringUtils.replace(valore[1], ".", ","), null, null,
////                        filiale.getDe_branch(),
////                        filiale.getCod(),
////                        conto_RVE,
////                        "", ck.getVatcode(), writer, scontrino_NORM);
////
////                nreg++;
////            }
////
////            //WESTERN UNION
////            ArrayList<String[]> wu = new ArrayList<>();
////            ArrayList<String[]> wu_value = new ArrayList<>();
////
////            ArrayList<String[]> nc = new ArrayList<>();
////            ArrayList<String[]> nc_value = new ArrayList<>();
////            ArrayList<String[]> tisc = new ArrayList<>();
////            ArrayList<String[]> tisc_value = new ArrayList<>();
////
////            for (int i = 0; i < nc_list.size(); i++) {
////                if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getFg_tipo_transazione_nc().equals("1")) {
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
////                        pos = nc_list.get(i).getPos();
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////                    String[] va = {nc_list.get(i).getCausale_nc(), pos};
////                    wu.add(va);
////                } else if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getFg_tipo_transazione_nc().equals("21")) { //ticket senzacomm
////                    String comm;
////                    if (fd(nc_list.get(i).getCommissione()) > 0) {
////                        comm = nc_list.get(i).getCommissione();
////                    } else {
////                        comm = nc_list.get(i).getTi_ticket_fee();
////                    }
////                    comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
////                    if (parseDoubleR(comm) == 0.0D) {
////                        String pos;
////                        if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                            pos = "00";
////                        } else if (nc_list.get(i).getSupporto().equals("08")) {
////                            pos = nc_list.get(i).getPos();
////                        } else {
////                            pos = nc_list.get(i).getPos();
////                        }
////                        String[] va = {nc_list.get(i).getGruppo_nc(), pos};
////                        tisc.add(va);
////                    }
////                } else if (nc_list.get(i).getFiliale().equals(filiale.getCod())
////                        && (nc_list.get(i).getFg_tipo_transazione_nc().equals("2") || nc_list.get(i).getFg_tipo_transazione_nc().equals("4")
////                        || nc_list.get(i).getFg_tipo_transazione_nc().equals("5"))) {
////
////                    NC_category nc0 = getNC_category(listcategory, nc_list.get(i).getGruppo_nc());
////                    NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc());
////                    if (nc0 != null && nc1 != null) {
////                        if (!nc1.getDe_causale_nc().toUpperCase().contains("ACQUISTO")) {
////                            if (!nc0.getConto_coge_01().trim().equals("") && nc0.getConto_coge_02().trim().equals("")) {
////                                String pos;
////                                if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                                    pos = "00";
////                                } else if (nc_list.get(i).getSupporto().equals("08")) {
////                                    pos = nc_list.get(i).getPos();
////                                } else {
////                                    pos = nc_list.get(i).getPos();
////                                }
////                                String[] va = {nc_list.get(i).getGruppo_nc(), pos};
////                                nc.add(va);
////                            }
////                        }
////                    }
////                }
////
////            }
////
////            removeDuplicatesALAr(wu);
////            removeDuplicatesALAr(tisc);
////            removeDuplicatesALAr(nc);
////
////            for (int x = 0; x < tisc.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
////                        pos = nc_list.get(i).getPos();
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////                    if (nc_list.get(i).getGruppo_nc().equals(tisc.get(x)[0])
////                            && nc_list.get(i).getFiliale().equals(filiale.getCod())
////                            && pos.equals(tisc.get(x)[1])) {
////                        String comm;
////                        if (fd(nc_list.get(i).getCommissione()) > 0) {
////                            comm = nc_list.get(i).getCommissione();
////                        } else {
////                            comm = nc_list.get(i).getTi_ticket_fee();
////                        }
////                        comm = StringUtils.replace(StringUtils.replace(comm, ".", ","), "-", "").trim();
////                        if (parseDoubleR(comm) == 0.0D) {
////                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                        }
////                    }
////                }
////                String[] va = {tisc.get(x)[0], tisc.get(x)[1], roundDoubleandFormat(importo, 2),};
////                tisc_value.add(va);
////            }
////
////            for (int x = 0; x < wu.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
////                        pos = nc_list.get(i).getPos();
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////                    if (nc_list.get(i).getCausale_nc().equals(wu.get(x)[0]) && nc_list.get(i).getFiliale().equals(filiale.getCod()) && pos.equals(wu.get(x)[1])) {
////                        importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                    }
////                }
////                String[] va = {wu.get(x)[0], wu.get(x)[1], roundDoubleandFormat(importo, 2)};
////                wu_value.add(va);
////            }
////
////            for (int x = 0; x < nc.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    NC_causal nc1 = getNC_causal(listcausal, nc_list.get(i).getCausale_nc());
////                    String pos;
////                    if (nc_list.get(i).getSupporto().equals("01") || nc_list.get(i).getSupporto().equals("...")) {
////                        pos = "00";
////                    } else if (nc_list.get(i).getSupporto().equals("08")) {
////                        pos = nc_list.get(i).getPos();
////                    } else {
////                        pos = nc_list.get(i).getPos();
////                    }
////                    if (nc_list.get(i).getGruppo_nc().equals(nc.get(x)[0]) && nc_list.get(i).getFiliale().equals(filiale.getCod()) && pos.equals(nc.get(x)[1])) {
////                        if (nc1.getNc_de().equals("09") || nc1.getNc_de().equals("15") || nc1.getNc_de().equals("05") || nc1.getNc_de().equals("17")) {
////                            importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                        } else if (nc1.getNc_de().equals("10") || nc1.getNc_de().equals("16") || nc1.getNc_de().equals("06") || nc1.getNc_de().equals("18")) {
////                            importo = importo - parseDoubleR(nc_list.get(i).getTotal());
////                        }
////                    }
////                }
////                String[] va = {nc.get(x)[0], nc.get(x)[1], roundDoubleandFormat(importo, 2)};
////                nc_value.add(va);
////            }
////
////            for (int x = 0; x < nc_value.size(); x++) {
////                String valori[] = nc_value.get(x);
////                NC_category nc0 = getNC_category(listcategory, valori[0]);
////                if (nc0 != null) {
////
////                    String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "").trim();
////                    if (valori[1].equals("00")) { //CONTANTI
////                        if (fd(valori[2]) > 0) {
////                            noChange("NCV", anno, data,
////                                    String.valueOf(nreg),
////                                    filiale.getCod(),
////                                    importo,
////                                    nc0.getConto_coge_01(),
////                                    //"470106", //nc0.getConto_coge_01(), SOSTITUIRE
////                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                    null, null, writer);
////                            nreg++;
////                        } else {
////                            noChange("NCR", anno, data,
////                                    String.valueOf(nreg),
////                                    filiale.getCod(),
////                                    importo,
////                                    nc0.getConto_coge_01(),
////                                    nc0.getDe_gruppo_nc() + " RETTIFICA - " + filiale.getDe_branch(),
////                                    null, null, writer);
////                            nreg++;
////                        }
////
////                    } else if (valori[1].equals("99")) {
////                        if (fd(valori[2]) > 0) {
////                            noChange("NCV", anno, data,
////                                    String.valueOf(nreg),
////                                    filiale.getCod(),
////                                    importo,
////                                    nc0.getConto_coge_01(),
////                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                    conto_RVE, "", writer);
////                            nreg++;
////                        }
////                    } else {
////                        if (fd(valori[2]) > 0) {
////                            noChange("NCV", anno, data,
////                                    String.valueOf(nreg),
////                                    filiale.getCod(),
////                                    importo,
////                                    nc0.getConto_coge_01(), //nc0.getConto_coge_01(), SOSTITUIRE
////                                    nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                    formatAL(valori[1], bank, 2),
////                                    //"150524", //Utility.formatAL(valori[1], bank, 2), SOSTITUIRE
////                                    "", writer);
////                            nreg++;
////                        }
////                    }
////
////                }
////
////            }
////
////            for (int x = 0; x < tisc_value.size(); x++) {
////                String valori[] = tisc_value.get(x);
////                String importo = StringUtils.replace(StringUtils.replace(valori[2], ".", ","), "-", "").trim();
////                NC_category nc0 = getNC_category(listcategory, valori[0]);
////                if (nc0 != null) {
////                    if (valori[1].equals("00")) {
////                        ticket("TSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                importo,
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                null, null, writer);
////                        nreg++;
////                    } else if (valori[1].equals("99")) {
////                        ticket("TSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                importo,
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                conto_RVE,
////                                "", writer);
////                        nreg++;
////                    } else {
////                        ticket("TSC", anno, data, String.valueOf(nreg),
////                                filiale.getCod(),
////                                importo,
////                                nc0.getConto_coge_01(),
////                                nc0.getDe_gruppo_nc() + " " + filiale.getDe_branch(),
////                                formatAL(valori[1], bank, 2),
////                                "", writer);
////                        nreg++;
////                    }
////                }
////
////            }
////
////            for (int x = 0; x < wu_value.size(); x++) {
////                String valori[] = wu_value.get(x);
////                NC_causal nc1 = getNC_causal(listcausal, valori[0]);
////                if (nc1 != null) {
////                    NC_category nc0 = getNC_category(listcategory, nc1.getGruppo_nc());
////                    if (nc0 != null) {
////                        if (nc1.getFg_in_out().equals("2")) { //OUT - RECEIVE
////                            westernUnion("WUR", anno, data,
////                                    String.valueOf(nreg),
////                                    filiale.getCod(),
////                                    StringUtils.replace(valori[2], ".", ","),
////                                    nc0.getConto_coge_01(),
////                                    nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                    "", "", writer);
////                            nreg++;
////                        } else if (nc1.getFg_in_out().equals("1")) { //IN - SEND
////                            if (valori[1].equals("00")) { //CONTANTI
////                                westernUnion("WUS", anno, data,
////                                        String.valueOf(nreg),
////                                        filiale.getCod(),
////                                        StringUtils.replace(valori[2], ".", ","),
////                                        nc0.getConto_coge_01(),
////                                        nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                        null, null, writer);
////                                nreg++;
////                            } else if (valori[1].equals("99")) { //BANK ACCOUNT
////                                westernUnion("WUS", anno, data,
////                                        String.valueOf(nreg),
////                                        filiale.getCod(),
////                                        StringUtils.replace(valori[2], ".", ","),
////                                        nc0.getConto_coge_01(),
////                                        nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                        conto_RVE, "", writer);
////                                nreg++;
////                            } else { //POS
////                                westernUnion("WUS", anno, data,
////                                        String.valueOf(nreg),
////                                        filiale.getCod(),
////                                        StringUtils.replace(valori[2], ".", ","),
////                                        nc0.getConto_coge_01(),
////                                        nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                        formatAL(valori[1], bank, 2),
////                                        "", writer);
////                                nreg++;
////                            }
////                        }
////                    }
////                }
////            }
////
////            //VAT REFUND
////            ArrayList<String> vat_refund = new ArrayList<>();
////            ArrayList<String[]> vat_refund_value = new ArrayList<>();
////            for (int i = 0; i < nc_list.size(); i++) {
////                if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getFg_tipo_transazione_nc().equals("3")) {
////                    vat_refund.add(nc_list.get(i).getGruppo_nc());
////                }
////            }
////            removeDuplicatesAL(vat_refund);
////            for (int x = 0; x < vat_refund.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    if (nc_list.get(i).getGruppo_nc().equals(vat_refund.get(x)) && nc_list.get(i).getFiliale().equals(filiale.getCod())) {
////                        importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                    }
////                }
////                String[] va = {vat_refund.get(x), roundDoubleandFormat(importo, 2)};
////                vat_refund_value.add(va);
////            }
////            for (int x = 0; x < vat_refund_value.size(); x++) {
////                String valori[] = vat_refund_value.get(x);
////                NC_category nc1 = getNC_category(listcategory, valori[0]);
////                if (nc1 != null) {
////                    vatRefound("VAT", anno, data, String.valueOf(nreg),
////                            filiale.getCod(),
////                            StringUtils.replace(valori[1], ".", ","),
////                            nc1.getConto_coge_01(),
////                            nc1.getDe_gruppo_nc() + " " + filiale.getDe_branch(), writer);
////                    nreg++;
////                }
////            }
////
////            //ANTICIPO DIP
////            ArrayList<String[]> anticipo_dip = new ArrayList<>();
////            ArrayList<String[]> anticipo_dip_value = new ArrayList<>();
////
////            for (int i = 0; i < nc_list.size(); i++) {
////                if (nc_list.get(i).getFiliale().equals(filiale.getCod()) && nc_list.get(i).getFg_tipo_transazione_nc().equals("6")) {
////                    String[] va = {nc_list.get(i).getUser(), nc_list.get(i).getCausale_nc()};
////                    anticipo_dip.add(va);
////                }
////
////            }
////            removeDuplicatesALAr(anticipo_dip);
////
////            for (int x = 0; x < anticipo_dip.size(); x++) {
////                double importo = 0.00;
////                for (int i = 0; i < nc_list.size(); i++) {
////                    if (nc_list.get(i).getUser().equals(anticipo_dip.get(x)[0])
////                            && nc_list.get(i).getCausale_nc().equals(anticipo_dip.get(x)[1])
////                            && nc_list.get(i).getFiliale().equals(filiale.getCod())) {
////                        importo = importo + parseDoubleR(nc_list.get(i).getTotal());
////                    }
////                }
////                String[] va = {anticipo_dip.get(x)[0], anticipo_dip.get(x)[1], roundDoubleandFormat(importo, 2)};
////                anticipo_dip_value.add(va);
////            }
////
////            for (int x = 0; x < anticipo_dip_value.size(); x++) {
////                String valori[] = anticipo_dip_value.get(x);
////                NC_causal nc1 = getNC_causal(listcausal, valori[1]);
////                if (nc1 != null) {
////                    NC_category nc0 = getNC_category(listcategory, nc1.getGruppo_nc());
////                    Users us = get_user(valori[0], listusers);
////                    if (nc1.getFg_in_out().equals("2")) { //OUT - AND
////                        anticipoDipendenti("AND",
////                                anno, data,
////                                String.valueOf(nreg),
////                                us.getConto(),
////                                us.getCod(),
////                                StringUtils.replace(valori[2], ".", ","),
////                                us.getCod() + " " + nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                filiale.getCod(), writer, nc0.getConto_coge_01());
////                        nreg++;
////                    } else if (nc1.getFg_in_out().equals("1")) { //IN - ADR
////                        anticipoDipendenti("ADR",
////                                anno, data,
////                                String.valueOf(nreg),
////                                us.getConto(),
////                                us.getCod(),
////                                StringUtils.replace(valori[2], ".", ","),
////                                us.getCod() + " " + nc1.getDe_causale_nc() + " " + filiale.getDe_branch(),
////                                filiale.getCod(), writer, nc0.getConto_coge_01());
////                        nreg++;
////                    }
////                }
////            }
////            writer.close();
////            if (nreg > 1) {
////                return f;
////            }
////            f.delete();
////        } catch (IOException ex) {
////            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
////        }
////        return null;
////    }
//}
