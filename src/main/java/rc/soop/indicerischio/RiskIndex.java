/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.indicerischio;

import com.google.common.util.concurrent.AtomicDouble;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import rc.soop.start.Utility;

/**
 *
 * @author rcosco
 */
public class RiskIndex {

    public static final Logger log = createLog("Mac2.0_INDICERISCHIO", Utility.rb.getString("path.log"), "yyyyMMdd");

    public static Logger createLog(String appname, String folderini, String patterndatefolder) {
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + "-" + dt.toString("HHmmssSSS") + ".log";
            File dirING = new File(folderini);
            dirING.mkdirs();
            if (patterndatefolder != null) {
                File dirINGNew = new File(dirING.getPath() + File.separator + dt.toString(patterndatefolder));
                dirINGNew.mkdirs();
                filename = dirINGNew.getPath() + File.separator + filename;
            } else {
                filename = dirING.getPath() + File.separator + filename;
            }
            Handler fileHandler = new FileHandler(filename);
            LOGGER.addHandler(fileHandler);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException localIOException) {
        }

        return LOGGER;
    }

    public static void engine() {
        log.warning("START...");
        Db_Master dbm = new Db_Master();
        Office of = dbm.get_national_office();
        ArrayList<Branch> allenabledbr = dbm.list_branch_enabled();
        ArrayList<String> br1 = dbm.list_cod_branch_enabled();
        dbm.closeDB();

        DateTime oggi = new DateTime();
        String dt = oggi.toString("yyyy-MM-dd HH:mm:ss");
//        try {

        Db_Master dbm1 = new Db_Master();
        boolean DELETED = dbm1.deleteAll_Indicerischio();
        dbm1.closeDB();

        if (DELETED) {
            log.info("RISK ASSESSMENT INDEX CANCELLATI");

            Db_Master dbm2 = new Db_Master();
            List<Value> rs = dbm2.list_CustomerTransactionList_value(br1, oggi.minusDays(Integer.parseInt(of.getRisk_days())).toString(patternsql), oggi.toString(patternsql),
                    allenabledbr, of.getRisk_ntr(), of.getRisk_soglia());
            dbm2.closeDB();
            log.log(Level.INFO, "TOTALE LISTA TRANSAZIONI: {0}", rs.size());
            List<String> cl = rs.stream().map(valore -> valore.getCl_cod()).distinct().collect(Collectors.toList());
            log.log(Level.INFO, "TOTALE LISTA CLIENTI: {0}", cl.size());
            List<IndiceRischio> out1 = new ArrayList<>();
            List<IndiceRischio> out2 = new ArrayList<>();

            Db_Master dbm3 = new Db_Master();

//            for (int i = 0; i < cl.size(); i++) {
            cl.forEach(client -> {
//                String client = cl.get(i);
                Client cli = dbm3.query_Client(client);
                if (cli != null) {
                    String msg = cli.getCognome() + " " + cli.getNome();

                    IndiceRischio c1 = new IndiceRischio();
                    c1.setId(generaId(50));
                    c1.setDt(dt);
                    c1.setMessage(msg.toUpperCase());
                    AtomicDouble tot = new AtomicDouble(0.00);

                    rs.forEach(valore -> {
//                while (rs.next()) {
                        if (valore.getCl_cod().equals(client)) {
//                        cl_numtr.add(valore.getCod());
                            if (valore.getTipotr().equals("S")) {
                                tot.addAndGet(fd(valore.getPay()));
                            } else {
                                tot.addAndGet(fd(valore.getTotal()));
                            }
                        }
                    });

                    List<String> cl_numtr = rs.stream().filter(valore -> valore.getCl_cod().equals(client)).map(valore -> valore.getCod()).distinct().collect(Collectors.toList());

                    //removeDuplicatesAL(cl_numtr);
                    if (cl_numtr.size() >= Integer.parseInt(of.getRisk_ntr())) {
                        c1.setStato("0");
                        out1.add(c1);
                    } else if (tot.get() >= fd((of.getRisk_soglia()))) {
                        c1.setStato("2");
                        out2.add(c1);
                    }
                }
//            rs.beforeFirst();
            });
            dbm3.closeDB();

            log.log(Level.INFO, "RISK ASSESSMENT INDEX - NUMBER OF TRANSACTIONS: {0}", out1.size());

            Db_Master dbm4 = new Db_Master();
            out1.forEach(ir -> {
//            for (int x = 0; x < out1.size(); x++) {
//                IndiceRischio ir = out1.get(x);
                dbm4.insert_Indicerischio(ir);
            });
            dbm4.closeDB();

            log.log(Level.INFO, "RISK ASSESSMENT INDEX - NUMBER OF THRESHOLD: {0}", out2.size());
            Db_Master dbm5 = new Db_Master();
            out2.forEach(ir -> {
//            for (int x = 0; x < out2.size(); x++) {
//                IndiceRischio ir = out2.get(x);
                dbm5.insert_Indicerischio(ir);
            });

//        } catch (SQLException ex) {
//            ex.printStackTrace();
//        }
            dbm5.closeDB();

            log.warning("END.");
        }
    }

    public static boolean removeDuplicatesAL(ArrayList l) {
        int sizeInit = l.size();

        Iterator p = l.iterator();
        while (p.hasNext()) {
            Object op = p.next();
            Iterator q = l.iterator();
            Object oq = q.next();
            while (op != oq) {
                oq = q.next();
            }
            boolean b = q.hasNext();
            while (b) {
                oq = q.next();
                if (op.equals(oq)) {
                    p.remove();
                    b = false;
                } else {
                    b = q.hasNext();
                }
            }
        }

        Collections.sort(l);

        return sizeInit != l.size();
    }

    public static double fd(String si_t_old) {
        double d1;
        try {
            si_t_old = si_t_old.replace(",", "").trim();
            d1 = Double.parseDouble(si_t_old);
        } catch (Exception e) {
            d1 = 0.0D;
        }
        return d1;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternnormdate_filter = "dd/MM/yyyy";
}

class IndiceRischio implements Comparable<IndiceRischio> {

    String id, codcl, message, stato, dt;

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getCodcl() {
        return codcl;
    }

    public void setCodcl(String codcl) {
        this.codcl = codcl;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStato() {
        return stato;
    }

    public void setStato(String stato) {
        this.stato = stato;
    }

    @Override
    public int compareTo(IndiceRischio o) {
        return this.message.compareTo(o.getMessage());
    }

}
