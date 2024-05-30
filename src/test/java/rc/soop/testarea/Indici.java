/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.testarea;

import rc.soop.aggiornamenti.Utility;
import rc.soop.aggiornamenti.Db;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class Indici {

    private static final String host_PROD = rb.getString("db.ip") + "/maccorpita";

    public static void main(String[] args) {
        try {
            Db db1 = new Db(host_PROD, false);
            ArrayList<String[]> ip = db1.getIpFiliale();

            DatabaseMetaData dbMetaData = db1.getC().getMetaData();
            ArrayList<String> tables = new ArrayList<>();
            ArrayList<String[]> index = new ArrayList<>();
            ResultSet rs = dbMetaData.getTables(null, null, "%", null);
            while (rs.next()) {
//                if (rs.getString(3).equals("ch_transaction_doc")) {
                tables.add(rs.getString(3));
//                }
            }
            for (int x = 0; x < tables.size(); x++) {
                String sql = "SHOW index FROM " + tables.get(x);
                ResultSet rs1 = db1.getC().createStatement().executeQuery(sql);
                while (rs1.next()) {
                    String name = rs1.getString(3);
                    if (!name.equals("PRIMARY")) {
                        String t1 = rs1.getString(5);
                        String[] v1 = {tables.get(x), name, t1};
                        index.add(v1);
                    }
                }
            }
            db1.closeDB();

            ArrayList<TipoIndici> elencoindicitotali = new ArrayList<>();
            for (int i = 0; i < tables.size(); i++) {
                ArrayList<String[]> index_T = new ArrayList<>();
                ArrayList<String> index_NAME = new ArrayList<>();
                for (int c = 0; c < index.size(); c++) {
                    String[] v1 = index.get(c);
                    if (v1[0].equals(tables.get(i))) {
                        String[] v2 = {v1[1], v1[2]};
                        index_NAME.add(v1[1]);
                        index_T.add(v2);
                    }
                }
                removeDuplicatesAL(index_NAME);
                String table = tables.get(i);
                for (int v = 0; v < index_NAME.size(); v++) {
                    String name = index_NAME.get(v);
                    ArrayList<String> listacampi = new ArrayList<>();
                    for (int c = 0; c < index_T.size(); c++) {
                        if (index_T.get(c)[0].equals(index_NAME.get(v))) {
                            listacampi.add(index_T.get(c)[1]);
                        }
                    }
                    TipoIndici t = new TipoIndici(table, name, listacampi);
                    elencoindicitotali.add(t);
                }
            }

//            System.out.println(elencoindicitotali.size());
//            String filialedafare = "079";   // 22/01 23:00
//            String filialedafare = "147";   // 23/01 21:30
//            String filialedafare = "104";   // 23/01 23:30
//            String filialedafare = "119";   // 24/01 11:00
//            String filialedafare = "019";   // 24/01 12:30
//            String filialedafare = "125";   // 24/01 14:35
//            String filialedafare = "173";   // 25/01 10:15
//            String[] f1 = Utility.formatAL(filialedafare, ip);
//            System.out.println(f1[0]);
//            System.out.println(f1[1]);
            for (int i = 0; i < ip.size(); i++) {

                String[] f1 = ip.get(i);
                if (!f1[0].equals("000")) {
                    Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
                    if (dbfil.getC() != null) {
                        for (int y = 0; y < elencoindicitotali.size(); y++) {
                            TipoIndici t = elencoindicitotali.get(y);
                            boolean found = false;
                            String sql = "SHOW index FROM " + t.getTable();
                            ResultSet rs1 = dbfil.getC().createStatement().executeQuery(sql);
                            while (rs1.next()) {
                                String name = rs1.getString(3);
                                if (name.equals(t.getName())) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                String ind = "ALTER TABLE `" + t.getTable() + "` ADD INDEX `" + t.getName() + "` " + Utility.formatIndex(t.getListacampi()) + " USING BTREE;";
                                dbfil.getC().createStatement().execute(ind);
                                System.out.println(ind);
                            }
                        }
                        dbfil.closeDB();
                    } else {
                        System.err.println(f1[0] + " NON RAGGIUNGIBILE");
                    }
                }
            }
//            System.out.println(elencoindicitotali.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean removeDuplicatesAL(ArrayList l) {
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
}

class TipoIndici {

    String table;
    String name;
    ArrayList<String> listacampi;

    public TipoIndici(String table, String name, ArrayList<String> listacampi) {
        this.table = table;
        this.name = name;
        this.listacampi = listacampi;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<String> getListacampi() {
        return listacampi;
    }

    public void setListacampi(ArrayList<String> listacampi) {
        this.listacampi = listacampi;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
