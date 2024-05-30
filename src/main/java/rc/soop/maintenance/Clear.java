/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import rc.soop.aggiornamenti.Db;
import static rc.soop.aggiornamenti.Mactest.host_PROD;

/**
 *
 * @author rcosco
 */
public class Clear {

    public static void CENTRAL_deleteaggiornamentimenouno(String datalike) {
    System.out.println("DELETE AGGIORNAMENTI MENO UN GIORNO, START...");
    Db db = new Db(host_PROD, false);
    try {
        if (db.getC() != null) {
            boolean riprova = true;
            int x = 0;
            String sql = "SELECT cod FROM aggiornamenti_mod WHERE cod like ? AND fg_stato='1' LIMIT 1";
            try (PreparedStatement psSelect = db.getC().prepareStatement(sql)) {
                psSelect.setString(1, datalike + "%");
                while (riprova) {
                    try (ResultSet rs = psSelect.executeQuery()) {
                        if (rs.next()) {
                            String cod = rs.getString("cod");
                            String deleteSQL = "DELETE FROM aggiornamenti_mod WHERE cod=?";
                            try (PreparedStatement psDelete = db.getC().prepareStatement(deleteSQL)) {
                                psDelete.setString(1, cod);
                                psDelete.executeUpdate();
                                x++;
                                System.out.println(x + ") " + cod + " Eliminato");
                            }
                        } else {
                            riprova = false;
                        }
                    }
                }
            }
        }
    } catch (SQLException ex) {
        ex.printStackTrace();
    } finally {
        db.closeDB();
    }
    System.out.println("DELETE AGGIORNAMENTI MENO UN GIORNO, END");
}


    private static void CENTRAL_delete_chtransactiondoc_story1() {
        int index = 0;
        System.out.println("DELETE AGGIORNAMENTI DOC STORY, START...");
        ArrayList<String> doc = new ArrayList<>();
        ArrayList<String> story = new ArrayList<>();
        Db db = new Db(host_PROD, false);
        try {
            if (db.getC() != null) {
                ResultSet rs = db.getC().createStatement().executeQuery("select distinct(codice_documento) FROM ch_transaction_doc");
                while (rs.next()) {
                    doc.add(rs.getString(1));
                }
                ResultSet rs1 = db.getC().createStatement().executeQuery("select distinct(codice_documento) FROM ch_transaction_doc_story");
                while (rs1.next()) {
                    story.add(rs1.getString(1));
                }
            }
            for (int x = 0; x < story.size(); x++) {
                if (doc.contains(story.get(x))) {
                    db.getC().createStatement().execute("DELETE FROM ch_transaction_doc_story WHERE codice_documento = '" + story.get(x) + "'");
                    System.out.println(index + ") DELETE FROM ch_transaction_doc_story WHERE codice_documento = '" + story.get(x) + "'");
                    index++;
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        db.closeDB();
    }

    public static void CENTRAL_delete_chtransactiondoc_story() {
        System.out.println("DELETE AGGIORNAMENTI DOC STORY, START...");
        Db db = new Db(host_PROD, false);
        try {
            if (db.getC() != null) {
                int x = 0;
                String selectSQL = "SELECT DISTINCT codice_documento FROM ch_transaction_doc";
                try (PreparedStatement psSelect = db.getC().prepareStatement(selectSQL);
                     ResultSet rs = psSelect.executeQuery()) {
                    while (rs.next()) {
                        String codice_documento = rs.getString("codice_documento");
                        String deleteSQL = "DELETE FROM ch_transaction_doc_story WHERE codice_documento = ?";
                        try (PreparedStatement psDelete = db.getC().prepareStatement(deleteSQL)) {
                            psDelete.setString(1, codice_documento);
                            int y = psDelete.executeUpdate();
                            if (y > 0) {
                                System.out.println(x + ") " + codice_documento + " Eliminato.");
                                x++;
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            db.closeDB();
        }
        System.out.println("DELETE AGGIORNAMENTI DOC STORY, END.");
    }
    

    private static void size_filiale() {

        Db dbc = new Db(host_PROD, false);
        ArrayList<String[]> ip = dbc.getIpFiliale();
        dbc.closeDB();

        for (int i = 0; i < ip.size(); i++) {
            String[] f1 = ip.get(i);
            String fil = f1[0];
            if (!fil.equals("000")) {
                Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
                if (dbfil.getC() != null) {
                    try {
                        ResultSet rs0 = dbfil.getC().createStatement().executeQuery(
                                "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) "
                                + "\"DB Size in MB\" FROM information_schema.tables WHERE table_schema = 'maccorp'");
                        while (rs0.next()) {
                            System.out.println(fil + " - " + rs0.getString(1));
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    dbfil.closeDB();
                } else {
                    System.out.println(fil + " NON CONNESSO");
                }

            }
        }
        ;

        //
    }

    private static void Maintenance_filiale_single(String filialeing) {

        Db dbc = new Db(host_PROD, false);
        ArrayList<String[]> ip = dbc.getIpFiliale(filialeing);
        dbc.closeDB();

        for (int i = 0; i < ip.size(); i++) {

            String[] f1 = ip.get(i);
            String fil = f1[0];
            if (!fil.equals("000")) {
                Db dbfil = new Db("//" + f1[1] + ":3306/maccorp", true);
                if (dbfil.getC() != null) {
                    System.out.println(fil + " CONNESSO");
                    try {
                        int x = 0;
                        ResultSet rs0 = dbfil.getC().createStatement().executeQuery("select distinct(codice_documento) FROM ch_transaction_doc where codice_documento in (select distinct(codice_documento) FROM ch_transaction_doc_story)");
                        while (rs0.next()) {
                            dbfil.getC().createStatement().execute("DELETE FROM ch_transaction_doc_story WHERE codice_documento = '" + rs0.getString(1) + "'");
                            x++;
                            System.out.println(x + " . ch_transaction_doc_story eliminato");
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    dbfil.closeDB();
                } else {
                    System.out.println(fil + " NON CONNESSO");
                }
                Db dbfil1 = new Db("//" + f1[1] + ":3306/maccorp", true);
                if (dbfil1.getC() != null) {
                    try {
                        boolean riprova = true;
                        int x = 0;
                        while (riprova) {
                            String SQL = "SELECT cod FROM aggiornamenti_mod WHERE fg_stato='1' limit 1000";
                            ResultSet rs = dbfil1.getC().createStatement().executeQuery(SQL);
                            if (!rs.next()) {
                                riprova = false;
                            } else {
                                rs.beforeFirst();
                                while (rs.next()) {
                                    dbfil1.getC().createStatement().execute("DELETE FROM aggiornamenti_mod WHERE cod='" + rs.getString(1) + "'");
                                    x++;
                                    System.out.println(x + " . aggiornamenti_mod eliminato");
                                }
                            }
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    dbfil1.closeDB();
                    break;
                } else {
                    System.out.println(fil + " NON CONNESSO");
                }
            }
        }
    }

//    public static void main(String[] args) {
//
//////////        String date;
//////////
//////////        try {
//////////            date = args[0];
//////////        } catch (Exception e) {
//////////            date = "18";
//////////        }
//////////
//////////        System.out.println("DELETE AGGIORNAMENTI MENO UN GIORNO, START...");
//////////        Db db = new Db(host_TEST, false);
//////////        try {
//////////            if (db.getC() != null) {
//////////                boolean riprova = true;
//////////                int x = 0;
//////////                while (riprova) {
//////////                    String SQL = "SELECT cod FROM aggiornamenti_mod WHERE cod like '" + date + "%' LIMIT 1";
//////////                    ResultSet rs = db.getC().createStatement().executeQuery(SQL);
//////////                    if (rs.next()) {
//////////                        db.getC().createStatement().execute("DELETE FROM aggiornamenti_mod WHERE cod='" + rs.getString(1) + "'");
//////////                        x++;
//////////                        System.out.println(x + ") " + rs.getString(1) + " Eliminato");
//////////                    } else {
//////////                        riprova = false;
//////////                    }
//////////                }
//////////            }
//////////        } catch (SQLException ex) {
//////////            ex.printStackTrace();
//////////        }
//////////        db.closeDB();
//
//        String val;
//        try {
//            val = args[0];
//            if (val == null) {
//                val = "ALL";
//            }
//        } catch (Exception e) {
//            val = "ALL";
//        }
////        CENTRAL_deleteaggiornamentimenouno("190314");
////        size_filiale();
//        if (val.equals("1") || val.equals("ALL")) {
//            //1) AGGIORNAMENTI CENTRALE -1 DAY
//            DateTime dt = new DateTime().minusDays(1);
//            String datetime = dt.toString("YYMMdd");
//            CENTRAL_deleteaggiornamentimenouno(datetime);
//        }
//        if (val.equals("2") || val.equals("ALL")) {
//            //2) DOC STORY CENTRALE
//            CENTRAL_delete_chtransactiondoc_story();
//        }
////        if (val.equals("3") || val.equals("ALL")) {
////            //3) DELETE FILIALI
////            String FILIALE = null;
////            try {
////                FILIALE = args[1];
////                if (FILIALE == null) {
////                    FILIALE = "ALL";
////                }
////            } catch (Exception e) {
////                FILIALE = "ALL";
////            }            
////            Maintenance_filiale_single(FILIALE);
////        }
//    }

}
