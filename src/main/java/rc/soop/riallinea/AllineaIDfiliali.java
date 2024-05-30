/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.riallinea;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author rcosco
 */
public class AllineaIDfiliali {

    private static void ripristinaIDtransazioniChange(String filiale, String ip, boolean cz) {
        try {

            ArrayList<String> li_temp0 = new ArrayList<>();
            ArrayList<String> li_update = new ArrayList<>();
            String sql = "select cod,id,data FROM ch_transaction_temp WHERE filiale='" + filiale + "'";
            String sql1 = "select cod,id,data FROM ch_transaction WHERE filiale='" + filiale + "'";

            Db_Master dbfil = new Db_Master(true, ip);
            if (dbfil.getC() != null) {
                ResultSet rs2 = dbfil.getC().createStatement().executeQuery(sql);
                while (rs2.next()) {
                    String cod = rs2.getString("cod");
                    String id = rs2.getString("id");
                    // String[] v = {cod, id};
                    li_update.add("UPDATE ch_transaction_temp SET id = '" + id + "' WHERE cod='" + cod + "'");
                    li_temp0.add(cod);
                }
                ResultSet rs3 = dbfil.getC().createStatement().executeQuery(sql1);
                while (rs3.next()) {
                    String cod = rs3.getString("cod");
                    String id = rs3.getString("id");
                    li_update.add("UPDATE ch_transaction SET id = '" + id + "' WHERE cod='" + cod + "'");
                }
                dbfil.closeDB();
            }
            Db_Master db = new Db_Master(cz, false, false);
            ResultSet rs1 = db.getC().createStatement().executeQuery(sql);
            while (rs1.next()) {
                String cod = rs1.getString("cod");
                String id = rs1.getString("id");
                // String[] v = {cod, id};
                if (!li_temp0.contains(cod)) {
                    String del1 = "DELETE FROM ch_transaction_temp WHERE cod ='" + cod + "'";
                    System.out.println(del1 + " : " + (db.getC().createStatement().executeUpdate(del1) > 0));
                }
            }
            for (int t = 0; t < li_update.size(); t++) {
                System.out.println(
                        li_update.get(t) + " : " + (db.getC().createStatement().executeUpdate(li_update.get(t)) > 0));
            }
            db.closeDB();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void ripristinaIDtransazioniNOChange(String filiale, String ip, boolean cz) {
        try {
            ArrayList<String[]> li_temp1 = new ArrayList<>();
            ArrayList<String[]> li_temp2 = new ArrayList<>();

            String sql = "SELECT cod, id FROM nc_transaction WHERE filiale = ? ORDER BY data";
            Db_Master db = new Db_Master(cz, false, false);
            PreparedStatement ps1 = db.getC().prepareStatement(sql);
            ps1.setString(1, filiale);
            ResultSet rs1 = ps1.executeQuery();

            while (rs1.next()) {
                String cod = rs1.getString("cod").trim();
                String id = rs1.getString("id").trim();
                String[] v = { cod, id };
                li_temp1.add(v);
            }
            rs1.close();
            ps1.close();

            Db_Master dbfil = new Db_Master(true, ip);
            if (dbfil.getC() != null) {
                PreparedStatement ps2 = dbfil.getC().prepareStatement(sql);
                ps2.setString(1, filiale);
                ResultSet rs2 = ps2.executeQuery();

                while (rs2.next()) {
                    String cod = rs2.getString("cod").trim();
                    String id = rs2.getString("id").trim();
                    String[] v = { cod, id };
                    li_temp2.add(v);
                }
                rs2.close();
                ps2.close();
                dbfil.closeDB();
            }

            String updateSql = "UPDATE nc_transaction SET id = ? WHERE cod = ?";
            PreparedStatement updatePs = db.getC().prepareStatement(updateSql);

            for (String[] ce : li_temp1) {
                for (String[] fi : li_temp2) {
                    if (ce[0].equals(fi[0])) {
                        if (!ce[1].equals(fi[1])) {
                            updatePs.setString(1, fi[1]);
                            updatePs.setString(2, ce[0]);
                            System.out.println(updatePs + " : " + (updatePs.executeUpdate() > 0));
                        }
                    }
                }
            }
            updatePs.close();
            db.closeDB();
            System.out.println(li_temp1.size());
            System.out.println(li_temp2.size());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private static void ripristinaIDETChange(String filiale, String ip, boolean cz) {
        try {
            // ArrayList<String> li_temp0 = new ArrayList<>();
            ArrayList<String[]> li_temp1 = new ArrayList<>();
            ArrayList<String[]> li_temp2 = new ArrayList<>();
            // ArrayList<String> li_update = new ArrayList<>();

            String sql = "select cod,id FROM et_change WHERE filiale='" + filiale + "' ORDER BY dt_it";
            Db_Master db = new Db_Master(cz, false, false);
            ResultSet rs1 = db.getC().createStatement().executeQuery(sql);
            while (rs1.next()) {
                String cod = rs1.getString("cod").trim();
                String id = rs1.getString("id").trim();
                String[] v = { cod, id };
                // li_temp0.add(cod);
                li_temp1.add(v);
            }

            Db_Master dbfil = new Db_Master(true, ip);
            if (dbfil.getC() != null) {
                ResultSet rs2 = dbfil.getC().createStatement().executeQuery(sql);
                while (rs2.next()) {
                    String cod = rs2.getString("cod").trim();
                    String id = rs2.getString("id").trim();
                    String[] v = { cod, id };
                    li_temp2.add(v);

                    // li_update.add("UPDATE nc_transaction SET id = '" + id + "' WHERE cod='" + cod
                    // + "'");
                    // if (!li_temp0.contains(cod)) {
                    // String del1 = "DELETE FROM nc_transaction WHERE cod ='" + cod + "'";
                    // System.out.println(del1);
                    // }
                }
                dbfil.closeDB();
            }

            for (int x = 0; x < li_temp1.size(); x++) {
                String[] ce = li_temp1.get(x);
                for (int y = 0; y < li_temp2.size(); y++) {
                    String[] fi = li_temp2.get(y);
                    if (ce[0].equals(fi[0])) {
                        if (!ce[1].equals(fi[1])) {
                            String upd = "UPDATE et_change SET id = '" + fi[1] + "' WHERE cod='" + ce[0] + "'";

                            // System.out.println(upd);
                            System.out.println(upd + " : " + (db.getC().createStatement().executeUpdate(upd) > 0));

                        }
                    }
                }
            }
            db.closeDB();
            System.out.println(li_temp1.size());
            System.out.println(li_temp2.size());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // ripristinaIDtransazioniChange("161", "192.168.112.2", false);
        ripristinaIDtransazioniNOChange("051", "192.168.243.5", false);
        //// ripristinaIDETChange("115", "192.168.35.2", false);
    }

}
