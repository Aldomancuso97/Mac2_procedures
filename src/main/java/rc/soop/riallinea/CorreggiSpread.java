/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.riallinea;

import static rc.soop.riallinea.Util.createLog;
import static rc.soop.riallinea.Util.estraiEccezione;
import static rc.soop.riallinea.Util.fd;
import static rc.soop.riallinea.Util.roundDouble;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import rc.soop.maintenance.Monitor;
import java.util.Optional;
/**
 *
 * @author rcosco
 */
public class CorreggiSpread {

    private static final Logger log = createLog("Mac2.0_CorreggiSpreadCZ_", Monitor.rb.getString("path.log"),
            "yyyyMMdd");

    public static void enginecz() {
        try {
            Db_Master db1 = new Db_Master(true, false);
            List<IpFiliale> lf = db1.getIpFiliale();
            String selectTransactionQuery = "SELECT c.cod,c.data,c.filiale,c.tipotr,c.id FROM ch_transaction c WHERE c.data > ? AND CAST(c.spread_total AS DECIMAL(12,8)) = 0";
            try (PreparedStatement ps1 = db1.getC().prepareStatement(selectTransactionQuery)) {
                ps1.setDate(1, java.sql.Date.valueOf("2021-10-01"));
                try (ResultSet rs1 = ps1.executeQuery()) {
                    while (rs1.next()) {
                        String codtr = rs1.getString(1);
                        String data = new DateTime(rs1.getDate("c.data").getTime()).toString("yyyy-MM-dd");
                        String filiale = rs1.getString("c.filiale");
                        String tipotr = rs1.getString("c.tipotr");
                        String id = rs1.getString("c.id");
                        Optional<IpFiliale> optionalIpFiliale = lf.stream()
                                .filter(d1 -> d1.getFiliale().equals(filiale))
                                .findAny();

                        if (optionalIpFiliale.isPresent()) {
                            String ip = optionalIpFiliale.get().getIp();
                            // Usa l'ip come necessario
                        String selectTransactionValoriQuery = "SELECT c.valuta,c.quantita,c.rate,c.spread FROM ch_transaction_valori c WHERE c.cod_tr=?";
                        try (PreparedStatement ps2 = db1.getC().prepareStatement(selectTransactionValoriQuery)) {
                            ps2.setString(1, codtr);
                            try (ResultSet rs2 = ps2.executeQuery()) {
                                if (rs2.next()) {
                                    String valuta = rs2.getString(1);
                                    String quantita = rs2.getString(2);
                                    String rate = rs2.getString(3);
                                    String spread = rs2.getString(4);
                                    if (fd(spread) == 0.00) {
                                        double cv = roundDouble(fd(quantita) * fd(rate), 2);
                                        String selectRateQuery = "SELECT rif_bce FROM rate WHERE valuta=? AND data=?";
                                        try (PreparedStatement ps3 = db1.getC().prepareStatement(selectRateQuery)) {
                                            ps3.setString(1, valuta);
                                            ps3.setString(2, data);
                                            try (ResultSet rs3 = ps3.executeQuery()) {
                                                if (rs3.next()) {
                                                    String bce = rs3.getString(1);
                                                    double cv_bce = roundDouble(fd(quantita) * fd(bce), 2);
                                                    double spread_res = roundDouble(cv - cv_bce, 2);
                                                    if (tipotr.equals("B")) {
                                                        spread_res = roundDouble(cv_bce - cv, 2);
                                                    }
                                                    Db_Master db2 = new Db_Master(true, ip);
                                                    if (db2.getC() != null) {
                                                        String updateTransactionQuery = "UPDATE ch_transaction SET spread_total=? WHERE cod = ?";
                                                        try (PreparedStatement psUpdateTransaction = db2.getC()
                                                                .prepareStatement(updateTransactionQuery)) {
                                                            psUpdateTransaction.setDouble(1, spread_res);
                                                            psUpdateTransaction.setString(2, codtr);
                                                            psUpdateTransaction.executeUpdate();
                                                        }

                                                        String updateTransactionValoriQuery = "UPDATE ch_transaction_valori SET spread=? WHERE cod_tr = ?";
                                                        try (PreparedStatement psUpdateTransactionValori = db2.getC()
                                                                .prepareStatement(updateTransactionValoriQuery)) {
                                                            psUpdateTransactionValori.setDouble(1, spread_res);
                                                            psUpdateTransactionValori.setString(2, codtr);
                                                            psUpdateTransactionValori.executeUpdate();
                                                        }

                                                        log.log(Level.INFO,
                                                                "OK CENTRALE {0} - {1} - {2} - {3} - {4} - {5} - {6} - {7} - {8} - {9} - {10}",
                                                                new Object[] { filiale, data, id, ip, spread, valuta,
                                                                        quantita, rate, cv, cv_bce, spread_res });

                                                        // Log per la filiale
                                                        log.log(Level.INFO,
                                                                "OK FILIALE {0} - {1} - {2} - {3} - {4} - {5} - {6} - {7} - {8} - {9} - {10}",
                                                                new Object[] { filiale, data, id, ip, spread, valuta,
                                                                        quantita, rate, cv, cv_bce, spread_res });

                                                        db2.closeDB();
                                                    } else {
                                                        log.log(Level.SEVERE,
                                                                "KO FILIALE NON RAGGIUNGIBILE {0} {1} {2}",
                                                                new Object[] { filiale, data, id });
                                                    }
                                                }else{

                                                }

                                                }
                                            }
                                        }
                                    } else {
                                        log.log(Level.SEVERE, "VERIFICARE {0}", codtr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            db1.closeDB();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.severe(estraiEccezione(ex));
        }
    }

    //
    public static void main(String[] args) {
        enginecz();
    }

}
