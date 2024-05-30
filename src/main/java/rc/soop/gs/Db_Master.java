package rc.soop.gs;

import com.fasterxml.jackson.databind.ObjectMapper;
import static rc.soop.gs.Config.categoria;
import static rc.soop.gs.Config.codiceTenant;
import static rc.soop.gs.Config.fd;
import static rc.soop.gs.Config.log;
import static rc.soop.gs.Config.patternD1;
import static rc.soop.gs.Config.roundDoubleandFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import static rc.soop.start.Utility.rb;

public class Db_Master {

    public Connection c = null;

    public Db_Master() {
        try {
            String drivername = rb.getString("db.driver");
            String typedb = rb.getString("db.tipo");
            String user = rb.getString("db.User");
            String pwd = rb.getString("db.pwd");
            Class.forName(drivername).newInstance();
            Properties p = new Properties();
            p.put("user", user);
            p.put("password", pwd);
            p.put("useUnicode", "true");
            p.put("characterEncoding", "UTF-8");
            p.put("useSSL", "false");
            p.put("connectTimeout", "1000");
            p.put("useUnicode", "true");
            p.put("useJDBCCompliantTimezoneShift", "true");
            p.put("useLegacyDatetimeCode", "false");
            p.put("serverTimezone", "Europe/Rome");
            String host = rb.getString("db.ip") + "/maccorpita";
            this.c = DriverManager.getConnection("jdbc:" + typedb + ":" + host, p);
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
            this.c = null;
        }
    }

    public Connection getC() {
        return c;
    }

    public void setC(Connection c) {
        this.c = c;
    }

    public void closeDB() {
        try {
            if (this.c != null) {
                this.c.close();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
    }

    public ArrayList<DatiInvio> query_datiinvio_annuale(ArrayList<String[]> ing) {
        ArrayList<DatiInvio> out = new ArrayList<>();

        DateTime date = new DateTime().dayOfYear().withMinimumValue().withTimeAtStartOfDay();
        DateTime last = new DateTime().minusDays(1);

        for (int x = 0; x < ing.size(); x++) {
            String[] ingvalue = ing.get(x);
            String filiale = ingvalue[0];
            String codice = ingvalue[1];
            while (date.isBefore(last) || date.isEqual(last)) {
//                DateTime dt1 = dt.minusDays(31 - y);
                String datestart = date.toString(patternD1);
                Daily_value dv = list_Daily_value(filiale, datestart + " 00:00", datestart + " 23:59", true);
                double buy = fd(dv.getPurchGrossTot());
                double sel = fd(dv.getSalesGrossTot());
                double sco = fd(dv.getNoTransPurch()) + fd(dv.getNoTransSales());
                //query con filiale
                String lordo = roundDoubleandFormat(buy + sel, 2);
//                String netto = roundDoubleandFormat(buy + sel, 2);
                String scontrini = StringUtils.leftPad(roundDoubleandFormat(sco, 0), 3, "0");
                log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{filiale, datestart, lordo, scontrini});
                DatiInvio di = new DatiInvio(codiceTenant, codice, datestart, categoria, lordo, lordo, scontrini);
                out.add(di);
                date = date.plusDays(1);
            }

        }

        System.out.println("com.rest.Db_Master.query_datiinvio_annuale() " + date.toString(patternD1));
        System.out.println("com.rest.Db_Master.query_datiinvio_annuale() " + last.toString(patternD1));

        return out;
    }

    public List<DatiInvio> query_datiinvio(List<Filiale> ing, DateTime dt1, DateTime dtEND) {

        dtEND = dtEND.withMillisOfDay(0);

        List<DatiInvio> out = new ArrayList<>();
        for (int x = 0; x < ing.size(); x++) {
            Filiale ingvalue = ing.get(x);
            DateTime dtSTART = dt1.withMillisOfDay(0);
            while (dtSTART.isBefore(dtEND)) {
                String datestart = dtSTART.toString(patternD1);
                Daily_value dv = list_Daily_value(ingvalue.getCod(), datestart + " 00:00", datestart + " 23:59", true);
                double buy = fd(dv.getPurchGrossTot());
                double sel = fd(dv.getSalesGrossTot());
                double sco = fd(dv.getNoTransPurch()) + fd(dv.getNoTransSales());
                //query con filiale
                String lordo = roundDoubleandFormat(buy + sel, 2);
//                String netto = roundDoubleandFormat(buy + sel, 2);
                String scontrini = StringUtils.leftPad(roundDoubleandFormat(sco, 0), 3, "0");
                log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{ingvalue.getCod(), datestart, lordo, scontrini});
                DatiInvio di = new DatiInvio(codiceTenant, ingvalue.getContratto(), datestart, categoria, lordo, lordo, scontrini);
                out.add(di);
                dtSTART = dtSTART.plusDays(1);
            }
        }
        return out;
    }

    public List<DatiInvio> query_datiinvio(List<Filiale> ing, DateTime dt) {
        List<DatiInvio> out = new ArrayList<>();
        for (int x = 0; x < ing.size(); x++) {
            Filiale ingvalue = ing.get(x);
            for (int y = 0; y < 31; y++) {
                DateTime dt1 = dt.minusDays(31 - y);
                String datestart = dt1.toString(patternD1);
                Daily_value dv = list_Daily_value(ingvalue.getCod(), datestart + " 00:00", datestart + " 23:59", true);
                double buy = fd(dv.getPurchGrossTot());
                double sel = fd(dv.getSalesGrossTot());
                double sco = fd(dv.getNoTransPurch()) + fd(dv.getNoTransSales());
                //query con filiale
                String lordo = roundDoubleandFormat(buy + sel, 2);
//                String netto = roundDoubleandFormat(buy + sel, 2);
                String scontrini = StringUtils.leftPad(roundDoubleandFormat(sco, 0), 3, "0");

                DatiInvio di = new DatiInvio(codiceTenant, ingvalue.getContratto(), datestart, categoria, lordo, lordo, scontrini);
                if (ingvalue.getCod().equals("051")) {
                    if (dt1.isAfter(new DateTime(2022, 10, 1, 0, 0)) || dt1.isEqual(new DateTime(2022, 10, 1, 0, 0))) {
                        out.add(di);
                        log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{ingvalue.getCod(), datestart, lordo, scontrini});
                    }
                } else if (ingvalue.getCod().equals("040")) {
                    if (dt1.isAfter(new DateTime(2022, 11, 21, 0, 0)) || dt1.isEqual(new DateTime(2022, 11, 21, 0, 0))) {
                        out.add(di);
                        log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{ingvalue.getCod(), datestart, lordo, scontrini});
                    }
                } else if (ingvalue.getCod().equals("041")) {
                    if (dt1.isBefore(new DateTime(2022, 11, 21, 0, 0))) {
                        out.add(di);
                        log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{ingvalue.getCod(), datestart, lordo, scontrini});
                    }
                } else {
                    log.log(Level.INFO, "FILIALE {0} -  DATA {1} : LORDO {2} SC {3}", new Object[]{ingvalue.getCod(), datestart, lordo, scontrini});
                    out.add(di);
                }

            }
        }
        return out;
    }

   public Daily_value list_Daily_value(String fil, String datad1, String datad2, boolean now) {
    if (datad1 != null && datad2 != null) {
        try {
            Daily_value d = new Daily_value();

            double setPurchTotal = 0.0;
            double setPurchComm = 0.0;
            double setSalesTotal = 0.0;
            double setSalesGrossTot = 0.0;
            int setNoTransPurch;
            int setNoTransSales;

            String sql = "SELECT * FROM ch_transaction tr1 WHERE tr1.del_fg='0' AND tr1.filiale = ? ";
            sql += "AND tr1.data >= ? ";
            sql += "AND tr1.data <= ? ";
            sql += " ORDER BY tr1.data";

            try (PreparedStatement ps = this.c.prepareStatement(sql)) {
                ps.setString(1, fil);
                ps.setString(2, datad1 + ":00");
                ps.setString(3, datad2 + ":59");

                try (ResultSet rs = ps.executeQuery()) {
                    setNoTransPurch = 0;
                    setNoTransSales = 0;
                    while (rs.next()) {
                        if (rs.getString("tipotr").equals("B")) {
                            boolean ca = false;
                            try (PreparedStatement psval = this.c.prepareStatement("SELECT * FROM ch_transaction_valori WHERE cod_tr = ?")) {
                                psval.setString(1, rs.getString("cod"));
                                try (ResultSet rsval = psval.executeQuery()) {
                                    while (rsval.next()) {
                                        switch (rsval.getString("supporto")) {
                                            case "04":
                                            case "06":
                                            case "07":
                                            case "08":
                                                //CASH ADVANCE, CREDIT CARD, BANCOMAT, etc.
                                                ca = true;
                                                break;
                                            default:
                                                setPurchTotal += fd(rsval.getString("net"));
                                                setPurchComm += fd(rsval.getString("tot_com")) + fd(rsval.getString("roundvalue"));
                                                break;
                                        }
                                    }
                                }
                            }
                            if (!ca) {
                                setNoTransPurch++;
                            }
                        } else {
                            setNoTransSales++;
                            setSalesTotal += fd(rs.getString("pay"));
                            setSalesGrossTot += fd(rs.getString("total"));
                        }
                    }
                }
            }

            d.setPurchGrossTot(roundDoubleandFormat(setPurchTotal + setPurchComm, 2));
            d.setSalesTotal(roundDoubleandFormat(setSalesTotal, 2));
            d.setSalesGrossTot(roundDoubleandFormat(setSalesGrossTot, 2));
            d.setNoTransPurch(String.valueOf(setNoTransPurch));
            d.setNoTransSales(String.valueOf(setNoTransSales));

            return d;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
    }
    return null;
}


    public List<Filiale> getConfList() {
        List<Filiale> out = new ArrayList<>();
        try {
            String sql = "SELECT des FROM conf WHERE id='gs.json.filiali'";
            try ( Statement st = this.c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);  ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    out = Arrays.asList(new ObjectMapper().readValue(rs.getString(1), Filiale[].class));
                }
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0} ERROR: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        out.sort(Comparator.comparing(Filiale::getCod));
        return out;

    }

}
