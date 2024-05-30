package rc.soop.testarea;

import static rc.soop.esolver.Util.patternsql;
import rc.soop.rilasciofile.Ch_transaction;
import rc.soop.rilasciofile.DatabaseCons;
import rc.soop.rilasciofile.Excel;
import rc.soop.rilasciofile.GeneraFile;
import java.io.File;
import java.util.ArrayList;
import java.util.Locale;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import rc.soop.esolver.Branch;
import rc.soop.esolver.NC_category;
import rc.soop.rilasciofile.ControlloGestione;
import static rc.soop.rilasciofile.Utility.patternmonthsql;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/**
 *
 * @author rcosco
 */
public class ReportOndemand {

    public static void main(String[] args) {
        GeneraFile gf = new GeneraFile();
        gf.setIs_IT(true);
        gf.setIs_UK(false);
        gf.setIs_CZ(false);
        DatabaseCons db = new DatabaseCons(gf);
        String path = db.getPath("temp");
        ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
        ArrayList<Branch> allenabledbr = db.list_branch();

        DateTime iniziomese = new DateTime(2024, 3, 1, 0, 0);

        String data1 = iniziomese.toString(patternsql);
        String data2 = new DateTime(2024, 3, 31, 23, 59).toString(patternsql);

        String datecreation = new DateTime().withZone((DateTimeZone.forID("Europe/Rome"))).toString("yyyyMMddHHmmss");
//        DateTime iniziomese = new DateTime().minusDays(1).dayOfMonth().withMinimumValue();
//        System.out.println("com.fl.upload.GeneraFile.rilasciafile() "+);
//        String mesemysql = iniziomese.toString(patternmonthsql);

//        String meseriferimento = iniziomese.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
//        String annoriferimento = iniziomese.year().getAsText(Locale.ITALY).toUpperCase();

        String nomereport = "MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL N1 DA " + data1 + " A " + data2 + "_" + datecreation + ".xlsx";
        File Output = new File(path + nomereport);
        String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, true, allenabledbr, db);
        System.out.println("rc.soop.testarea.ReportOndemand.main() "+Output.getPath());
        
        
//        ArrayList<Ch_transaction> result = db.query_transaction_ch_new(data1, data2, br1);
//        String nomereport = "LIST TRANSACTION CHANGE DA " + data1 + " A " + data2 + ".xlsx";
//        File Output = new File(path + nomereport);
//        String base64 = Excel.excel_transaction_listEVO(gf, Output, result);
//        ArrayList<NC_transaction> result = db.query_NC_transaction_NEW(data1, data2, br1, "NO");
//        String nomereport = "LIST TRANSACTION NOCHANGE DA " + data1 + " A " + data2 + ".xlsx";
//        File Output = new File(path + nomereport);
//        String base64 = Excel.excel_transactionnc_list(gf, Output, result);
        db.closeDB();

        System.out.println(path + nomereport);
    }
}
