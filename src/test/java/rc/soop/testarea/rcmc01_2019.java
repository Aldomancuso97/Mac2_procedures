package rc.soop.testarea;


import rc.soop.esolver.Branch;
import static rc.soop.esolver.Util.patternsql;
import rc.soop.rilasciofile.ControlloGestione;
import rc.soop.rilasciofile.DatabaseCons;
import rc.soop.rilasciofile.GeneraFile;
import java.io.File;
import java.util.ArrayList;
import org.joda.time.DateTime;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/**
 *
 * @author Raffaele
 */
public class rcmc01_2019 {

    public static void main(String[] args) {
        GeneraFile gf = new GeneraFile();
        gf.setIs_IT(true);
        gf.setIs_UK(false);
        gf.setIs_CZ(false);
        DatabaseCons db = new DatabaseCons(gf);
        String path = db.getPath("temp");
        ArrayList<String> br1 = db.list_branchcode_completeAFTER311217();
        ArrayList<Branch> allenabledbr = db.list_branch();
        

        String data1 = new DateTime(2023, 10, 1, 0, 0).toString(patternsql);
        String data2 = new DateTime(2023, 10, 1, 23, 59).toString(patternsql);

        String nomereport = "MANAGEMENT CONTROL - REPORT MANAGEMENT CONTROL N1 DA " + data1 + " A " + data2 + ".xlsx";
        File Output = new File(path + nomereport);
        String base64 = ControlloGestione.management_change_n1(Output, br1, data1, data2, true, allenabledbr, db);
        db.closeDB();
        System.out.println(Output.getPath());

    }
    
}
