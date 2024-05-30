/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.testarea;

import java.io.File;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import static rc.soop.aggiornamenti.Mactest.updateSQL;

/**
 *
 * @author rcosco
 */
public class Update {

    public static void main(String[] args) {

//        File new1 = new File("C:\\mnt\\mac\\temp\\KYC_v2023.pdf");
        
        try {

//            updateSQL("UPDATE conf SET des = '" + 
//                    Base64.encodeBase64String(FileUtils.readFileToByteArray(new1)) 
//                    + "' where id ='path.profcl';", false);

//            updateSQL("UPDATE conf SET des = '" + 
//                    Base64.encodeBase64String(FileUtils.readFileToByteArray(new1)) 
//                    + "' where id ='path.profcl';", false);
            
//            String profcl = Base64.encodeBase64String(FileUtils.readFileToByteArray(new1));
//            Db db1 = new Db(host_TEST, false);
//            String upd = "UPDATE conf SET des = ? WHERE id = ?";
//            PreparedStatement ps1 = db1.getC().prepareStatement(upd);
//            ps1.setString(1, profcl);
//            ps1.setString(2, "path.profcl");
//            String dtoper = new DateTime().toString(patternsqldate);
//            String dt_val = Utility.formatStringtoStringDate(dtoper, patternsqldate, patternnormdate);
//            db1.insertValue_agg(ps1, null, null, dt_val, "setaser", false);
//            db1.closeDB();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        
//        try {     
//            Db db = new Db(host_TEST, false);
//
//            db.getC().crea
//
//            db.closeDB();
////            File f = new File("C:\\mnt\\MACCORP_SALDI_2018.zip");
////            String data = "2018-12-31 15:05:09";
//            Db db = new Db(host_PROD, false);
//            db.getC().createStatement().executeUpdate("UPDATE cora SET content = '"
//                    + Base64.encodeBase64String(FileUtils.readFileToByteArray(f))
//                    +
//                    "' WHERE data = '" + data + "'");
//            db.closeDB();
//
//        } catch (SQLException | IOException ex) {
//            ex.printStackTrace();
//        }
        

//            Db db = new Db(host_TEST, false);
//            
//            db.getC().crea
//            
//            db.closeDB();
////            File f = new File("C:\\mnt\\MACCORP_SALDI_2018.zip");
////            String data = "2018-12-31 15:05:09";
//            Db db = new Db(host_PROD, false);
//            db.getC().createStatement().executeUpdate("UPDATE cora SET content = '"
//                    + Base64.encodeBase64String(FileUtils.readFileToByteArray(f))
//                    + 
//                    "' WHERE data = '" + data + "'");
//            db.closeDB();
//            
//        } catch (SQLException | IOException ex) {
//            ex.printStackTrace();
//        }
    }

}
