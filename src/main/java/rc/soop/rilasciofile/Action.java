/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import rc.soop.sftp.Db;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author srotella
 */
public class Action {

    public static ArrayList<Fileinfo> getFile(String table) {
        Db db = new Db(false);
        ArrayList<Fileinfo> out = db.getFile(table);
        db.closeDB();
        return out;
    }

    public static boolean insertFile(Fileinfo fileinfo, String table) {
        Db db = new Db(false);
        boolean out = db.insertFile(fileinfo, table);
        db.closeDB();
        return out;
    }

    public static String getPath(String path) {
        Db db = new Db(false);
        String out = db.getPath(path, "url");
        db.closeDB();
        return out;
    }

    public static String estraiNumber(String full) {
        StringBuilder sbuil = new StringBuilder("");
        List<String> sp1 = Splitter.on("/").splitToList(full);
        sp1.forEach(content -> {
            if (StringUtils.isNumeric(content)) {
                sbuil.append(content);
            }
        });
        return sbuil.toString();
    }

    public static String formatTypeTransaction_stockprice(String type) {
        if (type.equals("CH") || type.equals("NC")) {
            return "Transaction";
        } else if (type.equals("ET")) {
            return "Ext. Transfert";
        } else if (type.equals("IT")) {
            return "Int. Transfert";
        } else if (type.equals("OC")) {
            return "Open/Close Error";
        }
        return type;
    }

}
