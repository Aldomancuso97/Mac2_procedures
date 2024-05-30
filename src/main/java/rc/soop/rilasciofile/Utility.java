/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

import com.google.common.base.Splitter;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import rc.soop.esolver.Branch;
import rc.soop.esolver.NC_category;
import rc.soop.esolver.Users;
import static rc.soop.esolver.Util.fd;
import static rc.soop.esolver.Util.roundDoubleandFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.commons.codec.binary.Base64;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import org.apache.commons.io.FileUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

/**
 *
 * @author raffaele
 */
public class Utility {

    public static final String separatorString = "_";
    public static final String PATTERN1 = "_";
    public static final String path_log = "/mnt/mac/log/";

    public static final String patternnormdate_filter = "dd/MM/yyyy";
    public static final String pattermonthnorm = "MM/yyyy";
    public static final String patternnormdate_f = "dd/MM/yyyy HH:mm";
    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternyear = "yyyy";

    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternmonthsql = "yyyy-MM";
    public static final String patternsql_f = "yyyy-MM-dd HH:mm";
    public static final String patterntsdate = "yyyyMMddHHmmssSSS";
    public static final String patterntsdate2 = "yyMMddHHmmssSSS";
    public static final String patterntsdatecora = "yyyyMMddHHmmss";
    public static final String patternhours_d = "HH:mm:ss";
    public static final String patternhours_d1 = "HH:mm";

    public static String ctrlString(String a) {
        if (a == null) {
            return "-";
        }
        return a;
    }

    public static String ctrlInt(String a) {
        if (a == null) {
            return "0";
        }
        return a;
    }

    public static String convMd5(String psw) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(psw.getBytes());
            byte byteData[] = md.digest();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < byteData.length; i++) {
                sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString().trim();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "-";
        }
    }

    public static String randomP() {
        final SecureRandom random = new SecureRandom();
        String r = new BigInteger(130, random).toString(32);
        r = r.substring(0, 6);
        r = r + "!1";
        return r;
    }

    public static String generaId() {
        String random = RandomStringUtils.randomAlphanumeric(5).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String generaTokenSito() {
        String random = RandomStringUtils.randomAlphanumeric(10).trim();
        return random;
    }

    public static boolean isNumeric(String val) {
        return StringUtils.isNumeric(val);
    }

    public static String formatStringtoStringDate(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter fmt = DateTimeFormat.forPattern(pattern1);
                DateTime dtout = fmt.parseDateTime(dat);
                return dtout.toString(pattern2, Locale.ITALY);
            }
        } catch (IllegalArgumentException e) {
        }
        return "";
    }

    public static String getStringBase64_IO(File file) {
        try {
            byte[] bytes = FileUtils.readFileToByteArray(file);
            if (bytes != null) {
                byte[] base64 = Base64.encodeBase64(bytes);
                return new String(base64);
            }
        } catch (IOException io) {

        }
        return null;
    }

    public static String getHASH(File ing) {
        try (FileInputStream fis = new FileInputStream(ing)) {
            String md5 = md5Hex(fis);
            return md5;
        } catch (Exception ex) {
        }
        return null;
    }

    public static boolean isDirectory(ChannelSftp sftp, String dir) {
        try {
            sftp.lstat(dir);
            return true;
        } catch (SftpException ex) {
            return false;
        }

    }

    public static boolean isFile(String filename, ArrayList<Fileinfo> lis) {
        for (Fileinfo fileinfo : lis) {
            if (filename.equals(fileinfo.getName())) {
                return true;
            }
        }
        return false;
    }

    public static String visualizzaStringaMySQL(String ing) {
        if (ing == null) {
            return "";
        }
        ing = StringUtils.replace(ing, "\\'", "'");
        ing = StringUtils.replace(ing, "\'", "'");
        ing = StringUtils.replace(ing, "\"", "'");
        return ing.trim();
    }

    public static String getStringCurrency(ArrayList<String> li1, ArrayList<Currency> li2) {
        String out = "";
        for (int l = 0; l < li1.size(); l++) {
            for (int i = 0; i < li2.size(); i++) {
                if (li1.get(l).equals(li2.get(i).getCode())) {
                    out = out + li2.get(i).getCode() + "-" + li2.get(i).getDescrizione() + "; ";
                }
            }
        }
        return out.trim();
    }

    public static String getStringFigures(ArrayList<String> li1, ArrayList<Figures> li2) {
        String out = "";
        for (int l = 0; l < li1.size(); l++) {
            for (int i = 0; i < li2.size(); i++) {
                if (li1.get(l).equals(li2.get(i).getSupporto())) {
                    out = out + li2.get(i).getDe_supporto() + "; ";
                }
            }
        }
        return out.trim();
    }

    public static String formatCountry(String code, ArrayList<String[]> lista) {
        for (int i = 0; i < lista.size(); i++) {
            if (lista.get(i)[0].trim().equals(code.trim())) {
                return lista.get(i)[1];
            }
        }
        return "-";
    }

    public static String formatALN(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return cod;
    }

    public static String formatSex(String s) {
        if (s.equalsIgnoreCase("M")) {
            return "Male";
        } else if (s.equalsIgnoreCase("F")) {
            return "Female";
        } else if (s.equalsIgnoreCase("O")) {
            return "Other";
        }
        return s;
    }

    public static double parseDoubleR(GeneraFile gf, String value) {
        value = StringUtils.replace(value, "-", "").trim();

        double d1;
        try {
            d1 = Double.parseDouble(value);
        } catch (Exception e) {
            value = formatDoubleforMysql(gf, value);
            return parseDoubleR(gf, value);
        }
        return d1;
    }

    public static String formatDoubleforMysql(GeneraFile gf, String value) {
        if (value == null) {
            return "0.00";
        }
        if (value.equals("-") || value.equals("")) {
            return "0.00";
        }
        String add = "";
        if (value.contains("-")) {
            add = "-";
            value = StringUtils.replace(value, "-", "").trim();
        }

        if (!value.equals("0.00")) {
            if (gf.getThousand().equals(".")) {
                if (value.contains(gf.getDecimal())) {
                    if (gf.getDecimal().equals(",")) {
                        value = StringUtils.replace(value, "\\.", "");
                        value = StringUtils.replace(value, ",", "");
                    }
                } else {
                    if (gf.isIs_CZ()) {
                        if (value.contains(".")) {
                            List<String> ex = Splitter.on(".").splitToList(value);
                            if (ex.size() == 2) {
                                String int1 = ex.get(0);
                                String dec1 = ex.get(1);
                                switch (dec1.length()) {
                                    case 2:
                                        return add + value;
                                    case 1:
                                        return int1 + "." + dec1 + "0";
                                    default:
                                        value = StringUtils.replace(value, "\\.", "");
                                        return add + value + ".00";
                                }
                            } else {
                                value = StringUtils.replace(value, "\\.", "");
                                return add + value + ".00";
                            }
                        }
                        return add + value;
                    } else {
                        value = StringUtils.replace(value, "\\.", "");
                        return add + value + ".00";
                    }
                }
            } else if (gf.getThousand().equals(",")) {
                if (value.contains(gf.getDecimal())) {
                    if (gf.getDecimal().equals(".")) {
                        value = StringUtils.replace(value, ",", "");
                    }
                } else {
                    value = StringUtils.replace(value, ",", "");
                    if (gf.isIs_IT()) {
                        return value + "00";
                    } else {
                        return value + ".00";
                    }
                }
            }
        }

        return add + value;

    }

    public static String formatALNC_category(String cod, ArrayList<NC_category> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i).getGruppo_nc())) {
                return array.get(i).getDe_gruppo_nc();
            }
        }
        return "-";
    }

    public static NC_category getNC_category(ArrayList<NC_category> li, String nc_code) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getGruppo_nc().equals(nc_code)) {
                return li.get(i);
            }
        }
        return null;
    }

    public static String formatALNC_causal(String cod, ArrayList<NC_causal> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i).getCausale_nc())) {
                return array.get(i).getDe_causale_nc();
            }
        }
        return "-";
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
    }

    public static String formatALNC_causal_ncde(String cod, ArrayList<NC_causal> array, ArrayList<String[]> array_nc_descr) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i).getCausale_nc())) {
                for (int j = 0; j < array_nc_descr.size(); j++) {
                    if (array.get(i).getNc_de().equals(array_nc_descr.get(j)[0])) {
                        return array_nc_descr.get(j)[1];
                    }
                }
            }
        }
        return "-";
    }

    public static Branch get_Branch(String cod, List<Branch> array_branch) {
        for (int j = 0; j < array_branch.size(); j++) {
            if (cod.equals((array_branch.get(j)).getCod())) {
                return (array_branch.get(j));
            }
        }

        return null;
    }

    public static double roundDouble(
            double d,
            int scale
    ) {
//        try {
//            System.out.println("rc.soop.rilasciofile.Utility.roundDouble(A) " + d);
        d = new BigDecimal(d).setScale(scale, RoundingMode.HALF_UP).doubleValue();
//            System.out.println("rc.soop.rilasciofile.Utility.roundDouble(B) " + d);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return d;
    }

    public static String formatBankBranchReport(String cod, String type, ArrayList<String[]> array_bank, ArrayList<Branch> array_branch) {
        if ((cod != null)
                && (type != null)) {
            if (type.equals("BA")) {
                for (int j = 0; j < array_bank.size(); j++) {
                    if (cod.equals(((String[]) array_bank.get(j))[0])) {
                        return ((String[]) array_bank.get(j))[1].toUpperCase();
                    }
                }
            }
            if (type.equals("BR")) {
                for (int j = 0; j < array_branch.size(); j++) {
                    if (cod.equals((array_branch.get(j)).getCod())) {
                        return (array_branch.get(j)).getDe_branch();
                    }
                }
            }
        }

        return "";
    }

    public static Figures get_figures(ArrayList<Figures> li, String cod) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getSupporto().equals(cod)) {
                return li.get(i);
            }
        }
        Figures vuoto = new Figures();
        vuoto.setDe_supporto("-");
        return vuoto;
    }

    public static CustomerKind get_customerKind(ArrayList<CustomerKind> out, String cod) {
        for (int i = 0; i < out.size(); i++) {
            if (out.get(i).getTipologia_clienti().equals(cod)) {
                return out.get(i);
            }
        }
        return null;
    }

    public static String formatBankBranch(String cod, String type, ArrayList<String[]> array_bank, ArrayList<Branch> array_branch, ArrayList<String[]> credit_card) {
        if ((cod != null)
                && (type != null)) {
            if (type.equals("BA")) {
                for (int j = 0; j < array_bank.size(); j++) {
                    if (cod.equals(((String[]) array_bank.get(j))[0])) {
                        return ((String[]) array_bank.get(j))[1].toUpperCase();
                    }
                }

                for (int j = 0; j < credit_card.size(); j++) {
                    if (cod.equals(((String[]) credit_card.get(j))[0])) {
                        return ((String[]) credit_card.get(j))[1].toUpperCase();
                    }
                }

            }

            if (type.equals("BR")) {
                if (cod.equals("000")) {
                    return "000 - HEAD OFFICE";
                }
                for (int j = 0; j < array_branch.size(); j++) {

                    if (cod.equals((array_branch.get(j)).getCod())) {
                        return (array_branch.get(j)).getCod() + " - " + (array_branch.get(j)).getDe_branch().toUpperCase();
                    }
                }
            }
        }

        return "";
    }

    public static int parseIntR(String value) {
        if (value == null) {
            return 0;
        }
        value = StringUtils.replace(value, "-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        int d1;
        try {
            d1 = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            d1 = 0;
        }
        return d1;
    }

    public static Users get_user(String cod, ArrayList<Users> li) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getCod().equals(cod)) {
                return li.get(i);
            }
        }
        return null;
    }

    public static double divisione_controllozero(double num, double den) {
        double out = 0.0;
        if (den == 0.0) {
            return out;
        }
        try {
            out = num / den;
        } catch (ArithmeticException e) {
            return 0.00;
        }
        return out;
    }

    public static String[] descr_for_report(String descr) {
        Iterable<String> parameters = Splitter.on(" ").split(descr);
        Iterator<String> it = parameters.iterator();
        String st1 = "";
        if (it.hasNext()) {
            st1 = it.next();
        }
        String st2 = "";
        while (it.hasNext()) {
            st2 = st2 + it.next() + " ";
        }
        st2 = st2.trim();
        String[] d = {st1, st2};
        return d;
    }

    public static double parseDoubleR_CZ(GeneraFile gf, String value, boolean buy) {
        if (!gf.isIs_CZ()) {
            return parseDoubleR(gf, value);
        }

        double d1;
        try {
            d1 = Double.parseDouble(value);
            if (buy) {
                d1 = d1 * (-1.0);
            }
        } catch (NumberFormatException e) {
            value = formatDoubleforMysql(gf, value);
            return parseDoubleR_CZ(gf, value, buy);
        }
        return d1;
    }

    public static NC_causal getNC_causal(ArrayList<NC_causal> li, String nc_code) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getCausale_nc().equals(nc_code)) {
                return li.get(i);
            }
        }
        return null;
    }

    public static String subDays(String start, String pattern, int days) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
        DateTime dt = formatter.parseDateTime(start);
        return dt.minusDays(days).toString(pattern);
    }

    public static String formatALCurrency(String cod, ArrayList<Currency> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((Currency) array.get(i)).getCode())) {
                return ((Currency) array.get(i)).getDescrizione();
            }
        }
        return "-";
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

    public static double getControvalore(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return primo * secondo;
        }
    }

    public static double getControvaloreOFFSET(double primo, double secondo, boolean dividi) {
        if (dividi) {
            return primo / secondo;
        } else {
            return secondo / primo;
        }
    }

    public static String getGiornoAdeguatoAnnoPrecedente(String giorno) { //dd/MM/yyyy
        DateTimeFormatter formatter = DateTimeFormat.forPattern(patternnormdate_filter);
        DateTime dtoggi = formatter.parseDateTime(giorno);
        String giornodellasettimanaoggi = dtoggi.dayOfWeek().getAsShortText(Locale.ITALY);

        DateTime dtannoscorso = dtoggi.minusYears(1);
        String giornodellasettimanaannoscorso = dtannoscorso.dayOfWeek().getAsShortText(Locale.ITALY);
        int add = 1;
        while (!giornodellasettimanaoggi.equals(giornodellasettimanaannoscorso)) {
            dtannoscorso = dtannoscorso.plusDays(add);
            giornodellasettimanaannoscorso = dtannoscorso.dayOfWeek().getAsShortText(Locale.ITALY);
        }
        return dtannoscorso.toString(patternnormdate_filter);
    }

    public static String formatStringtoStringDate_null(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (IllegalArgumentException le) {
        }
        return null;
    }

    public static DateTime getDT(String start, String pattern) {
        if (start.length() == 21) {
            start = start.substring(0, 19);
        }
        if (start.length() == pattern.length()) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
            return formatter.parseDateTime(start);
        }
        return null;
    }

    public static String formatType_new(String tipotr, String internetbooking,
            String rimborso, String cau1, String cau2, String cau3, ArrayList<NC_category> listcat, ArrayList<NC_causal> listcaus) {
        if (tipotr != null) {
            if (tipotr.equals("B")) {
                if (internetbooking.equals("1")) {
                    return "BUY - ACQUISTO ON-LINE";
                } else {
                    return "BUY";
                }
            }
            if (tipotr.equals("S")) {
                if (internetbooking.equals("1")) {
                    return "SELL - VENDITA ON-LINE";
                } else if (rimborso.equals("0") || rimborso.equals("-")) {
                    return "SELL - VENDITA A SPORTELLO";
                } else if (rimborso.equals("1")) {
                    ArrayList<String> caus = new ArrayList<>();
                    if (!cau1.equals("-")) {
                        caus.add(getNC_causal(listcaus, cau1).getGruppo_nc());
                    }
                    if (!cau2.equals("-")) {
                        caus.add(getNC_causal(listcaus, cau2).getGruppo_nc());
                    }
                    if (!cau3.equals("-")) {
                        caus.add(getNC_causal(listcaus, cau3).getGruppo_nc());
                    }
                    removeDuplicatesAL(caus);

                    String out = "";
                    for (int c = 0; c < caus.size(); c++) {
                        out = out + getNC_category(listcat, caus.get(c)).getDe_gruppo_nc().toUpperCase() + " ";
                    }

                    return "SELL - RIMBORSO IVA : " + out.trim();
                }
            }
        }
        return "-";
    }

    public static String format_tofrom_brba_new(String fg_tofrom, String fg_brba, String coddest, ArrayList<String[]> array_credit_card,
            ArrayList<String[]> array_bank) {

        if (fg_tofrom.equals("T")) {
            if (fg_brba.equals("BR")) {
                return "TO BRANCH";
            } else if (fg_brba.equals("BA")) {
                for (int i = 0; i < array_credit_card.size(); i++) {
                    if (array_credit_card.get(i)[0].equals(coddest)) {
                        return "TO POS/Bank Account - " + coddest + ": " + array_credit_card.get(i)[1];
                    }
                }
                return "TO BANK - " + coddest + ": " + Utility.formatAL(coddest, array_bank, 1);
            }
        } else if (fg_tofrom.equals("F")) {
            if (fg_brba.equals("BR")) {
                return "FROM BRANCH";
            } else if (fg_brba.equals("BA")) {
                return "FROM BANK - " + coddest + ": " + Utility.formatAL(coddest, array_bank, 1);
            }
        }
        return "";
    }

    public static String get_Value_history_BB(ArrayList<String[]> history_BB, DateTime dt_tr, Figures f) {
        for (int x = 0; x < history_BB.size(); x++) {
            String[] va  = history_BB.get(x);
            DateTime dt1 = getDT(va[0], patternsqldate);
            DateTime dt2 = getDT(va[1], patternsqldate);
            if (dt_tr.isAfter(dt1) && dt_tr.isBefore(dt2)) {
                return va[2];
            }
        }
        return f.getBuy_back_commission();
    }

    public static DateTime parseStringDate(String dat, String pattern1) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                return formatter.parseDateTime(dat);
            }
        } catch (IllegalArgumentException ex) {

        }
        return null;
    }

    public static String formatMysqltoDisplay(String ing) {
        String decimal = ",";
        String thousand = ".";

        if (ing == null) {
            return "";
        }
        if (ing.trim().equals("") || ing.trim().equals("-")) {
            return "";
        }
        if (ing.length() == 0) {
            return "";
        }
        if (ing.trim().startsWith(".") || ing.trim().startsWith(",")) {
            return "0" + decimal + "00";
        }

        String start = ing.substring(0, 1);
        if (start.equals("-") || start.equals("+")) {
            ing = StringUtils.replace(ing, start, "");
        } else {
            start = "";
        }

        String out = "";
        if (ing.contains(",")) {
            ing = StringUtils.replace(ing, ",", "");
        }
        if (ing.contains(".")) {
            String[] inter1 = splitStringEvery(ing.split("\\.")[0], 3);
            if (inter1.length > 1) {
                for (String inter11 : inter1) {
                    out = out + inter11 + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
            String dec = ing.split("\\.")[1];
            out = out + decimal + dec;
        } else {

            String[] inter1 = splitStringEvery(ing, 3);
            if (inter1.length > 1) {
                for (String inter11 : inter1) {
                    out = out + inter11 + thousand;
                }
            } else {
                out = inter1[0];
            }
            if (out.lastIndexOf(thousand) + 1 == out.length()) {
                out = out.substring(0, out.lastIndexOf(thousand));
            }
        }
        return start + out;
    }

    public static String[] splitStringEvery(String s, int interval) {
        int arrayLength = (int) Math.ceil(s.length() / (double) interval); 
        String[] result = new String[arrayLength]; 
        int endIndex = s.length(); 
       
        for (int i = arrayLength - 1; i >= 0; i--) {
            int startIndex = Math.max(endIndex - interval, 0); 
            result[i] = s.substring(startIndex, endIndex);
            endIndex -= interval; 
        }
        
        return result; 
    }
    

    public static String getValueDiff_R(String value1, String value2, String diff, String rate, boolean dividi) {
        double diff1 = fd(value1) - fd(value2);
        double out;
        if (dividi) {
            out = diff1 / fd(rate);
        } else {
            out = diff1 * fd(rate);
        }
        return roundDoubleandFormat(out, 2);
    }

    public static String sanitizeInput(String in) {
        try {
            return Jsoup.clean(in, Whitelist.basic());
        } catch (Exception e) {
        }
        return in;
    }
}
