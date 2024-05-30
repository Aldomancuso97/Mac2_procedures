/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import com.jcraft.jsch.ChannelSftp;
import rc.soop.sftp.SftpConnection;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static rc.soop.esolver.ESolver.sanitizeInput;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class Util {

    public static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";

    public static final String patternatl = "yyMMdd";

    public static final String patternnormdate_filter = "dd/MM/yyyy";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patterncomplete = "yyMMddHHmmssSSS";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternyear = "yyyy";
    public static final String thousand = ".";
    public static final String decimal = ",";

    public static final ResourceBundle rb = ResourceBundle.getBundle("esolver.conf");
    public static final boolean test = rb.getString("test").equals("SI");

    public static final Logger log = createLog("Mac2.0_ESOLVER", rb.getString("path.log"), pattern4);

    public static Logger createLog(String appname, String folderini, String patterndatefolder) {
        Logger LOGGER = Logger.getLogger(appname);
        try {
            DateTime dt = new DateTime();
            String filename = appname + "-" + dt.toString(pattern3) + ".log";
            File dirING = new File(folderini);
            dirING.mkdirs();
            if (patterndatefolder != null) {
                File dirINGNew = new File(dirING.getPath() + File.separator + dt.toString(patterndatefolder));
                dirINGNew.mkdirs();
                filename = dirINGNew.getPath() + File.separator + filename;
            } else {
                filename = dirING.getPath() + File.separator + filename;
            }
            Handler fileHandler = new FileHandler(filename);
            LOGGER.addHandler(fileHandler);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException localIOException) {
        }

        return LOGGER;
    }

    public static String formatDoubleforMysql(String value) {
        if (value == null) {
            return "0.00";
        }
        if (value.equals("-") || value.equals("")) {
            return "0.00";
        }
        String add = "";
        if (value.contains("-")) {
            add = "-";
            value = StringUtils.replace(value.trim(), "-", "").trim();
        }

        if (!value.equals("0.00")) {
            if (thousand.equals(".")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(",")) {
                        value = StringUtils.replace(value, ".", "");
                        value = StringUtils.replace(value, ",", ".");
                    }
                } else {

                    value = StringUtils.replace(value, ".", "");
                    return add + value + ".00";

                }
            } else if (thousand.equals(",")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(".")) {
                        value = value.replaceAll(",", "");
                    }
                } else {
                    value = value.replaceAll(",", "");
                    return value + "00";

                }
            }
        }

        return add + value;

    }

    public static String generaId() {
        String random = RandomStringUtils.randomAlphanumeric(5).trim();
        return new DateTime().toString(patterncomplete) + random;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString(patterncomplete) + random;
    }

    public static String roundDoubleandFormat(double d, int scale) {
        String dest = "%." + scale + "f";
        return StringUtils.replace(String.format(dest, d), ",", ".");
    }

    public static boolean containsInfinity(String ing) {
        String infinitySymbol;
        try {
            infinitySymbol = new String(String.valueOf(Character.toString('\u221E')).getBytes("UTF-8"), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            infinitySymbol = "?";
        }
        return ing.contains(infinitySymbol);
    }

    public static double fd(String si_t_old) {
        try {
            if (containsInfinity(si_t_old)) {
                return 0.0D;
            }
            si_t_old = si_t_old.replace(",", "").trim();
            double d1 = Double.parseDouble(si_t_old);
            return d1;
        } catch (Exception ex) {
            return 0.0D;
        }

    }

    public static double parseDoubleR(String value) {
        if (containsInfinity(value)) {
            return 0.0D;
        }
        value = StringUtils.replace(value, " ", "");
        value = StringUtils.replace(value, "\\s+", "");
        value = StringUtils.replace(value, "-", "");
        value = StringUtils.replace(value, "\\r", "");
        value = StringUtils.replace(value, "\\t", "");
        value = StringUtils.replace(value, "\\n", "");
        value = StringUtils.deleteWhitespace(value).trim();
        try {
            double d1 = Double.valueOf(value);
            return d1;
        } catch (Exception ex) {
            // ex.printStackTrace();
            // log.log(Level.SEVERE, "{0}: {1}", new
            // Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
            value = formatDoubleforMysql(value);
            return Double.valueOf(value);
            // return Double.parseDouble(value);
        }

    }

    public static String formatStringtoStringDate(String dat, String pattern1, String pattern2) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt = formatter.parseDateTime(dat);
                return dt.toString(pattern2, Locale.ITALY);
            }
        } catch (IllegalArgumentException ex) {
        }
        return dat;
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
        } catch (IllegalArgumentException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[] { ex.getStackTrace()[0].getMethodName(), ex.getMessage() });
        }
        return null;
    }

    public static String visualizzaStringaMySQL(String ing) {
        if (ing == null) {
            return "";
        }
        ing = StringUtils.replace(ing, "\\'", "'");
        ing = StringUtils.replace(ing, "\'", "'");
        ing = StringUtils.replace(ing, "\"", "'");
        // ing = StringUtils.replace(ing, "€", "&#0128;");
        return ing.trim();
    }

    public static int parseIntR(String value) {
        value = value.replaceAll("-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        int d1;
        try {
            d1 = Integer.parseInt(value);
        } catch (Exception ex) {
            // log.log(Level.SEVERE, "{0}: {1}", new
            // Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
            d1 = 0;
        }
        return d1;
    }

    public static String getValueDiff(String value1, String value2,
            String diff, String rate, boolean dividi) {

        if (fd(value2) == 0 || fd(rate) == 0) {
            return formatRa1(0.0);
        }

        String add;
        if (fd(value1) >= fd(value2)) {
            add = "";
        } else {
            add = "-";
        }

        double out;
        if (dividi) {
            double divider = fd(rate);
            if (divider == 0) {
                return "0.00";
            } else {
                out = fd(diff) / divider;
            }
        } else {
            out = fd(diff) * fd(rate);
        }

        String output = formatRa1(out);
        if (output.contains("-")) {
            return output;
        } else {
            return add + output;
        }
    }

    public static String formatRa1(double d1) {
        if (d1 >= 0) {
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ITALY);
            nf.setGroupingUsed(true);
            return StringUtils.deleteWhitespace(nf.format(d1)).trim();
        } else {
            DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(Locale.ITALY);
            DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
            symbols.setCurrencySymbol("");
            formatter.setDecimalFormatSymbols(symbols);
            String out = StringUtils.replace(formatter.format(d1), " ", "").trim();
            out = StringUtils.replace(out, " ", "").trim();
            out = StringUtils.replace(out, "\n", "").trim();
            out = StringUtils.replace(out, "\r", "").trim();
            out = StringUtils.replace(out, "\t", "").trim();
            return StringUtils.deleteWhitespace(out).trim();
        }
    }

    public static double calcolaIva(double total, double percentiva) {
        double t = 100.00;
        double out = total / ((t + percentiva) / t);
        return total - out;
    }

    public static Ch_transaction query_transaction_ch(String cod) {
        Db_Master db = new Db_Master();
        Ch_transaction out = db.query_transaction_ch(cod);
        db.closeDB();
        return out;
    }

    public static CustomerKind get_customerKind(String cod) {
        Db_Master db = new Db_Master();
        CustomerKind out = db.get_customerKind(cod);
        db.closeDB();
        return out;
    }

    public static ArrayList<Ch_transaction_value> query_transaction_value(String cod_tr) {
        Db_Master db = new Db_Master();
        ArrayList<Ch_transaction_value> out = db.query_transaction_value(cod_tr);
        db.closeDB();
        return out;
    }

    public static ArrayList<ET_change> get_ET_change_value(String cod) {
        Db_Master db = new Db_Master();
        ArrayList<ET_change> es = db.get_ET_change_value(cod);
        db.closeDB();
        return es;
    }

    public static ArrayList<String[]> list_oc_errors(String cod_oc) {
        Db_Master db = new Db_Master();
        ArrayList<String[]> out = db.list_oc_errors(cod_oc);
        db.closeDB();
        return out;
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        String out = "-";
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                out = array.get(i)[index];
            }
        }
        return out;
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

    public static String[] getCity_apm(String co) {
        Db_Master db = new Db_Master();
        String[] out = db.getCity_apm(co);
        db.closeDB();
        if (out == null) {
            String[] o1 = { co, co, "-" };
            return o1;
        }
        return out;
    }

    // public static NC_category getNC_category(ArrayList<NC_category> li, String
    // nc_code) {
    // for (int i = 0; i < li.size(); i++) {
    // if (li.get(i).getGruppo_nc().equals(nc_code)) {
    // return li.get(i);
    // }
    // }
    // return null;
    // }
    public static boolean removeDuplicatesALAr(ArrayList<String[]> l) {
        int sizeInit = l.size();
        Iterator<String[]> p = l.iterator();
        while (p.hasNext()) {
            String[] op = p.next();
            Iterator<String[]> q = l.iterator();
            String[] oq = q.next();
            while (!op[0].equals(oq[0]) || !op[1].equals(oq[1])) {
                oq = q.next();
            }
            boolean b = q.hasNext();
            while (b) {
                oq = q.next();
                if (op[0].equals(oq[0]) && op[1].equals(oq[1])) {
                    p.remove();
                    b = false;
                } else {
                    b = q.hasNext();
                }
            }
        }
        return sizeInit != l.size();
    }

    public static VATcode get_vat(String cod, ArrayList<VATcode> valist) {

        for (int i = 0; i < valist.size(); i++) {
            if (valist.get(i).getCodice().equals(cod)) {
                return valist.get(i);
            }
        }

        return null;
    }

    public static String formatBankBranchReport(String cod, String type, ArrayList<String[]> array_bank,
            ArrayList<Branch> array_branch) {
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

    public static NC_category getNC_category(ArrayList<NC_category> li, String nc_code) {
        try {
            NC_category n = li.stream().filter(nc1 -> nc1.getGruppo_nc().equalsIgnoreCase(nc_code)).findAny()
                    .orElse(null);
            return n;
        } catch (Exception ex) {

        }
        return null;
    }

    public static NC_causal getNC_causal(ArrayList<NC_causal> li, String nc_code, String nc_category) {
        try {

            NC_causal n = li.stream().filter(nc1 -> nc1.getCausale_nc().equalsIgnoreCase(nc_code)
                    && nc1.getGruppo_nc().equalsIgnoreCase(nc_category)).findAny().orElse(null);
            return n;
            // System.out.println(nc_code + "esolver.Util.getNC_causal() " + nc_category + "
            // : " + (n == null));
        } catch (Exception ex) {

        }
        return null;
    }

    // public static NC_causal getNC_causal(ArrayList<NC_causal> li, String nc_code)
    // {
    // for (int i = 0; i < li.size(); i++) {
    // if (li.get(i).getCausale_nc().equals(nc_code)) {
    // return li.get(i);
    // }
    // }
    // return null;
    // }
    public static Users get_user(String cod, ArrayList<Users> li) {
        for (int i = 0; i < li.size(); i++) {
            if (li.get(i).getCod().equals(cod)) {
                return li.get(i);
            }
        }
        return null;
    }

    public static boolean checkTXT(File txt) {
        if (txt != null) {
            try {
                String st;
                try (FileReader fr = new FileReader(sanitizeFile(txt));
                        BufferedReader br = new BufferedReader(fr)) {
                    while ((st = br.readLine()) != null) {
                        return !st.trim().equals("");
                    }
                }
            } catch (Exception ex) {
                log.log(Level.SEVERE, "{0}: {1} - {2}", new Object[] { ex.getStackTrace()[0].getMethodName(),
                        sanitizeInput(txt.getName()), ex.getMessage() });
            }
        }
        return false;
    }

    public static File sanitizeFile(File file) {
        String filePath = file.getPath().replaceAll("[^a-zA-Z0-9._-]", "");
        return new File(filePath);
    }

    public static String getAnno(String from) {
        try {
            String v = Stream.of(from.split("/")).map(elem -> elem).collect(Collectors.toList()).get(2);
            return v;
        } catch (Exception e) {
        }
        return "";
    }

    public static String verificaFE(String ing) {
        if (ing.equalsIgnoreCase("IT")) {
            return "2";
        } else {
            return "0";
        }

    }

    public static String[] verificaClientNumber(String ing) {
        String out[] = { "", "" };
        if (ing != null) {
            if (ing.trim().length() == 16) {
                out[0] = ing.trim().toUpperCase();
            } else if (ing.trim().length() == 11 || ing.trim().length() == 13) {
                out[1] = ing.trim().toUpperCase();
            }
        }
        return out;
    }

    public static DateTime getDateRif(String from) {
        try {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(patternnormdate_filter);
            DateTime dt = formatter.parseDateTime(from);
            return dt;
        } catch (Exception ex) {
            return null;
        }
    }

    // public static List<String> branchList_BT() {
    // return Lists.newArrayList(Splitter.on(";").split(rb.getString("bt")));
    // }
    public static boolean sendMailHtml(String[] dest, String oggetto, String testo, File filedallegare) {
        Properties props = new Properties();
        props.put("mail.smtp.host", rb.getString("mail.smtp"));
        props.put("mail.smtp.socketFactory.port", rb.getString("mail.smtp.port"));
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", rb.getString("mail.smtp.port"));
        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(rb.getString("mail.sender"), rb.getString("mail.pass"));
            }
        });
        try {
            Message message = new MimeMessage(session);
            InternetAddress froms = new InternetAddress(rb.getString("mail.sender"));
            froms.setPersonal("Noreply Mac 2.0");
            message.setFrom(froms);

            for (String dest1 : dest) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(dest1));
            }

            message.setRecipient(Message.RecipientType.CC, new InternetAddress("mac2.0@smartoop.it"));

            message.setSubject(oggetto);
            Multipart mp = new MimeMultipart();
            MimeBodyPart mbp1 = new MimeBodyPart();
            mbp1.setContent(testo, "text/html");
            mp.addBodyPart(mbp1);
            if (filedallegare != null) {
                DataSource source = new FileDataSource(filedallegare);
                MimeBodyPart messageBodyPart = new MimeBodyPart();
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(filedallegare.getName());
                mp.addBodyPart(messageBodyPart);
            }
            message.setContent(mp);
            Transport.send(message);
            return true;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "send_Mail_HTML {0}", ex.getMessage());
        }
        return false;
    }

    public static File createFile_R(String pathfile) {
        try {
            File myObj = new File(pathfile);
            if (myObj.createNewFile()) {
                log.log(Level.INFO, "File created: {0}", pathfile);
            } else {
                log.log(Level.INFO, "File already exists: {0}", pathfile);
            }
            return myObj;
        } catch (Exception e) {
            log.log(Level.SEVERE, "createNewFile {0}", estraiEccezione(e));
        }
        try {
            return Files.createFile(Paths.get(pathfile)).toFile();
        } catch (Exception e) {
            log.log(Level.SEVERE, "Files.createFile {0}", estraiEccezione(e));
        }
        try {
            File myObj = new File(pathfile);
            com.google.common.io.Files.touch(myObj);
            return myObj;
        } catch (Exception e) {
            log.log(Level.SEVERE, "com.google.common.io.Files.touch {0}", estraiEccezione(e));
        }
        try {
            File myObj = new File(pathfile);
            FileUtils.touch(myObj);
            return myObj;
        } catch (Exception e) {
            log.log(Level.SEVERE, "FileUtils.touch {0}", estraiEccezione(e));
        }

        return null;
    }

    public static void copyFile_R(File ing, File out) {
        boolean response = false;
        try {
            FileUtils.copyFile(ing, out);
            if (out.length() > 0 && out.canRead()) {
                response = true;
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "FileUtils.copyFile {0}", estraiEccezione(e));
            response = false;
        }
        if (!response) {
            try {
                Files.copy(ing.toPath(), out.toPath());
                if (out.length() > 0 && out.canRead()) {
                    response = true;
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Files.copy {0}", estraiEccezione(e));
                response = false;
            }
        }
        if (!response) {
            try {
                try (FileChannel sourceChannel = new FileInputStream(ing).getChannel();
                        FileChannel destChannel = new FileOutputStream(out).getChannel()) {
                    destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
                }
                if (out.length() > 0 && out.canRead()) {
                    response = true;
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "FileChannel {0}", estraiEccezione(e));
                response = false;
            }
        }

        if (!response) {
            try {
                try (InputStream is = new FileInputStream(ing); OutputStream os = new FileOutputStream(out)) {
                    byte[] buffer = new byte[4096];
                    int length;
                    while ((length = is.read(buffer)) > 0) {
                        os.write(buffer, 0, length);
                    }
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "buffer {0}", estraiEccezione(e));
            }
        }

    }

    public static String estraiEccezione(Exception ec1) {
        try {
            return ec1.getStackTrace()[0].getMethodName() + " - " + getStackTrace(ec1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ec1.getMessage();

    }

    public static boolean rilasciaFileEsolverFTP(File file) {

        try {
            log.warning("FTP RILASCIO");
            String se_user = rb.getString("ftp.user");
            String se_pwd = rb.getString("ftp.pass");
            String se_ip = rb.getString("ftp.ip");
            int se_port = Util.parseIntR(rb.getString("ftp.port"));
            FTPClient ftpClient = new FTPClient();
            ftpClient.connect(se_ip, se_port);
            ftpClient.login(se_user, se_pwd);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            if (ftpClient.isConnected()) {
                try {
                    try (InputStream is = new FileInputStream(file)) {
                        ftpClient.appendFile(file.getName(), is);
                    }
                } catch (Exception e) {
                    log.log(Level.SEVERE, "rilasciaFileEsolverFTP UPLOAD {0}", estraiEccezione(e));
                    return false;
                }
                ftpClient.disconnect();
                return true;
            } else {
                log.log(Level.SEVERE, "FTP {0} NON CONNESSO", se_ip);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "rilasciaFileEsolverFTP {0}", estraiEccezione(e));
        }
        return false;
    }

    public static boolean rilasciaFileEsolver(File ing) {
        try {
            if (rb.getString("protocol").equalsIgnoreCase("SFTP")) {
                return rilasciaFileEsolverSFTP(sanitizeFile(ing));
            }
        } catch (Exception e) {
        }
        return rilasciaFileEsolverFTP(sanitizeFile(ing));
    }

    public static boolean rilasciaFileEsolverSFTP(File ing) {
        try {
            log.warning("SFTP RILASCIO");
            ChannelSftp es1 = SftpConnection.connect(
                    rb.getString("sftp.user"),
                    rb.getString("sftp.pass"),
                    rb.getString("sftp.ip"),
                    Util.parseIntR(rb.getString("sftp.port")),
                    log);
            if (es1 != null && es1.isConnected()) {
                es1.cd(rb.getString("sftp.path"));
                try (InputStream is = new FileInputStream(ing)) {
                    es1.put(is, ing.getName());
                }
                SftpConnection.closeConnection(es1, log);
                return true;
            } else {
                log.severe("SFTP ESOLVER NON CONNESSO");
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "rilasciaFIleEsolver {0}", estraiEccezione(e));
        }
        return false;
    }

    // public static void main(String[] args) {
    // String se = StringUtils.deleteWhitespace("0 10 1 1");
    // System.out.println("esolver.Util.main() "+se.startsWith("01011"));
    //
    // String out[] = verificaClientNumber("22512474584IT");
    // System.out.println("esolver.Util.main() " + out[0]);
    // System.out.println("esolver.Util.main() " + out[1]);
    //
    //// System.out.println(checkTXT(new
    // File("C:\\Users\\rcosco\\Desktop\\2.txt")));
    // }
}
