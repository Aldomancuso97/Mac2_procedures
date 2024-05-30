/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *
 * @author rcosco
 */
public class Monitor {

    public static final ResourceBundle rb = ResourceBundle.getBundle("maintenance.conf");

    private static final String host = rb.getString("path.monitor.host");
    private static final int port = parseIntR(rb.getString("path.monitor.port"));
    private static final String usr = rb.getString("path.monitor.user");
    private static final String psw = rb.getString("path.monitor.psw");
//    private static final String dir = rb.getString("path.monitor.dir");
    private static final String config = rb.getString("path.monitor.config");
    private static final String patterndir = "yyyyMMdd";
    private static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    private static final String pattern4 = "yyyyMMdd";
    private static final String pattern3 = "HHmmssSSS";

    public static final Logger log = createLog("Mac2.0_MONITOR", rb.getString("path.log"), pattern4);

    private static Logger createLog(String appname, String folderini, String patterndatefolder) {
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

    private static String addStandard(String buy_std, String cambio_bce) {
        double d_rifbce = Double.parseDouble(cambio_bce);
        double d_standard = Double.parseDouble(buy_std);
        double tot_st = d_rifbce * (100 + d_standard) / 100;
        return roundDoubleandFormat(tot_st, 3);
    }

    private static boolean generateFile(String path, String filiale, ArrayList<Branch> allbr, FTPClient ftpClient) {
        Db_Master dbm = new Db_Master();
        ArrayList<String[]> al = dbm.getValuteForMonitor(filiale);
        dbm.closeDB();
        if (!al.isEmpty()) {

            if (filiale.equals("---")) {
                boolean es1 = true;
                for (int i = 0; i < allbr.size(); i++) {
                    ArrayList<String[]> alinside = new ArrayList<>();
                    String fil = allbr.get(i).getCod();
                    for (int j = 0; j < al.size(); j++) {
                        if (fil.equals(al.get(j)[9])) {
                            alinside.add(al.get(j));
                        }
                    }
                    if (!alinside.isEmpty()) {
                        boolean es = printFile(path, fil, alinside, config, ftpClient);
                        if (!es) {
                            es1 = false;
                            break;
                        }
                    }
                }
                return es1;
            } else {
                return printFile(path, filiale, al, config, ftpClient);
            }

        }
        return false;
    }

    public static boolean printFile(String path, String filiale, ArrayList<String[]> al, String config, FTPClient ftpClient) {
        try {
            String directory_str = sanitizePath(path + new DateTime().toString(patterndir)); 
            new File(directory_str).mkdirs();

            File xml_output = new File(directory_str + File.separator + filiale + ".xml");
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("valute");
            doc.appendChild(rootElement);

            DateTime d = new DateTime();
            String dataMillis = d.toString(patternnormdate);
            DateTimeFormatter df = DateTimeFormat.forPattern(patternnormdate);
            DateTime d2 = df.parseDateTime(dataMillis);
            Element timestamp = doc.createElement("timestamp");
            dataMillis = (d2.getMillis() + "").substring(0, 10);
            timestamp.appendChild(doc.createTextNode(dataMillis));
            rootElement.appendChild(timestamp);

            for (int i = 0; i < al.size(); i++) {
                if (!addStandard(al.get(i)[5], al.get(i)[2]).equals("0.0") && !addStandard(al.get(i)[8], al.get(i)[2]).equals("0.0")) {

                    if (al.get(i)[10].equals("0") && al.get(i)[11].equals("0")) {
                        continue;
                    }

                    Element valuta = doc.createElement("valuta");
                    rootElement.appendChild(valuta);

                    Element code = doc.createElement("code");
                    code.appendChild(doc.createTextNode(al.get(i)[0].toUpperCase()));
                    valuta.appendChild(code);

                    Element immagine = doc.createElement("immagine");
                    immagine.appendChild(doc.createCDATASection(StringUtils.replace(config, "###", al.get(i)[0].toLowerCase())));
                    valuta.appendChild(immagine);

                    Element nome = doc.createElement("nome");
                    nome.appendChild(doc.createTextNode(al.get(i)[1]));
                    valuta.appendChild(nome);

                    Element buy = doc.createElement("buy");
                    if (al.get(i)[10].equals("0")) {
                        buy.appendChild(doc.createTextNode(""));
                    } else {
                        if (al.get(i)[4].equals("0")) {
                            buy.appendChild(doc.createTextNode((addStandard(al.get(i)[5], al.get(i)[2]))));
                        } else {
                            buy.appendChild(doc.createTextNode(al.get(i)[4]));
                        }
                    }
                    valuta.appendChild(buy);

                    Element sell = doc.createElement("sell");
                    if (al.get(i)[11].equals("0")) {
                        sell.appendChild(doc.createTextNode(""));
                    } else {
                        if (al.get(i)[7].equals("0")) {
                            sell.appendChild(doc.createTextNode((addStandard(al.get(i)[8], al.get(i)[2]))));
                        } else {
                            sell.appendChild(doc.createTextNode(al.get(i)[6]));
                        }
                    }
                    valuta.appendChild(sell);

                    Element alwaysVisible = doc.createElement("alwaysVisible");
                    alwaysVisible.appendChild(doc.createTextNode("true"));
                    valuta.appendChild(alwaysVisible);
                }
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(xml_output);
            transformer.transform(source, result);

            boolean es = ftpUploadFile(ftpClient, xml_output);
            return es;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return false;
    }

    private static String sanitizePath(String input) {
        return input.replaceAll("[^a-zA-Z0-9./\\\\]", "");
    }
    
    private static String roundDoubleandFormat(double d, int scale) {
        return StringUtils.replace(String.format("%." + scale + "f", d), ",", ".");
    }

    private static int parseIntR(String value) {
        value = value.replaceAll("-", "").trim();
        if (value.contains(".")) {
            StringTokenizer st = new StringTokenizer(value, ".");
            value = st.nextToken();
        }
        int d1;
        try {
            d1 = Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
            d1 = 0;
        }
        return d1;
    }

    private static FTPClient ftpConnect(String server, int port, String user, String pass) {
        try {
            FTPClient ftpClient = new FTPClient();
            ftpClient.connect(server, port);
            ftpClient.setDefaultTimeout(2000);
            boolean logi = ftpClient.login(user, pass);
            if (logi) {
                if (ftpClient.isConnected()) {
                    log.warning("FTP CONNECTED.");
                    return ftpClient;
                } else {
                    log.severe("1 FTP NOT CONNECTED.");
                }
            } else {
                log.severe("2 FTP NOT CONNECTED.");
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return null;
    }

    private static boolean ftpUploadFile(FTPClient ftpClient, File fileup) {
        try {
            String firstRemoteFile = StringUtils.deleteWhitespace(fileup.getName());
            boolean done;
            try (InputStream inputStream = new FileInputStream(fileup)) {
                done = ftpClient.storeFile(firstRemoteFile, inputStream);
            }
            if (done) {
                long originalsize = fileup.length();
                FTPFile[] filenames = ftpClient.listFiles(firstRemoteFile);
                for (FTPFile filename : filenames) {
                    long destsize = filename.getSize();
                    long perce = originalsize * 5 / 100;
                    long range = originalsize - perce;
                    if (destsize > range) {
                        log.log(Level.INFO, "{0} UPLOADED.", filename.getName());
                        return true;
                    }
                }
            }
        } catch (Exception ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return false;
    }

    private static boolean ftpDisconnect(FTPClient ftpClient) {
        try {
            if (ftpClient.isConnected()) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
            log.warning("FTP DISCONNECTED.");
            return true;
        } catch (IOException ex) {
            log.log(Level.SEVERE, "{0}: {1}", new Object[]{ex.getStackTrace()[0].getMethodName(), ex.getMessage()});
        }
        return false;
    }

    public static void exe() {
        Db_Master dbm = new Db_Master();
        String path = dbm.getPath("temp");
        ArrayList<Branch> brl = dbm.list_branch_enabled();
        dbm.closeDB();

        FTPClient ftpClient = ftpConnect(host, port, usr, psw);
        if (ftpClient != null) {
            ftpClient.enterLocalPassiveMode();
//            ftpChangeDir(ftpClient, dir);
            generateFile(path, "---", brl, ftpClient);
            ftpDisconnect(ftpClient);
        }

    }
    
    
//    public static void main(String[] args) {
//        System.out.println("rc.soop.maintenance.Monitor.main() "+(addStandard("16.00", "25011.00")));
//        System.out.println("rc.soop.maintenance.Monitor.main() "+formatValue(addStandard("16.00", "25011.00")));
//    }
}
