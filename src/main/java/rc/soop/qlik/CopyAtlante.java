/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.qlik;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.joda.time.DateTime;
import rc.soop.esolver.Util;

/**
 *
 * @author rcosco
 */
public class CopyAtlante {

    private static final ResourceBundle rb = ResourceBundle.getBundle("qlik.conf");
    private static final String hostsiap = rb.getString("hostsiap");
    private static final String usersiap = rb.getString("usersiap");
    private static final String passsiap = rb.getString("passsiap");

    private static final String pathtemp = rb.getString("pathtemp");
    private static final String pathdest = rb.getString("pathdest");

    // private static final String sambaDomain = rb.getString("sambaDomain");
    // private static final String sambaUsername = rb.getString("sambaUsername");
    // private static final String sambaPass = rb.getString("sambaPass");
    // private static final String sambaIP = rb.getString("sambaIP");
    // private static final String sambaSharedPath =
    // rb.getString("sambaSharedPath");
    // private static final String sambaSharedPath =
    // rb.getString("sambaSharedPath");
    public static void engine() {
        try {
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        } catch (Exception e) {
            // Ignorare eccezione se già presente
        }

        Db db = new Db(false);
        LoggerNew logger = new LoggerNew("FTP_QLIK", db.getPath("Pathlog"));
        db.closeDB();

        try {
            logger.log.info("FTP ATLANTE BIGLIETTI");
            DateTime now = new DateTime();
            String date = now.toString("yyyy-MM-dd");
            FTPClient ftpClient = new FTPClient();

            try {
                ftpClient.connect(hostsiap, 21);
                ftpClient.login(usersiap, passsiap);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

                FTPFileFilter ff = (FTPFile ftpf) -> ftpf.getName().toUpperCase().contains("BIGLIETTI")
                        || ftpf.getName().toUpperCase().contains("PRATICHE")
                        || ftpf.getName().toUpperCase().contains("VOUCHER");

                FTPFile[] files = ftpClient.listFiles("/ExportBI" + File.separator + date + File.separator, ff);
                new File(date).mkdirs();

                if (files != null) {
                    logger.log.log(Level.INFO, "NUMERO FILES DISPONIBILI: {0}", files.length);

                    for (FTPFile aFile : files) {
                        if (!aFile.isDirectory()) {
                            File downloadFile1 = new File(date + File.separator + aFile.getName());
                            boolean success;
                            try (OutputStream outputStream1 = new BufferedOutputStream(
                                    new FileOutputStream(downloadFile1))) {
                                success = ftpClient.retrieveFile(
                                        "/ExportBI" + File.separator + date + File.separator + aFile.getName(),
                                        outputStream1);
                            }

                            if (success) {
                                String name = downloadFile1.getName();
                                if (name.toUpperCase().contains("BIGLIETTI")) {
                                    name = "BIGLIETTI.xls";
                                } else if (name.toUpperCase().contains("PRATICHE")) {
                                    name = "PRATICHE.xls";
                                } else if (name.toUpperCase().contains("VOUCHERS")) {
                                    name = "VOUCHERS.xls";
                                } else {
                                    continue;
                                }

                                File out = convertXls(downloadFile1, logger);
                                logger.log.log(Level.INFO, "DEST: {0}", out.getPath());
                                try {
                                    Path from = out.toPath(); // convert from File to Path
                                    Path to = Paths.get(pathdest + name); // convert from String to Path
                                    Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
                                    logger.log.log(Level.INFO,
                                            "{0} SCARICATO CORRETTAMENTE E CARICATO NELLA CARTELLA DI QLIK", name);
                                } catch (Exception e) {
                                    logger.log.log(Level.SEVERE, "{0} ERRORE NUMERO FILES DISPONIBILI: ", name);
                                    logger.log.severe(Util.estraiEccezione(e));
                                }
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                logger.log.severe(ex.getMessage());
            } finally {
                try {
                    ftpClient.logout();
                    ftpClient.disconnect();
                } catch (IOException ex) {
                    logger.log.severe(ex.getMessage());
                }
            }
        } catch (Exception ex) {
            logger.log.severe(ex.getMessage());
        }
    }

    private static File convertXls(File ing, LoggerNew log) {
        try {
            DateTime inizioanno = new DateTime().withMonthOfYear(1).withDayOfMonth(1).withMillisOfDay(0);
            DateTime oggi = new DateTime().withMillisOfDay(0);
            List<String> elencofogliOK = new ArrayList<>();
            while (inizioanno.isBefore(oggi)) {
                elencofogliOK.add(inizioanno.toString("MM") + "_"
                        + inizioanno.monthOfYear().getAsText(Locale.ITALY).toUpperCase());
                inizioanno = inizioanno.plusMonths(1);
            }
            List<String> elencofogli = new ArrayList<>();

            try (FileInputStream fis = new FileInputStream(ing);
                    HSSFWorkbook wb = new HSSFWorkbook(fis)) {
                for (int i = 0; i < wb.getNumberOfSheets(); i++) {
                    elencofogli.add(wb.getSheetAt(i).getSheetName());
                }
                String nomefoglio = oggi.toString("MM") + "_"
                        + oggi.monthOfYear().getAsText(Locale.ITALY).toUpperCase();
                if (!elencofogli.contains(nomefoglio)) {
                    elencofogliOK.forEach(f1 -> {
                        if (!elencofogli.contains(f1)) {
                            HSSFSheet nuovofoglio = wb.cloneSheet(elencofogli.size() - 1);
                            wb.setSheetName(wb.getSheetIndex(nuovofoglio), f1);
                            nuovofoglio.getLastRowNum();
                            log.log.info(f1 + " DA CREARE " + nuovofoglio.getLastRowNum());
                            if (nuovofoglio.getLastRowNum() > 6) {
                                for (int x = 7; x <= nuovofoglio.getLastRowNum(); x++) {
                                    HSSFRow r = nuovofoglio.getRow(x);
                                    if (r != null) {
                                        for (int y = 0; y <= r.getLastCellNum(); y++) {
                                            if (r.getCell(y) != null) {
                                                r.getCell(y).setCellValue("");
                                            }
                                        }
                                    }
                                }
                                wb.setForceFormulaRecalculation(true);
                                log.log.info(f1 + " RIGHE DA SVUOTARE ");
                            }
                        }
                    });
                    elencofogli.forEach(f1 -> {
                        if (!elencofogliOK.contains(f1)) {
                            wb.removeSheetAt(wb.getSheetIndex(f1));
                            log.log.info(f1 + " DA ELIMINARE");
                        }
                    });
                    File file2 = new File(ing.getPath() + "_v2.xls");
                    try (FileOutputStream outputStream = new FileOutputStream(file2)) {
                        wb.write(outputStream);
                    }
                    return file2;
                }
            }
        } catch (Exception e) {
            log.log.severe(e.getMessage());
        }
        return ing;
    }

    // private static boolean copy_SMB(DiskShare share, String filename, byte[]
    // bytes, LoggerNew logger) {
    // // this is com.hierynomus.smbj.share.File !
    // int idx = filename.lastIndexOf("/");
    // // if file is in folder(s), create them first
    // if (idx > -1) {
    // String folder = filename.substring(0, idx);
    // try {
    // if (!share.folderExists(folder)) {
    // share.mkdir(folder);
    // }
    // } catch (Exception ex) {
    // logger.log.severe(ex.getMessage());
    // return false;
    // }
    // }
    // // I am creating file with flag FILE_CREATE, which will throw if file exists
    // already
    // if (share.fileExists(filename)) {
    // share.rm(filename);
    // }
    // com.hierynomus.smbj.share.File f = share.openFile(filename,
    // new HashSet<>(Arrays.asList(AccessMask.GENERIC_ALL)),
    // new HashSet<>(Arrays.asList(FileAttributes.FILE_ATTRIBUTE_NORMAL)),
    // SMB2ShareAccess.ALL,
    // SMB2CreateDisposition.FILE_CREATE,
    // new HashSet<>(Arrays.asList(SMB2CreateOptions.FILE_DIRECTORY_FILE))
    // );
    // if (f == null) {
    // return false;
    // }
    // try {
    // try (OutputStream os = f.getOutputStream()) {
    // os.write(bytes);
    // }
    // return true;
    // } catch (Exception ex) {
    // logger.log.severe(ex.getMessage());
    // }
    // return false;
    // }
    // public static void main(String[] args) {
    // try {
    // SmbConfig cfg = SmbConfig.builder().
    // withMultiProtocolNegotiate(true).
    // withSecurityProvider(new JceSecurityProvider(new BouncyCastleProvider())).
    // build();
    //
    // SMBClient client = new SMBClient(cfg);
    // Connection connection = client.connect(sambaIP);
    // com.hierynomus.smbj.session.Session session = connection.authenticate(new
    // AuthenticationContext(sambaUsername, sambaPass.toCharArray(), sambaDomain));
    // DiskShare share = (DiskShare) session.connectShare(sambaSharedPath);
    //
    // System.out.println("rc.soop.qlik.CopyAtlante.main() "+copy_SMB(share, "New
    // Text Document.txt", FileUtils.readFileToByteArray(new
    // File("C:\\Users\\Administrator\\Desktop\\New Text Document.txt")), null));
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    //
    // }
}
