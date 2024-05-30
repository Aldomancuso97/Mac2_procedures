/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import rc.soop.qlik.LoggerNew;

import static rc.soop.esolver.ESolver.sanitizePath;
import static rc.soop.rilasciofile.Action.estraiNumber;
import static rc.soop.rilasciofile.Action.insertFile;
import static rc.soop.rilasciofile.Utility.getHASH;
import static rc.soop.rilasciofile.Utility.getStringBase64_IO;
import static rc.soop.rilasciofile.Utility.isFile;
import static rc.soop.rilasciofile.Utility.sanitizeInput;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.joda.time.DateTime;
import rc.soop.esolver.Util;
import rc.soop.rilasciofile.Fileinfo;

public class SftpMaccorp {

    public static final ResourceBundle rb = ResourceBundle.getBundle("sftp.conf");

    ///////////////////////////////////////////////////////////////
    // //SFTP CLIENTE - MERCURY
    private static final String MER = "MERCURY".toLowerCase();
    // //SFTP CLIENTE - BASSILICHI
    public static final String BAS = "BASSILICHI".toLowerCase();
    // SFTP - PROD
    public static final String se_user = rb.getString("ftp.user");
    public static final String se_pwd = rb.getString("ftp.pass");
    // SFTP - TEST
    // public static final String se_user = "macsftp_test";
    // public static final String se_pwd = "Dop77$$R.";

    public static final String se_ip = rb.getString("ftp.ip");
    public static final int se_port = Util.parseIntR(rb.getString("ftp.port"));
    ///////////////////////////////////////////////////////////////

    private ArrayList<Fileinfo> allfile_MER, allfile_BAS;
    private String download_sftp_MER, download_sftp_BAS, upload_mercury, upload_bassilichi, download_locale;
    public LoggerNew logger;

    public SftpMaccorp() {
        String dtnow = new DateTime().toString("yyyyMMdd");
        Db db = new Db(false);
        this.download_sftp_MER = db.getPath("download_sftp", "url");
        this.download_sftp_BAS = db.getPath("download_sftp_bass", "url");
        this.upload_mercury = db.getPath("upload_mercury", "url") + dtnow + "/";
        this.upload_bassilichi = db.getPath("upload_bassilichi", "url") + dtnow + "/";
        this.download_locale = db.getPath("download_locale", "url") + File.separator + dtnow + File.separator;
        this.allfile_MER = db.getFile(MER);
        this.allfile_BAS = db.getFile(BAS);
        this.logger = new LoggerNew("SFTP_MAC", db.getPath("Pathlog", "url"));
        db.closeDB();
    }

    public void sftpmercury() {
        // SFTP CLIENTE - MERCURY
        String MER_user = sanitizeInput(rb.getString("mer.prod.user"));
        String MER_pwd = sanitizeInput(rb.getString("mer.prod.pass"));
        String MER_ip = sanitizeInput(rb.getString("mer.prod.host"));
        String MER_port = sanitizeInput(rb.getString("mer.prod.port"));

        this.logger.log.log(Level.WARNING, "STARTING DOWNLOAD SFTP {0}", sanitizeInput(MER.toUpperCase()));
        // MERCURY
        ChannelSftp sftpmercury = SftpConnection.connect(
                MER_user,
                MER_pwd,
                MER_ip,
                Util.parseIntR(MER_port),
                this.logger);
        new File(sanitizeFilePath(this.download_locale + MER)).mkdirs();
        // Inizio download da SFTP MERCURY
        if (sftpmercury != null && sftpmercury.isConnected()) {
            System.out.println("rc.soop.sftp.SftpMaccorp.sftpmercury(CONNESSO)");
            try {
                ArrayList<LsEntry> v = new ArrayList<>(sftpmercury.ls(sanitizeInput(this.download_sftp_MER)));
                for (int i = 0; i < v.size(); i++) {
                    if (!v.get(i).getAttrs().isDir()) {
                        if (!isFile(sanitizeInput(v.get(i).getFilename()), this.allfile_MER)) {
                            long size = v.get(i).getAttrs().getSize();
                            if (size > 0) {
                                File download = new File(sanitizeFilePath(
                                        this.download_locale + MER + File.separator + v.get(i).getFilename()));
                                try (OutputStream outputStream = new BufferedOutputStream(
                                        new FileOutputStream(download))) {
                                    sftpmercury.get(sanitizeInput(this.download_sftp_MER + v.get(i).getFilename()),
                                            outputStream);
                                    outputStream.flush();
                                }
                                String hash = getHASH(download);
                                this.logger.log.log(Level.INFO, "{3} FILE SCARICATO: {0} SIZE:{1} HASH: {2}",
                                        new Object[] { sanitizeInput(v.get(i).getFilename()), size, hash,
                                                sanitizeInput(MER.toUpperCase()) });
                                insertFile(new Fileinfo(
                                        sanitizeInput(v.get(i).getFilename()),
                                        hash,
                                        size,
                                        getStringBase64_IO(download)), MER);
                            } else {
                                logger.log.log(Level.SEVERE, "ERRORE {0} : Il file {1} ha dimensione pari a 0.",
                                        new Object[] { sanitizeInput(MER.toUpperCase()),
                                                sanitizeInput(v.get(i).getFilename()) });
                            }
                        }
                    }
                }
                SftpConnection.closeConnection(sftpmercury, this.logger);
            } catch (Exception e) {
                this.logger.log.log(Level.SEVERE, "ERRORE {1} FILE: {0}",
                        new Object[] { sanitizeInput(e.getMessage()), sanitizeInput(MER.toUpperCase()) });
            }
        } else {
            this.logger.log.log(Level.SEVERE, "CLIENT {0} NON CONNESSO", sanitizeInput(MER.toUpperCase()));
        }
        this.logger.log.log(Level.WARNING, "END DOWNLOAD SFTP {0}", sanitizeInput(MER.toUpperCase()));
        File download_dir = new File(sanitizeFilePath(this.download_locale + MER));
        File[] ls = download_dir.listFiles();
        this.logger.log.log(Level.WARNING, "FILES DOWNLOADED: {0}", ls.length);
        if (ls.length > 0) {
            UPLOAD_AWS(ls, sanitizeInput(this.upload_mercury), sanitizeInput(MER));
        }
    }

    public String sanitizeInput(String input) {
        return input.replaceAll("[^a-zA-Z0-9]", "");
    }

    public String sanitizeFilePath(String filePath) {
        return filePath.replaceAll("\\.\\.", "").replaceAll("/", "").replaceAll("\\\\", "");
    }

    private static void listFiles(ArrayList<LsEntry> originale, ChannelSftp sftpbassilichi, String path,
            List<Fileinfo> listafinale) {

        try {
            ArrayList<LsEntry> v = new ArrayList<>(sftpbassilichi.ls(path));
            for (int x = 0; x < v.size(); x++) {
                LsEntry valoreinterno = v.get(x);
                if (valoreinterno.getAttrs().isDir()) {
                    if (valoreinterno.getFilename().equals(".") || valoreinterno.getFilename().equals("..")) {

                    } else {
                        listFiles(v, sftpbassilichi, path + "/" + valoreinterno.getFilename(), listafinale);
                    }
                } else {
                    listafinale.add(new Fileinfo(estraiNumber(path) + "_" + valoreinterno.getFilename(),
                            valoreinterno.getAttrs().getSize(), path + "/" + valoreinterno.getFilename(), true));
                }
            }

        } catch (Exception ex) {
        }
    }

    public void sftpbassilichinexi(boolean prod) {
        this.logger.log.log(Level.WARNING, "STARTING DOWNLOAD SFTP {0}", sanitizeInput(BAS.toUpperCase()));

        String BAS_ip = sanitizeInput(rb.getString("nexi.preprod.host"));
        String privateKey = sanitizeInput(rb.getString("nexi.preprod.key"));

        if (prod) {
            BAS_ip = sanitizeInput(rb.getString("nexi.prod.host"));// PROD
            privateKey = sanitizeInput(rb.getString("nexi.prod.key"));
        }

        ChannelSftp sftpnexi = SftpConnection.connect(
                sanitizeInput(rb.getString("nexi.prod.user")), // USER
                BAS_ip, // HOST
                Util.parseIntR(sanitizeInput(rb.getString("atm.preprod.port"))), // PORTA
                privateKey,
                this.logger);

        // Inizio download da SFTP BASSILICHI - NEXI
        if (sftpnexi != null && sftpnexi.isConnected()) {
            new File(sanitizeFilePath(this.download_locale + BAS)).mkdirs();
            this.logger.log.log(Level.WARNING, "SFTP CONNECTED: {0}", sanitizeInput(BAS_ip));

            try {
                sftpnexi.cd(sanitizeFilePath(this.download_sftp_BAS));
                ArrayList<LsEntry> v = new ArrayList<>(sftpnexi.ls(sftpnexi.pwd()));
                List<Fileinfo> listafinale = new ArrayList<>();
                String start = sftpnexi.pwd();

                for (int i = 0; i < v.size(); i++) {
                    if (!v.get(i).getFilename().equals(".") && !v.get(i).getFilename().equals("..")) {
                        listFiles(v, sftpnexi, start + "/" + v.get(i).getFilename(), listafinale);
                    }
                }

                for (int x = 0; x < listafinale.size(); x++) {
                    Fileinfo file1 = listafinale.get(x);
                    if (file1.isIsfile()) {
                        if (!isFile(file1.getName(), this.allfile_BAS)) {
                            if (file1.getSize() > 0) {
                                try {
                                    File download = new File(sanitizeFilePath(
                                            this.download_locale + BAS + File.separator + file1.getName()));
                                    try (OutputStream outputStream = new BufferedOutputStream(
                                            new FileOutputStream(download))) {
                                        sftpnexi.get(sanitizeInput(file1.getPath()), outputStream);
                                        outputStream.flush();
                                    }
                                    String hash = getHASH(download);
                                    this.logger.log.log(Level.INFO, "{3}: FILE SCARICATO: {0} - SIZE:{1} - HASH: {2}",
                                            new Object[] { sanitizeInput(file1.getName()), file1.getSize(), hash,
                                                    sanitizeInput(BAS.toUpperCase()) });
                                    insertFile(new Fileinfo(
                                            sanitizeInput(file1.getName()),
                                            hash,
                                            file1.getSize(),
                                            getStringBase64_IO(download)),
                                            BAS);
                                } catch (Exception e) {
                                    this.logger.log.log(Level.SEVERE, "ERRORE {1} FILE: {0}", new Object[] {
                                            sanitizeInput(e.getMessage()), sanitizeInput(BAS.toUpperCase()) });
                                }
                            } else {
                                logger.log.log(Level.SEVERE, "ERRORE {0} : Il file {1} ha dimensione pari a 0.",
                                        new Object[] { sanitizeInput(BAS.toUpperCase()),
                                                sanitizeInput(file1.getName()) });
                            }
                        }
                    }
                }
                SftpConnection.closeConnection(sftpnexi, this.logger);
            } catch (Exception e) {
                this.logger.log.log(Level.SEVERE, "ERRORE {1} SFTP LS: {0}",
                        new Object[] { sanitizeInput(e.getMessage()), sanitizeInput(BAS.toUpperCase()) });
            }
        } else {
            this.logger.log.log(Level.SEVERE, "CLIENT {0} NON CONNESSO", sanitizeInput(BAS.toUpperCase()));
        }
        this.logger.log.log(Level.WARNING, "END DOWNLOAD SFTP {0}", sanitizeInput(BAS.toUpperCase()));

        File download_dir = new File(sanitizeFilePath(download_locale + BAS));
        File[] ls = download_dir.listFiles();

        this.logger.log.log(Level.WARNING, "FILES DOWNLOADED: {0}", ls.length);

        if (ls.length > 0) {
            UPLOAD_AWS(ls, sanitizeInput(this.upload_bassilichi), sanitizeInput(BAS));
        }
    }

    public static boolean UPLOAD_AWS(File file, String meseriferimento, String annoriferimento, LoggerNew l1) {
        try {

            Db db1 = new Db(false);
            String pathdest = db1.getPath("upload", "url") + annoriferimento + "/" + meseriferimento + "/";
            db1.closeDB();

            FTPClient ftpClient = new FTPClient();
            ftpClient.connect(se_ip, se_port);
            ftpClient.login(se_user, se_pwd);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            if (ftpClient.isConnected()) {
                try {
                    ftpClient.makeDirectory(pathdest);
                    ftpClient.sendSiteCommand("chown -R 1003:1010 " + pathdest);
                } catch (Exception e) {
                }
                try {
                    try (InputStream is = new FileInputStream(file)) {
                        ftpClient.appendFile((pathdest + file.getName()), is);
                        ftpClient.sendSiteCommand("chown 1003:1010 " + pathdest + file.getName());
                        l1.log.log(Level.INFO, "{3}: FILE CARICATO: {0} - SIZE: {1} - HASH: {2}",
                                new Object[] { pathdest + file.getName(), file.length(), getHASH(file), "REPORT" });
                    }
                } catch (Exception e) {
                    l1.log.log(Level.SEVERE, "ERRORE FILE: {0}", e.getMessage());
                }
                ftpClient.disconnect();
            } else {
                l1.log.severe("FTP AWS NON CONNESSO");
            }
            return true;
        } catch (Exception e) {
            l1.log.log(Level.SEVERE, "ERRORE FILE: {0}", e.getMessage());
        }
        return false;

    }

    private boolean UPLOAD_AWS(File[] ls, String pathdest, String cl) {

        try {

            FTPClient ftpClient = new FTPClient();
            ftpClient.connect(se_ip, se_port);
            ftpClient.login(se_user, se_pwd);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            if (ftpClient.isConnected()) {
                try {
                    ftpClient.makeDirectory(pathdest);
                    ftpClient.sendSiteCommand("chown -R 1003:1010 " + pathdest);
                } catch (Exception e) {
                }
                for (File file : ls) {
                    try {
                        if (file.isFile()) {
                            try (InputStream is = new FileInputStream(file)) {
                                ftpClient.appendFile((pathdest + file.getName()), is);
                                ftpClient.sendSiteCommand("chown -R 1003:1010 " + pathdest + file.getName());
                                this.logger.log.log(Level.INFO, "{3}: FILE CARICATO: {0} - SIZE: {1} - HASH: {2}",
                                        new Object[] { pathdest + file.getName(), file.length(), getHASH(file),
                                                cl.toUpperCase() });
                            }
                        }
                    } catch (Exception e) {
                        this.logger.log.log(Level.SEVERE, "ERRORE FILE: {0}", e.getMessage());
                    }
                }
                ftpClient.disconnect();
            } else {
                this.logger.log.severe("FTP AWS NON CONNESSO");
            }
            return true;
        } catch (Exception e) {
            this.logger.log.log(Level.SEVERE, "ERRORE FILE: {0}", e.getMessage());
        }
        return false;
    }

    public static void main(String[] args) {
        new SftpMaccorp().sftpmercury();
    }
    // public void main() {
    //
    //// ChannelSftp sftpaws = SftpConnection.connect(se_user, se_pwd, se_ip,
    // se_port, this.logger);//inizio dell'upload dei file.
    //// sftpaws.disconnect();
    // }
    //
    // public static void main(String[] args) {
    // new SftpMaccorp().main();
    // }
}
