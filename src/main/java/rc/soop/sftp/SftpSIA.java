/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import rc.soop.qlik.LoggerNew;
import static rc.soop.rilasciofile.Action.insertFile;
import static rc.soop.rilasciofile.Utility.getHASH;
import static rc.soop.rilasciofile.Utility.getStringBase64_IO;
import static rc.soop.rilasciofile.Utility.isFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.logging.Level;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.joda.time.DateTime;
import rc.soop.esolver.Util;
import rc.soop.rilasciofile.Fileinfo;
import static rc.soop.sftp.SftpMaccorp.rb;
import static rc.soop.sftp.SftpMaccorp.se_ip;
import static rc.soop.sftp.SftpMaccorp.se_port;
import static rc.soop.sftp.SftpMaccorp.se_pwd;
import static rc.soop.sftp.SftpMaccorp.se_user;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author rcosco
 */
public class SftpSIA {

    // SFTP CLIENTE - SIA
    private static final String SIA = "SIA".toLowerCase();
    private static final String SIA_user = rb.getString("sia.user");

    private ArrayList<Fileinfo> allfile_SIA;
    private String download_sftp_SIA, upload_SIA, download_locale, Pathlog;
    public LoggerNew logger;

    public SftpSIA() {
        String dtnow = new DateTime().toString("yyyyMMdd");
        Db db = new Db(false);
        this.download_sftp_SIA = db.getPath("download_sftp_sia", "url");
        this.upload_SIA = db.getPath("upload_sia", "url") + dtnow + "/";
        this.download_locale = db.getPath("download_locale", "url") + File.separator + dtnow + File.separator;
        this.allfile_SIA = db.getFile(SIA);
        this.Pathlog = db.getPath("Pathlog", "url");
        db.closeDB();
    }

    public void sftpsia(boolean prod) {
        this.Pathlog = rb.getString("sia.path.log");
        this.logger = new LoggerNew("SFTP_SIA", this.Pathlog);
        this.logger.log.log(Level.WARNING, "STARTING DOWNLOAD SFTP {0}", SIA.toUpperCase());

        String SIA_ip = rb.getString("sia.preprod.host");
        String privateKey = rb.getString("sia.preprod.key");
        String SIA_port = rb.getString("sia.preprod.port");
        if (prod) {
            SIA_ip = rb.getString("sia.prod.host"); // PROD1
            privateKey = rb.getString("sia.prod.key");
            SIA_port = rb.getString("sia.prod.port");
        }
        ChannelSftp sftpsia = SftpConnection.connect(
                SIA_user,
                SIA_ip,
                Util.parseIntR(SIA_port),
                privateKey,
                this.logger);
        if (prod) {
            if (sftpsia == null || !sftpsia.isConnected()) {
                SIA_ip = rb.getString("sia.prod.host2");// PROD2
                sftpsia = SftpConnection.connect(
                        SIA_user,
                        SIA_ip,
                        Util.parseIntR(SIA_port),
                        privateKey,
                        this.logger);
            }
        }
        this.download_locale = rb.getString("sia.path.out") + File.separator + new DateTime().toString("yyyyMMdd")
                + File.separator; // SIA
        new File(this.download_locale).mkdirs();
        if (sftpsia != null && sftpsia.isConnected()) {
            this.logger.log.log(Level.WARNING, "SFTP CONNECTED: {0}", SIA_ip);
            try {
                sftpsia.cd(this.download_sftp_SIA);
                ArrayList<LsEntry> v = new ArrayList<>(sftpsia.ls(this.download_sftp_SIA));

                for (int i = 0; i < v.size(); i++) {
                    if (!v.get(i).getAttrs().isDir()) {
                        String filename = v.get(i).getFilename();
                        if (!isFile(filename, this.allfile_SIA)) {// se il file non esiste lo scarica
                            long size = v.get(i).getAttrs().getSize();
                            if (size > 0) {
                                try {
                                    // Sanitize and validate filename
                                    filename = sanitizeFilename(filename);
                                    Path downloadPath = Paths.get(this.download_locale, filename).normalize();
                                    // Ensure the path lies within the download directory
                                    if (!downloadPath.startsWith(this.download_locale)) {
                                        throw new SecurityException("Invalid file path");
                                    }
                                    File download = downloadPath.toFile();
                                    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(download))) {
                                        this.logger.log.log(Level.INFO, "GET: {0}", new Object[] { filename });
                                        sftpsia.get(filename, out);
                                        out.flush();
                                    }
                                    String hash = getHASH(download);
                                    if (download.length() > 0) {
                                        this.logger.log.log(Level.INFO,
                                                "{3}: FILE SCARICATO: {0} - SIZE:{1} - HASH: {2}",
                                                new Object[] { filename, size, hash, SIA.toUpperCase() });
                                        insertFile(new Fileinfo(
                                                filename,
                                                hash,
                                                size,
                                                getStringBase64_IO(download)), SIA);
                                    } else {
                                        download.delete();
                                    }
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    this.logger.log.log(Level.SEVERE, "ERRORE {1} FILE: {0}",
                                            new Object[] { e1.getMessage(), SIA.toUpperCase() + " - " + filename });
                                }
                            } else {
                                logger.log.log(Level.SEVERE, "ERRORE {0} : Il file {1} ha dimensione pari a 0.",
                                        new Object[] { SIA.toUpperCase(), filename });
                            }
                        }
                    }
                }
                SftpConnection.closeConnection(sftpsia, this.logger);
            } catch (Exception e) {
                this.logger.log.log(Level.SEVERE, "ERRORE {1} SFTP LS: {0}",
                        new Object[] { e.getMessage(), SIA.toUpperCase() });
            }
        }

        File download_dir = new File(this.download_locale);
        File[] ls = download_dir.listFiles();
        this.logger.log.log(Level.WARNING, "FILES DOWNLOADED: {0}", ls.length);

        if (ls.length > 0) {
            UPLOAD_AWS(ls, this.upload_SIA, SIA);
        }
    }

    private String sanitizeFilename(String filename) {
        filename = filename.replaceAll("[/\\\\]", "");
        if (!filename.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid filename");
        }
        return filename;
    }

    public void rilasciaFIle(File fileupload, boolean prod) {
        this.logger = new LoggerNew("SFTP_SIA", this.Pathlog);
        this.logger.log.log(Level.WARNING, "STARTING UPLOAD ON DEMAND SFTP {0}", SIA.toUpperCase());

        String SIA_ip = rb.getString("sia.preprod.host");
        String privateKey = rb.getString("sia.preprod.key");
        String SIA_port = rb.getString("sia.preprod.port");
        if (prod) {
            SIA_ip = rb.getString("sia.prod.host"); // PROD1
            privateKey = rb.getString("sia.prod.key");
            SIA_port = rb.getString("sia.prod.port");
        }

        ChannelSftp sftpsia = SftpConnection.connect(
                SIA_user,
                SIA_ip,
                Util.parseIntR(SIA_port),
                privateKey,
                this.logger);

        if (prod) {
            if (sftpsia == null || !sftpsia.isConnected()) {
                SIA_ip = rb.getString("sia.prod.host2");// PROD2
                sftpsia = SftpConnection.connect(
                        SIA_user,
                        SIA_ip,
                        Util.parseIntR(SIA_port),
                        privateKey,
                        this.logger);
            }
        }

        if (sftpsia != null && sftpsia.isConnected()) {

            this.logger.log.log(Level.WARNING, "SFTP CONNECTED: {0}", SIA_ip);
            try {
                sftpsia.cd("/input/");
                try (InputStream is = new FileInputStream(fileupload)) {
                    sftpsia.put(is, fileupload.getName());
                }
            } catch (Exception ex1) {
                this.logger.log.log(Level.SEVERE, "ERROR: {0}", ex1.getMessage());
            }
            SftpConnection.closeConnection(sftpsia, this.logger);
        } else {
            this.logger.log.severe("SFTP SETA NON CONNESSO");
        }

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
                } catch (Exception e) {
                }
                for (File file : ls) {
                    try {
                        if (file.isFile()) {
                            try (InputStream is = new FileInputStream(file)) {
                                ftpClient.appendFile((pathdest + file.getName()), is);
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
}
