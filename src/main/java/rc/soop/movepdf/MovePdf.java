/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.movepdf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import static rc.soop.movepdf.Config.log;
import static rc.soop.movepdf.Config.startfile;
import static rc.soop.start.Utility.rb;

/**
 *
 * @author rcosco
 */
public class MovePdf {

    private static void remove_NC_Transaction_DOC() {
        String pathout = rb.getString("pdf.path.out");        
        Db_Master db = new Db_Master();
        ArrayList<Doc> val = db.get_list_trNC_doc_MOVE();
        db.closeDB();
        log.warning("START MOVE PDF NOCHANGE...");
        AtomicInteger index = new AtomicInteger(0);
        val.forEach(doc1 -> {
            index.addAndGet(1);
            String cod = doc1.getCodice_documento();
            log.log(Level.INFO, "{0}) PROCESSING Document code: {1}", new Object[]{index.get(), cod});
            String data = StringUtils.replace(doc1.getData_load().substring(0, 10), "-", "");
            String content = StringUtils.replaceFirst(doc1.getContent(), "pdf;", "");
            File dir = new File(pathout + data);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File output = new File(pathout + data + File.separator + cod + ".pdf");
            try {
                FileUtils.writeByteArrayToFile(output, Base64.decodeBase64(content));
                if (Config.checkPDF(output)) {
                    Db_Master db1 = new Db_Master();
                    boolean es = db1.setContentDocNC(cod, startfile + output.getPath());
                    db1.closeDB();
                    if (es) {
                        log.log(Level.INFO, "{0}) Document result: OK", index.get());
                    } else {
                        log.log(Level.SEVERE, "{0}) Document result: KO UPDATE", index.get());
                        return;
                    }
                } else {
                    log.log(Level.SEVERE, "{0}) Document result: KO FILE OUT {1}", new Object[]{index.get(), output.getPath()});
                    return;
                }
            } catch (IOException ex) {
                log.log(Level.SEVERE, "{0}) Document result: ERROR {1}", new Object[]{index.get(), ex.getMessage()});
                return;
            }

        });
    }

    private static void restore_NC_Transaction_DOC() {
        Db_Master db = new Db_Master();
        ArrayList<Doc> val = db.get_list_nc_doc_RESTORE();
        db.closeDB();
        AtomicInteger index = new AtomicInteger(0);
        val.forEach(doc1 -> {
            index.addAndGet(1);
            String cod = doc1.getCodice_documento();
            String data = StringUtils.replace(doc1.getData_load().substring(0, 10), "-", "");
            String content = doc1.getContent();
            if (content.startsWith("FILE[")) {
                String pa1 = StringUtils.replace(content, "FILE[", "");
                File f = new File(pa1);
                try {
                    content = Base64.encodeBase64String(FileUtils.readFileToByteArray(f));
                } catch (IOException ex) {
                    log.log(Level.SEVERE, "{0}) Document restore: KO FILE OUT {1}", new Object[]{index.get(), f.getPath()});
                    return;
                }
                Db_Master db1 = new Db_Master();
                boolean es1 = db1.setContentDocNC(cod, content);
                db1.closeDB();
                if (es1) {
                    log.log(Level.INFO, "{0}) Document restore: OK", index.get());
                } else {
                    log.log(Level.SEVERE, "{0}) Document restore: KO UPDATE", index.get());
                    return;
                }
            }
        });
    }
    
    public static void remove_CH_Transaction_DOC() {
        String pathout = rb.getString("path.out");
        Db_Master db = new Db_Master();
        List<Doc> val = db.get_list_tr_doc_MOVE();
        db.closeDB();
        log.warning("START MOVE PDF...");
        AtomicInteger index = new AtomicInteger(0);
        val.forEach(doc1 -> {
            index.addAndGet(1);
            String data = StringUtils.replace(doc1.getData_load().substring(0, 10), "-", "");
            log.log(Level.INFO, "{0}) PROCESSING Document code: {1}", new Object[]{index.get(), doc1.getCodice_documento()});
            
            // Sanitize directory path
            File dir = sanitizePath(pathout + data);
            if (!dir.exists()) {
                dir.mkdirs();
            }
    
            // Sanitize file path
            File output = sanitizePath(pathout + data + File.separator + doc1.getCodice_documento() + doc1.getNomefile());
            try {
                FileUtils.writeByteArrayToFile(output, Base64.decodeBase64(doc1.getContent()));
                if (Config.checkPDF(output)) {
                    String path1 = StringUtils.replaceAll(output.getPath(), "\\\\", "/");
                    Db_Master db1 = new Db_Master();
                    boolean es = db1.setContentDoc(doc1.getCodice_documento(), startfile + path1);
                    db1.closeDB();
                    if (es) {
                        log.log(Level.INFO, "{0}) Document result: OK", index.get());
                    } else {
                        log.log(Level.SEVERE, "{0}) Document result: KO UPDATE", index.get());
                    }
                } else {
                    log.log(Level.SEVERE, "{0}) Document result: KO FILE OUT {1}", new Object[]{index.get(), output.getPath()});
                }
            } catch (IOException ex) {
                log.log(Level.SEVERE, "{0}) Document result: ERROR {1}", new Object[]{index.get(), ex.getMessage()});
            }
        });
        log.warning("END MOVE PDF");
    }
    
    private static File sanitizePath(String path) {
        try {
            // Normalize the path to remove any .. or other path traversal attempts
            File baseDir = new File(rb.getString("path.out")).getCanonicalFile();
            File file = new File(path).getCanonicalFile();
    
            // Ensure the file is within the base directory
            if (!file.getPath().startsWith(baseDir.getPath())) {
                throw new IllegalArgumentException("Invalid file path detected: " + path);
            }
            return file;
        } catch (IOException ex) {
            throw new RuntimeException("Path sanitization failed: " + ex.getMessage(), ex);
        }
    }

    private static void restore_CH_Transaction_DOC() {
        String pathout = rb.getString("path.out");
        Db_Master db = new Db_Master();
        ArrayList<String[]> val = db.get_list_tr_doc_RESTORE();
        db.closeDB();
        for (int x = 0; x < val.size(); x++) {
            String cod = val.get(x)[0];
            String data = StringUtils.replace(val.get(x)[5].substring(0, 10), "-", "");
            String content = val.get(x)[3];
            if (content.startsWith("FILE[")) {
                String pa1 = StringUtils.replace(content, "FILE[", "");
                File f = new File(pa1);
                try {
                    content = Base64.encodeBase64String(FileUtils.readFileToByteArray(f));
                } catch (IOException ex) {
                    ex.printStackTrace();
                    content = val.get(x)[3];
                }
            }

            File dir = new File(pathout + data);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File output = new File(pathout + data + File.separator + cod + val.get(x)[4]);
            try {
                FileUtils.writeByteArrayToFile(output, Base64.decodeBase64(content));
                if (Config.checkPDF(output)) {
                    Db_Master db1 = new Db_Master();
                    System.out.println(db1.setContentDoc(cod, startfile + output.getPath()));
                    db1.closeDB();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }
    }

//    public static void main(String[] args) {
//        remove_CH_Transaction_DOC();
////        remove_NC_Transaction_DOC();
////        restore_NC_Transaction_DOC();
//    }

}

class Doc {

    String codice_documento, data_load, content, nomefile;

    public Doc(String codice_documento, String data_load, String content) {
        this.codice_documento = codice_documento;
        this.data_load = data_load;
        this.content = content;
    }

    public Doc(String codice_documento, String data_load, String content, String nomefile) {
        this.codice_documento = codice_documento;
        this.data_load = data_load;
        this.content = content;
        this.nomefile = nomefile;
    }

    public String getCodice_documento() {
        return codice_documento;
    }

    public void setCodice_documento(String codice_documento) {
        this.codice_documento = codice_documento;
    }

    public String getData_load() {
        return data_load;
    }

    public void setData_load(String data_load) {
        this.data_load = data_load;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getNomefile() {
        return nomefile;
    }

    public void setNomefile(String nomefile) {
        this.nomefile = nomefile;
    }

}
