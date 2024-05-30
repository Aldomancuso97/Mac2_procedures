/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.cora;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import static rc.soop.cora.Db_Master.fd;
import static rc.soop.cora.MacCORA.log;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import rc.soop.aggiornamenti.oggettoFile;

/**
 *
 * @author rcosco
 */
public class CORA {

    public static final String patternoam = "ddMMyyyy";
    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternnormdate_filter = "dd/MM/yyyy";
    public static final String pattermonthnorm = "MM/yyyy";
    public static final String patterntsdatecora = "yyyyMMddHHmmss";
    public static final String patternmonthsql = "yyyy-MM";

    public static String estraicodicecognomenome(String cognome, String nome) {
        String c1 = StringUtils.deleteWhitespace(cognome.trim()).toUpperCase();
        String n1 = StringUtils.deleteWhitespace(nome.trim()).toUpperCase();
        String out = c1 + n1;
        return StringUtils.substring(out, 0, 12);
    }

    private static final String space = " ";
    private static final String blank = "";
    private static final String zero = "0";
    private static final String one = "1";

    public static File annuale_TXT(String anno) {
        try {
            Db_Master db = new Db_Master();
            List<Codici> out = db.getCORA(anno);
            // List<Codici> out = db.getCORA_REC();

            String path = db.getPath("temp");

            path = sanitizePath(path);
            db.closeDB();
            String filename = path + "MACCORP_SALDI_" + anno + "_" + generaId(20) + ".TXT";
            System.out.println("maccora.CORA.annuale_TXT() " + filename);
            File txt = new File(filename);
            PrintWriter writer = new PrintWriter(txt);

            // TESTATA
            StringBuilder testata = new StringBuilder();
            testata.append(zero);
            testata.append("ARU00");
            testata.append(one);
            testata.append("3");
            testata.append(anno);
            testata.append("00");
            testata.append(StringUtils.rightPad(blank, 24, space));
            testata.append(StringUtils.rightPad("12951210157", 16, space));
            testata.append(StringUtils.rightPad(blank, 26, space));
            testata.append(StringUtils.rightPad(blank, 25, space));
            testata.append(StringUtils.rightPad(blank, 1, space));
            testata.append(StringUtils.rightPad(blank, 8, space));
            testata.append(StringUtils.rightPad(blank, 40, space));
            testata.append(StringUtils.rightPad(blank, 2, space));
            testata.append(StringUtils.rightPad("MACCORP ITALIANA SPA", 70, space));
            testata.append(StringUtils.rightPad("MILANO", 40, space));
            testata.append("MI");
            testata.append(StringUtils.rightPad(blank, 16, space));
            testata.append(zero);
            testata.append(StringUtils.rightPad(blank, 112, space));
            testata.append("A");
            writer.println(testata.toString());
            Db_Master db1 = new Db_Master();
            out.forEach(coracode -> {
                Transaction tra = db1.getTransaction(coracode.getCodicecliente());
                if (tra != null) {
                    StringBuilder record = new StringBuilder();
                    record.append("3");
                    record.append(StringUtils.rightPad(coracode.getCodice(), 50, space));
                    record.append("98");
                    record.append(anno);
                    record.append(StringUtils.rightPad(blank, 18, zero));
                    record.append(StringUtils.rightPad(blank, 18, zero));

                    double imp3 = fd(tra.getImporto3().split("\\.")[0]);
                    record.append(StringUtils.leftPad(tra.getImporto3().split("\\.")[0], 17, zero));
                    if (imp3 <= 1000000.00) {
                        record.append(zero);
                    } else {
                        record.append(one);
                    }
                    record.append(StringUtils.rightPad(blank, 18, zero));
                    double altreinfo = fd(tra.getAltre_info());
                    record.append(StringUtils.leftPad(tra.getAltre_info(), 17, zero));
                    if (altreinfo <= 1000000.00) {
                        record.append(zero);
                    } else {
                        record.append(one);
                    }
                    record.append(StringUtils.rightPad(blank, 18, zero));

                    record.append("242");
                    record.append(StringUtils.rightPad(blank, 229, space));
                    record.append("A");
                    writer.println(record.toString());
                }
            });
            db1.closeDB();

            //
            StringBuilder coda = new StringBuilder();
            coda.append("9");
            coda.append(StringUtils.leftPad(blank, 9, zero));
            coda.append(StringUtils.leftPad(blank, 9, zero));
            coda.append(StringUtils.leftPad(String.valueOf(out.size()), 9, zero));
            coda.append(StringUtils.leftPad(blank, 9, zero));
            coda.append(StringUtils.leftPad(blank, 360, space));
            coda.append("A");
            writer.println(coda.toString());
            writer.close();
            return txt;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    // public static void main(String[] args) {
    // annuale_TXT("2018");
    //
    //// mensile("", zero, one, one, zero, zero)
    // }
    // public static File annuale(String anno) {
    //
    // Db_Master db = new Db_Master();
    // List<Codici> out = db.getCORA(anno);
    //// List<Codici> out = db.getCORA(anno);
    // String path = db.getPath("temp");
    // ArrayList<String[]> city = db.city_Italy_APM();
    // ArrayList<String[]> country = db.country();
    // db.closeDB();
    //
    // String pathfin = path + anno + "_" + generaId(20) + "_A_Cora.csv";
    // System.out.println(pathfin + "maccora.CORA.annuale() " + out.size());
    //
    // String intestazione1 = "Identificativo;RAPPORTO;;;ANAGRAFICHE COLLEGATE AL
    // RAPPORTO;;;;;;;;;SALDI DA COMUNICARE (Vedi TabellaSaldi);;;;;;;;;TERNA
    // IDENTIFICATIVA RAPPORTO;;;";
    // String intestazione2 = "CODICE UNIVOCO;Cod;Data Inizio;Data
    // Fine;RUOLO;Cognome o Ragione Sociale;Nome;Codice Fiscale;Data
    // Nascita;Localita';PR;Sesso;Note;Importo1;Importo2;Importo3;Importo4;Altre
    // Inform.;VALUTA;Giacenza media;Descrizione per Rapporto Tipo 99 (obbligatoria
    // e MAX 24 Caratteri);CAB;Soggetto Obbligato;Numero File;Numero record nel
    // file;NUOVO CODICE UNIVOCO";
    // StringBuilder build = new StringBuilder();
    // try {
    // Db_Master db1 = new Db_Master();
    //
    // File txt = new File(pathfin);
    // FileWriter fw = new FileWriter(txt);
    // out.forEach(coracode -> {
    // String client_code = coracode.getCodicecliente();
    // Client cl1 = db1.query_Client_transaction(null, client_code);
    // String dtnascita;
    // if (cl1.getDt_nascita().contains("-")) {
    // dtnascita = formatStringtoStringDate(cl1.getDt_nascita(), patternsql,
    // patternnormdate_filter);
    // } else {
    // dtnascita = cl1.getDt_nascita();
    // }
    // String loc = formatAL(cl1.getCitta(), city, 1);
    // if (loc.equals("-")) {
    // loc = formatAL(cl1.getNazione_nascita(), country, 1);
    // }
    // String pr = cl1.getProvincia();
    // if (pr.trim().equals("") || pr.trim().equals("-")) {
    // pr = "EE";
    // }
    // String c1 =
    // StringUtils.removePattern(StringUtils.stripAccents(cl1.getCognome()),
    // "[^A-Za-z0-9]");
    // String n1 =
    // StringUtils.removePattern(StringUtils.stripAccents(cl1.getNome()),
    // "[^A-Za-z0-9]");
    // Transaction tra = db1.getTransaction(client_code);
    //
    // build.append(coracode.getCodice()).append(";98;").append(formatStringtoStringDate(tra.getData_inizio(),
    // patternsqldate,
    // patternnormdate_filter)).append(";;0;").append(c1.toUpperCase()).append(";").append(n1.toUpperCase()).append(";").append(cl1.getCodfisc().replaceAll("---",
    // "")).append(";").append(dtnascita).append(";").append(loc).append(";").append(pr).append(";").append(cl1.getSesso()).append(";;;;").append(tra.getImporto3()).append(";;").append(tra.getAltre_info()).append(";;;;;12951210157;;;\r\n");
    // });
    // db1.closeDB();
    //
    // fw.write(intestazione1 + "\r\n" + intestazione2 + "\r\n" + build.toString());
    // fw.flush();
    // fw.close();
    // return txt;
    // } catch (IOException e) {
    // log.severe(e.getMessage());
    // }
    // return null;
    // }
    public static oggettoFile mensile(String path, String from, String f1, String f2, String anno, String mese) {
        try {
            Db_Master db = new Db_Master();
            // Sanifica il percorso del file
            String sanitizedPath = sanitizePath(path);
            String datafile = db.getNowDT().toString("yyyyMMdd");
            ArrayList<Client> rs = db.getTransazioni(from, f1, f2);
            ArrayList<String[]> city = db.city_Italy_APM();
            ArrayList<String[]> country = db.country();
            List<Object_DB> prov = db.district();

            log.log(Level.INFO, "NUMERO TRANSAZIONI: {0}", rs.size());
            ArrayList<String> presenti = new ArrayList<>();

            String pathfin = sanitizedPath + from + "_" + generaId(20) + "_M_Cora.csv";
            String intestazione1 = "Identificativo;RAPPORTO;;;ANAGRAFICHE COLLEGATE AL RAPPORTO;;;;;;;;;SALDI DA COMUNICARE (Vedi TabellaSaldi);;;;;;;;;TERNA IDENTIFICATIVA RAPPORTO;;;";
            String intestazione2 = "CODICE UNIVOCO;Cod;Data Inizio;Data Fine;RUOLO;Cognome o Ragione Sociale;Nome;Codice Fiscale;Data Nascita;Località;PR;Sesso;Note;Importo1;Importo2;Importo3;Importo4;Altre Inform.;VALUTA;Giacenza media;Descrizione per Rapporto Tipo 99 (obbligatoria e MAX 24 Caratteri);CAB;Soggetto Obbligato;Numero File;Numero record nel file;NUOVO CODICE UNIVOCO";
            String cor = "";
            File txt = new File(pathfin);

            try (FileWriter fw = new FileWriter(txt)) {
                for (int i = 0; i < rs.size(); i++) {
                    Client cl = rs.get(i);
                    String dtnascita;
                    if (cl.getDt_nascita().contains("-")) {
                        dtnascita = formatStringtoStringDate(cl.getDt_nascita(), patternsql, patternnormdate_filter);
                    } else {
                        dtnascita = cl.getDt_nascita();
                    }
                    String loc = formatAL(cl.getCitta(), city, 1);
                    if (loc.equals("-")) {
                        loc = formatAL(cl.getNazione_nascita(), country, 1);
                    }

                    String pr = cl.getProvincia();
                    if (pr.trim().equals("") || pr.trim().equals("-")) {
                        pr = "EE";
                    } else {
                        if (pr.length() > 2) {
                            String sb1 = pr;
                            pr = prov.stream().filter(br -> br.getDescr().equalsIgnoreCase(sb1)).distinct().findFirst()
                                    .orElse(new Object_DB(sb1, sb1)).getCod();
                        }
                    }
                    String c1 = RegExUtils.removePattern(StringUtils.stripAccents(cl.getCognome()), "[^A-Za-z0-9]");
                    String n1 = RegExUtils.removePattern(StringUtils.stripAccents(cl.getNome()), "[^A-Za-z0-9]");

                    String coracode = db.generacodiceCORA(cl.getCode().toUpperCase(), anno, mese, datafile, c1, n1);

                    if (!presenti.contains(coracode)) {
                        presenti.add(coracode);
                        cor = cor + coracode + ";98;" + cl.getDatatr() + ";;0;"
                                + c1.toUpperCase() + ";" + n1.toUpperCase() + ";"
                                + cl.getCodfisc().replaceAll("---", "") + ";"
                                + dtnascita + ";"
                                + loc + ";" + pr + ";" + cl.getSesso()
                                + ";;;;;;;;;;;;;;\r\n";
                        log.log(Level.INFO, " INDICE: {0}) INSERIMENTO CODICE: {1}",
                                new Object[] { presenti.size(), coracode });
                    }
                }
                fw.write(intestazione1 + "\r\n" + intestazione2 + "\r\n" + cor);
                fw.flush();
            }

            db.closeDB();

            return new oggettoFile(txt, "OK");
        } catch (IOException ex) {
            log.severe(ex.getMessage());
            return new oggettoFile(null, ex.getMessage());
        }
    }

    private static String sanitizePath(String path) {
        return path.replaceAll("[^a-zA-Z0-9-_./]", "");
    }

    public static String getStringBase64(File file) {
        byte[] bytes = readBytesFromFileCod(file);
        if (bytes != null) {
            byte[] base64 = Base64.encodeBase64(bytes);
            return new String(base64);
        }
        return null;
    }

    private static byte[] readBytesFromFileCod(File file) {
        try {

            long length = file.length();
            if (length > 2147483647L) {
                return new byte[0];
            }
            byte[] bytes = new byte[(int) length];
            int offset = 0;
            int numRead = 0;
            try (InputStream is = new FileInputStream(file)) {
                while ((offset < bytes.length) && ((numRead = is.read(bytes, offset, bytes.length - offset)) >= 0)) {
                    offset += numRead;
                }
                if (offset < bytes.length) {
                    return new byte[0];
                }
            }
            return bytes;
        } catch (Exception localIOException) {
            return new byte[0];
        }

    }

    public static String generaId() {
        String random = RandomStringUtils.randomAlphanumeric(5).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
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
        } catch (IllegalArgumentException e) {
            log.severe(e.getMessage());
        }
        return dat;
    }

    public static String formatAL(String cod, ArrayList<String[]> array, int index) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((String[]) array.get(i))[0])) {
                return ((String[]) array.get(i))[index];
            }
        }
        return "-";
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
        } catch (IllegalArgumentException e) {
            log.severe(e.getMessage());
        }
        return null;
    }

    public static boolean zipListFiles(List<File> files, File targetZipFile) {
        try {
            try (OutputStream out = new FileOutputStream(targetZipFile);
                    ArchiveOutputStream os = new ArchiveStreamFactory().createArchiveOutputStream("zip", out)) {
                for (int i = 0; i < files.size(); i++) {
                    File ing = files.get(i);
                    os.putArchiveEntry(new ZipArchiveEntry(ing.getName()));
                    try (FileInputStream in = new FileInputStream(ing)) {
                        IOUtils.copy(in, os);
                    }
                    os.closeArchiveEntry();
                }
            }
            return targetZipFile.length() > 0;
        } catch (ArchiveException | IOException ex) {
            log.severe(ex.getMessage());
            return false;
        }
    }

}