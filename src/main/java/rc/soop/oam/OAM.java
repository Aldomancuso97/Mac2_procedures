/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.oam;

import rc.soop.aggiornamenti.oggettoFile;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import static rc.soop.oam.MacOAM.log;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author vcrugliano
 */
public class OAM {

    public static final String patternoam = "ddMMyyyy";
    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternnormdate_filter = "dd/MM/yyyy";

    public static String formatAL(String cod, ArrayList<String[]> array, int index, String defaultvalue) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i)[0])) {
                return (array.get(i))[index];
            }
        }
        return "-";
    }

    public static oggettoFile creaFile(int progressivo, String codicetrasm, String anno, String mese, String tipoinvio,
            String cfpi, String denom, String comune,
            String provincia, String tipologia, String controllo, ArrayList<Ch_transaction> list_tran,
            ArrayList<String[]> nations, ArrayList<Currency> curlist, ArrayList<Branch> allbr,
            ArrayList<String[]> tipodoc, String filename) {
        try {
            // Sanifica il percorso del file
            String sanitizedFilename = sanitizePath(filename);
            File f = new File(sanitizedFilename);
            PrintWriter writer = new PrintWriter(f);
            String esito_rigaA = recordA(codicetrasm, anno, mese, tipoinvio, cfpi, denom, comune, provincia, tipologia,
                    controllo, writer);
            if (esito_rigaA.equals("OK")) {
                Db_Master dbm = new Db_Master();
                for (int i = 0; i < list_tran.size(); i++) {
                    Ch_transaction tran = list_tran.get(i);
                    Client cl = dbm.query_Client_transaction(tran.getCod(), tran.getCl_cod());
                    ArrayList<Ch_transaction_value> valori = dbm.query_transaction_value(tran.getCod());
                    Branch b1 = get_Branch(tran.getFiliale(), allbr);
                    for (int x = 0; x < valori.size(); x++) {
                        String esito_recordb = recordB(tran, valori.get(x), cl, progressivo, nations, curlist, b1,
                                tipodoc, writer);
                        if (esito_recordb.equals("OK")) {
                            progressivo++;
                        } else {
                            writer.close();
                            f.delete();
                            return new oggettoFile(null, esito_recordb);
                        }
                    }
                }

                dbm.closeDB();
                String esitoZ = recordZ(progressivo, writer);
                if (esitoZ.equals("OK")) {
                    writer.close();
                    return new oggettoFile(f, "OK");
                } else {
                    writer.close();
                    f.delete();
                    return new oggettoFile(null, esitoZ);
                }
            } else {
                writer.close();
                f.delete();
                return new oggettoFile(null, esito_rigaA);
            }
        } catch (Exception ex) {
            log.severe(ex.getMessage());
            return new oggettoFile(null, ex.getMessage());
        }
    }

    private static String sanitizePath(String path) {
        return path.replaceAll("[^a-zA-Z0-9-_./]", "");
    }

    public static boolean controllaSize(String val, int size) {
        boolean es = val.trim().length() <= size || val.trim().length() == 0;
        if (!es) {
            log.log(Level.SEVERE, "macoam.OAM.controllaSize() {0} -- {1}", new Object[] { val, size });
        }
        return es;
    }

    public static String recordA(String codicetrasm, String anno, String mese, String tipoinvio, String cfpi,
            String denom, String comune, String provincia, String tipologia, String controllo, PrintWriter writer) {
        boolean ctrl = true;
        int[] sizetocheck = new int[9];
        sizetocheck[0] = 4;
        sizetocheck[1] = 2;
        sizetocheck[2] = 16;
        sizetocheck[3] = 70;
        sizetocheck[4] = 40;
        sizetocheck[5] = 2;
        String[] stringtocheck = new String[9];
        stringtocheck[0] = anno;
        stringtocheck[1] = mese;
        stringtocheck[2] = cfpi;
        stringtocheck[3] = denom;
        stringtocheck[4] = comune;
        stringtocheck[5] = provincia;

        int g = 0;
        while (g < 6 && ctrl) {
            ctrl = controllaSize(stringtocheck[g], sizetocheck[g]);
            g++;
        }
        if (g == 6 && ctrl) {
            while (cfpi.length() < 171) {
                cfpi = cfpi + " ";
            }
            while (denom.length() < 70) {
                denom = denom + " ";
            }
            while (comune.length() < 40) {
                comune = comune + " ";
            }
            mese = mese + tipoinvio;
            while (mese.length() <= 72) {
                mese = mese + " ";
            }
            while (provincia.length() <= 43) {
                provincia = provincia + " ";
            }
            while (tipologia.length() < 39) {
                tipologia = tipologia + " ";
            }
            writer.println("A" + codicetrasm + anno + mese + cfpi + denom + comune + provincia + tipologia + controllo);
            log.log(Level.INFO, "SCRIVO RECORD A: A{0}{1}{2}{3}{4}{5}{6}{7}{8}",
                    new Object[] { codicetrasm, anno, mese, cfpi, denom, comune, provincia, tipologia, controllo });
        } else {
            return "ERROR SIZE RIGA A";
        }
        return "OK";
    }

    public static String recordB(Ch_transaction tran, Ch_transaction_value val, Client cl,
            int progressivo, ArrayList<String[]> nations, ArrayList<Currency> curlist,
            Branch b1, ArrayList<String[]> tipodoc, PrintWriter writer) {
        boolean ctrl = true;
        int[] sizetocheck = new int[22];
        sizetocheck[0] = 30;
        sizetocheck[1] = 30;
        sizetocheck[2] = 1;
        sizetocheck[3] = 16;
        sizetocheck[4] = 8;
        sizetocheck[5] = 40;
        sizetocheck[6] = 2;
        sizetocheck[7] = 40;
        sizetocheck[8] = 3;
        sizetocheck[9] = 2;
        sizetocheck[10] = 20;
        sizetocheck[11] = 8;
        sizetocheck[12] = 40;
        sizetocheck[13] = 40;
        sizetocheck[14] = 8;
        sizetocheck[15] = 40;
        sizetocheck[16] = 1;
        sizetocheck[17] = 3;
        sizetocheck[18] = 14;
        sizetocheck[19] = 14;
        sizetocheck[20] = 14;
        sizetocheck[21] = 10;

        String[] stringtocheck = new String[22];
        try {

            stringtocheck[0] = StringUtils.substring(cl.getCognome().trim(), 0, 30);
            stringtocheck[1] = StringUtils.substring(cl.getNome().trim(), 0, 30);
            stringtocheck[2] = cl.getSesso().trim();

            stringtocheck[3] = cl.getCodfisc().replaceAll("---", "").trim();

            stringtocheck[4] = cl.getDt_nascita().trim().replaceAll("/", "").trim();
            stringtocheck[5] = cl.getCitta_nascita().trim();

            String pr = cl.getProvincia_nascita().trim();
            if (pr.trim().equals("") || pr.trim().equals("-") || pr.contains("-") || pr.contains("null")) {
                pr = "EE";
            }
            stringtocheck[6] = pr;

            stringtocheck[7] = formatAL(cl.getNazione(), nations, 1, "ITALIA").trim();
            stringtocheck[8] = cl.getNazione().trim();

            stringtocheck[9] = formatAL(cl.getTipo_documento(), tipodoc, 2, "CI").trim();

            stringtocheck[10] = cl.getNumero_documento().trim();
            stringtocheck[11] = cl.getDt_scadenza_documento().trim().replaceAll("/", "").trim();
            stringtocheck[12] = cl.getRilasciato_da_documento().trim();
            stringtocheck[13] = cl.getLuogo_rilascio_documento().trim();
            stringtocheck[14] = formatStringtoStringDate(tran.getData(), patternsqldate, patternoam).trim();

            stringtocheck[15] = "30121Venezia";
            stringtocheck[16] = tran.getTipotr().trim();
            stringtocheck[17] = val.getValuta().trim();
            stringtocheck[18] = val.getRate().trim();
            stringtocheck[19] = val.getQuantita().trim();
            stringtocheck[20] = val.getTotal().trim();

            stringtocheck[21] = tran.getId().trim().substring(tran.getId().length() - 10);

        } catch (Exception e) {
            return "ERROR ON CLIENT: " + cl.getCognome().trim() + " " + cl.getNome().trim();
        }
        int g = 0;
        while (g < 22 && ctrl) {
            ctrl = controllaSize(stringtocheck[g], sizetocheck[g]);
            g++;
        }

        if (g == 22 && ctrl) {
            int length = (int) Math.log10(progressivo) + 1;
            String progresscompl = "";
            while (length < 10) {
                progresscompl = progresscompl + "0";
                length++;
            }
            progresscompl = progresscompl + String.valueOf(progressivo);

            String cognome = cl.getCognome().trim();

            if (cognome.length() <= 30) {
                while (cognome.length() < 30) {
                    cognome = cognome + " ";
                }
            } else {
                cognome = cognome.substring(0, 30);
            }

            String nome = cl.getNome().trim();
            if (nome.length() <= 30) {
                while (nome.length() < 30) {
                    nome = nome + " ";
                }
            } else {
                nome = nome.substring(0, 30);
            }

            String sesso = cl.getSesso().trim();
            if (sesso.equals("M") || sesso.equals("F")) {

            } else {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String cfpiva = cl.getCodfisc().replaceAll("---", "").trim();

            if (cfpiva.length() <= 16) {
                while (cfpiva.length() < 16) {
                    cfpiva = cfpiva + " ";
                }
            } else {
                cfpiva = cfpiva.substring(0, 16);
            }

            String datan = StringUtils.replace(cl.getDt_nascita(), "/", "").trim();

            if (datan.length() != 8) {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String luogon = cl.getCitta_nascita().trim();
            if (luogon.length() <= 40) {
                while (luogon.length() < 40) {
                    luogon = luogon + " ";
                }
            } else {
                luogon = luogon.substring(0, 40);
            }

            String provn = cl.getProvincia_nascita().trim();
            if (provn.trim().equals("") || provn.trim().equals("-") || provn.trim().contains("-")
                    || provn.trim().contains("null")) {
                provn = "EE";
            }

            String paeseres = formatAL(cl.getNazione(), nations, 1, "ITALIA").trim();

            if (paeseres.length() == 1) {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            if (paeseres.length() <= 40) {
                while (paeseres.length() < 40) {
                    paeseres = paeseres + " ";
                }
            } else {
                paeseres = paeseres.substring(0, 40);
            }

            String siglapaeseres1 = cl.getNazione().trim();

            if (siglapaeseres1.length() != 3) {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String tipodocum = formatAL(cl.getTipo_documento(), tipodoc, 2, "CI").trim();

            if (tipodocum.length() != 2) {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String numdoc = cl.getNumero_documento().trim();
            if (numdoc.length() <= 20) {
                while (numdoc.length() < 20) {
                    numdoc = numdoc + " ";
                }
            } else {
                numdoc = numdoc.substring(0, 20);
            }

            String scaddoc = StringUtils.replace(cl.getDt_scadenza_documento(), "/", "").trim();

            String autrildoc = cl.getRilasciato_da_documento().trim();
            if (autrildoc.length() <= 40) {
                while (autrildoc.length() < 40) {
                    autrildoc = autrildoc + " ";
                }
            } else {
                autrildoc = autrildoc.substring(0, 40);
            }

            String luogorildoc = cl.getLuogo_rilascio_documento().trim();
            if (luogorildoc.length() <= 40) {
                while (luogorildoc.length() < 40) {
                    luogorildoc = luogorildoc + " ";
                }
            } else {
                luogorildoc = luogorildoc.substring(0, 40);
            }

            String dataop = formatStringtoStringDate(tran.getData(), patternsqldate, patternoam).trim();

            if (dataop.length() != patternoam.length()) {
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String luogop = b1.getAdd_cap().trim() + b1.getAdd_city().trim();
            if (luogop.length() <= 40) {
                while (luogop.length() < 40) {
                    luogop = luogop + " ";
                }
            } else {
                luogop = luogop.substring(0, 40);
            }

            String tipoop = tran.getTipotr().trim();
            if (tipoop.equalsIgnoreCase("S")) {
                tipoop = "1";
            } else {
                tipoop = "0";
            }

            if (val.getValuta().equals("---")) {
                return "ERROR ON TRANSACTION CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            String divest = getALCurrency(val.getValuta(), curlist).getUic().trim();

            String tasso = formatImporti(val.getRate(), 14, 4).trim();

            String inval = formatImporti(val.getQuantita(), 14, 2).trim();

            String outval = formatImporti(val.getNet(), 14, 2).trim();

            String idtrant = tran.getId().trim();
            if (idtrant.length() <= 10) {
                while (idtrant.length() < 10) {
                    idtrant = idtrant + "0";
                }
            } else {
                idtrant = idtrant.substring(idtrant.length() - 10);
            }
            int j = 0;
            while (j < 52) {
                idtrant = idtrant + " ";
                j++;
            }

            String print = "B" + progresscompl + cognome + nome + sesso + cfpiva + datan + luogon + provn + paeseres
                    + siglapaeseres1 + tipodocum
                    + numdoc + scaddoc + autrildoc + luogorildoc + dataop + luogop + tipoop
                    + divest + tasso + inval + outval + idtrant + "*";

            if (print.length() != 448 || errorformat(print)) {
                log.log(Level.SEVERE, "ERROR ON CLIENT: {0} {1}", new Object[] { cognome.trim(), nome.trim() });
                log.severe(print);
                return "ERROR ON CLIENT: " + cognome.trim() + " " + nome.trim();
            }

            writer.println(print);

        } else {
            return "ERROR SIZE RIGA B - INDICE " + g + " -- " + cl.getCognome();
        }
        return "OK";
    }

    public static boolean errorformat(String s1) {
        try {
            String s2 = new String(s1.getBytes(Charsets.ISO_8859_1), "UTF-8");
            return s1.length() != s2.length();
        } catch (Exception ex) {
            log.severe(ex.getMessage());
        }
        return true;
    }

    public static String formatImporti(String value, int max, int decimal) {
        Iterable<String> parameters = Splitter.on(".").split(value);
        Iterator<String> it = parameters.iterator();
        if (it.hasNext()) {
            String intero = it.next();
            String dec = "";
            if (it.hasNext()) {
                dec = it.next();
            }
            if (intero.length() < (max - decimal + 1)) {
                while (intero.length() < (max - decimal)) {
                    intero = "0" + intero;
                }
            }
            if (dec.length() > decimal) {
                dec = dec.substring(0, decimal);
            } else {
                while (dec.length() < (decimal)) {
                    dec = dec + "0";
                }
            }
            return intero + dec;
        }
        return null;
    }

    public static String recordZ(int progressivo, PrintWriter writer) {
        progressivo = progressivo - 1;
        String out = "Z";
        int i = 0;
        while (i < 200) {
            out = out + " ";
            i++;
        }
        int length = (int) Math.log10(progressivo) + 1;

        if (progressivo == 0) {
            length = 1;
        }
        String progresscompl = "";
        while (length < 11) {
            progresscompl = progresscompl + "0";
            length++;
        }

        progresscompl = progresscompl + String.valueOf(progressivo);
        out = out + progresscompl;
        int j = 0;
        while (j < 235) {
            out = out + " ";
            j++;
        }
        log.log(Level.INFO, "SCRIVO RECORD Z: {0}Z", out);
        writer.println(out + "Z");
        return "OK";
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
        } catch (Exception ex) {
            log.severe(dat + " -- " + ex.getMessage());
        }
        return dat;
    }

    public static Currency getALCurrency(String cod, ArrayList<Currency> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(((Currency) array.get(i)).getCode())) {
                return array.get(i);
            }
        }
        return null;
    }

    private static byte[] readBytesFromFileCod(File file) {
        try {
            byte[] bytes;
            try (InputStream is = new FileInputStream(file)) {
                long length = file.length();
                if (length > 2147483647L) {
                    throw new IOException("Size out: " + file.getName());
                }
                bytes = new byte[(int) length];
                int offset = 0;
                int numRead = 0;
                while ((offset < bytes.length) && ((numRead = is.read(bytes, offset, bytes.length - offset)) >= 0)) {
                    offset += numRead;
                }
                if (offset < bytes.length) {
                    throw new IOException("Error length " + file.getName());
                }
            }
            return bytes;
        } catch (FileNotFoundException localFileNotFoundException) {
        } catch (IOException localIOException) {
        }

        return null;
    }

    public static String getStringBase64(File file) {
        byte[] bytes = readBytesFromFileCod(file);
        if (bytes != null) {
            byte[] base64 = Base64.encodeBase64(bytes);
            return new String(base64);
        }
        return null;
    }

    public static Branch get_Branch(String cod, ArrayList<Branch> array_branch) {
        for (int j = 0; j < array_branch.size(); j++) {
            if (cod.equals((array_branch.get(j)).getCod())) {
                return (array_branch.get(j));
            }
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
        } catch (Exception ex) {
            log.severe(ex.getMessage());
            return false;
        }
    }

}