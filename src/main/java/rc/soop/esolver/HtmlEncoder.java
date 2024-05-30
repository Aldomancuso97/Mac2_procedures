package rc.soop.esolver;


import static rc.soop.esolver.Util.generaId;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.Base64;


public final class HtmlEncoder {

    public static <T extends Appendable> T escapeNonLatin(CharSequence sequence, T out)
            throws IOException {
        for (int i = 0; i < sequence.length(); i++) {
            char ch = sequence.charAt(i);
            if (Character.UnicodeBlock.of(ch) == Character.UnicodeBlock.BASIC_LATIN) {
                out.append(ch);
            } else {
                int codepoint = Character.codePointAt(sequence, i);

                i += Character.charCount(codepoint) - 1;

                out.append("&#x");
                out.append(Integer.toHexString(codepoint));
                out.append(";");
            }
        }
        return out;
    }

    public static String base64HTML(String path, String ing) {
        File f1 = null;
        try {
            f1 = new File(path + generaId(150) + ".html");
            try (FileOutputStream is = new FileOutputStream(f1);
                 OutputStreamWriter osw = new OutputStreamWriter(is);
                 BufferedWriter w = new BufferedWriter(osw)) {
                w.write(ing);
            }
    
            byte[] fileContent = Files.readAllBytes(f1.toPath());
            String base64 = Base64.getEncoder().encodeToString(fileContent);
            return base64;
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (f1 != null && f1.exists()) {
                f1.delete();
            }
        }
        return null;
    }
    

    public static String getBase64HTML(String base64) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64);
        String decodedString = new String(decodedBytes);
        return decodedString;
    }

}
