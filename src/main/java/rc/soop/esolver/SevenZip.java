/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.esolver;

import static rc.soop.esolver.ESolver.sanitizeFilePath;
import static rc.soop.esolver.Util.sanitizeFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;

/**
 *
 * @author rcosco
 */
public class SevenZip {

    public static void compress(List<File> files, String pathtemp, String name) throws IOException {
        try (SevenZOutputFile out = new SevenZOutputFile(new File(sanitizeFilePath(pathtemp) + name))) {
            for (File file : files) {
                addToArchiveCompression(out, sanitizeFile(file));
            }
        }
    }

    private static void addToArchiveCompression(SevenZOutputFile out, File file) throws IOException {
        String name = file.getName();
        if (file.isFile()) {
            SevenZArchiveEntry entry = out.createArchiveEntry(file, name);
            out.putArchiveEntry(entry);
    
            try (FileInputStream in = new FileInputStream(file)) {
                byte[] b = new byte[1024];
                int count = 0;
                while ((count = in.read(b)) > 0) {
                    out.write(b, 0, count);
                }
            }
            out.closeArchiveEntry();
    
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addToArchiveCompression(out, child);
                }
            }
        } else {
            System.out.println(file.getName() + " is not supported");
        }
    }
    

}
