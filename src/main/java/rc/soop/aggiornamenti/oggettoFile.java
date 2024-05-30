/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.aggiornamenti;

import java.io.File;

/**
 *
 * @author Administrator
 */
public class oggettoFile {

    File file;
    String errore;

    public oggettoFile(File file, String errore) {
        this.file = file;
        this.errore = errore;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getErrore() {
        return errore;
    }

    public void setErrore(String errore) {
        this.errore = errore;
    }

}
