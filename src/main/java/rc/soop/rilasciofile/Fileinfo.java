/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.rilasciofile;

/**
 *
 * @author srotella
 */
public class Fileinfo {

    String name, b_64;
    String hash;
    long size;
    
    String path;
    boolean isfile;
        
    public Fileinfo(String name, long size,String path,boolean isfile){
        this.name = name;
        this.path = path;
        this.size = size;
        this.isfile = isfile;
    }
    
    public Fileinfo(String name, String hash, long size, String b_64) {
        this.name = name;
        this.hash = hash;
        this.size = size;
        this.b_64 = b_64;
    }

    public String getName() {
        return name;
    }

    public String getHash() {
        return hash;
    }

    public long getSize() {
        return size;
    }

    public String getB_64() {
        return b_64;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isIsfile() {
        return isfile;
    }

    public void setIsfile(boolean isfile) {
        this.isfile = isfile;
    }
    
}
