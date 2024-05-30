package rc.soop.esolver;

import java.util.ArrayList;

public class Till
{
  String cod;
  String name;
  boolean safe;
  boolean open;
  String fil_opcl;
  String id_opcl;
  String date_opcl;
  String cod_opcl;
  String us_opcl;
  String ty_opcl;
  String labeltools;
  String labelbutton;
  String classbutton;
  String classicon;
  
  public Till(String cod, String name, boolean safe, boolean open) {
    this.cod = cod;
    this.name = name;
    this.safe = safe;
    this.open = open;
    
    if (this.open) {
      this.labeltools = "Close this";
      this.labelbutton = "Open";
      this.classbutton = "green-jungle";
      this.classicon = "fa-check";
    } else {
      this.labeltools = "Open this";
      this.labelbutton = "Closed";
      this.classbutton = "red";
      this.classicon = "fa-remove";
    }
  }
  
  public Till(String fil_opcl, String id_opcl, String date_opcl, String cod_opcl, String us_opcl, String ty_opcl, String cod, String name, boolean safe)
  {
    this.fil_opcl = fil_opcl;
    this.id_opcl = id_opcl;
    this.date_opcl = date_opcl;
    this.cod_opcl = cod_opcl;
    this.us_opcl = us_opcl;
    this.ty_opcl = ty_opcl;
    this.cod = cod;
    this.name = name;
    if (this.ty_opcl.equals("O")) {
      this.ty_opcl = "OPEN";
      this.labeltools = "Close this";
      this.labelbutton = "Open";
      this.classbutton = "green-jungle";
      this.classicon = "fa-check";
    }
    if (this.ty_opcl.equals("C")) {
      this.ty_opcl = "CLOSE";
      this.labeltools = "Open this";
      this.labelbutton = "Closed";
      this.classbutton = "red";
      this.classicon = "fa-remove";
    }
    
    this.safe = safe;
  }
  
  public static boolean isSafeTill(ArrayList<Till> al, String cod)
  {
    for (int i = 0; i < al.size(); i++) {
      if (((Till)al.get(i)).getCod().equals(cod)) {
        return ((Till)al.get(i)).isSafe();
      }
    }
    return false;
  }
  
  public static String formatDescTill(ArrayList<Till> al, String cod) {
    for (int i = 0; i < al.size(); i++) {
      if (((Till)al.get(i)).getCod().equals(cod)) {
        return ((Till)al.get(i)).getName();
      }
    }
    return "-";
  }
  
  public Till() {}
  
  public String getLabeltools()
  {
    return this.labeltools;
  }
  
  public void setLabeltools(String labeltools) {
    this.labeltools = labeltools;
  }
  
  public String getLabelbutton() {
    return this.labelbutton;
  }
  
  public void setLabelbutton(String labelbutton) {
    this.labelbutton = labelbutton;
  }
  
  public String getClassbutton() {
    return this.classbutton;
  }
  
  public void setClassbutton(String classbutton) {
    this.classbutton = classbutton;
  }
  
  public String getClassicon() {
    return this.classicon;
  }
  
  public void setClassicon(String classicon) {
    this.classicon = classicon;
  }
  
  public String getFil_opcl() {
    return this.fil_opcl;
  }
  
  public void setFil_opcl(String fil_opcl) {
    this.fil_opcl = fil_opcl;
  }
  
  public String getId_opcl() {
    return this.id_opcl;
  }
  
  public void setId_opcl(String id_opcl) {
    this.id_opcl = id_opcl;
  }
  
  public String getDate_opcl() {
    return this.date_opcl;
  }
  
  public void setDate_opcl(String date_opcl) {
    this.date_opcl = date_opcl;
  }
  
  public String getCod_opcl() {
    return this.cod_opcl;
  }
  
  public void setCod_opcl(String cod_opcl) {
    this.cod_opcl = cod_opcl;
  }
  
  public String getUs_opcl() {
    return this.us_opcl;
  }
  
  public void setUs_opcl(String us_opcl) {
    this.us_opcl = us_opcl;
  }
  
  public String getTy_opcl() {
    return this.ty_opcl;
  }
  
  public void setTy_opcl(String ty_opcl) {
    this.ty_opcl = ty_opcl;
  }
  
  public String getCod() {
    return this.cod;
  }
  
  public void setCod(String cod) {
    this.cod = cod;
  }
  
  public String getName() {
    return this.name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public boolean isSafe() {
    return this.safe;
  }
  
  public void setSafe(boolean safe) {
    this.safe = safe;
  }
  
  public boolean isOpen() {
    return this.open;
  }
  
  public void setOpen(boolean open) {
    this.open = open;
  }
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\Till.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */