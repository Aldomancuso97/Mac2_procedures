package rc.soop.esolver;

import java.util.ArrayList;

public class Users {

    String filiale;
    String cod;
    String username;
    String pwd;
    String de_nome;
    String de_cognome;
    String dt_mod_pwd;
    String validita;
    String conto;
    String email;
    String fg_tipo;
    String fg_stato;
    String dt_insert;

    public String formatStatususer(String stato) {
        if (stato == null) {
            stato = "";
        }
        if (stato.equals("1")) {
            return "<div class='font-green-jungle'>Enabled <i class='fa fa-check'></i></div>";
        }
        if (stato.equals("0")) {
            return "<div class='font-red'>Disabled <i class='fa fa-close'></i></div>";
        }
        if (stato.equals("2")) {
            return "<div class='font-blue'>Waiting <i class='fa fa-hourglass-start'></i></div>";
        }
        return stato;
    }

    public String formatStatususerExcel(String stato) {
        if (stato == null) {
            stato = "";
        }
        if (stato.equals("1")) {
            stato = "Enabled";
        }
        if (stato.equals("0")) {
            stato = "Disabled";
        }
        if (stato.equals("2")) {
            stato = "Waiting";
        }
        return stato;
    }

    public String formatTypeuser(String tipol) {
        if (tipol == null) {
            tipol = "";
        }
        if (tipol.equals("0")) {
            return "<div class='font-green-jungle'>User normal <i class='fa fa-user'></i></div>";
        }
        if (tipol.equals("1")) {
            return "<div class='font-blue'>User advanced <i class='fa fa-user'></i></div>";
        }
        if (tipol.equals("2")) {
            return "<div class='font-green'>User plus <i class='fa fa-user'></i></div>";
        }
        if (tipol.equals("3")) {
            return "<div class='font-red'>Admin <i class='fa fa-user'></i></div>";
        }
        return "Error";
    }

    public String formatTypeuserExcel(String tipol) {
        if (tipol == null) {
            return "";
        }
        if (tipol.equals("0")) {
            return "User normal";
        }
        if (tipol.equals("1")) {
            return "User";
        }
        if (tipol.equals("2")) {
            return "User plus";
        }
        if (tipol.equals("3")) {
            return "Admin";
        }
        return "Error";
    }

    public static ArrayList<String[]> listStatususer() {
        ArrayList<String[]> out = new ArrayList<>();
        String[] s1 = {"0", "Disabled"};
        String[] s2 = {"1", "Enabled"};
        String[] s3 = {"2", "Waiting"};
        out.add(s1);
        out.add(s2);
        out.add(s3);
        return out;
    }

    public static ArrayList<String[]> listTypeuser() {
        ArrayList<String[]> out = new ArrayList<>();
        String[] s1 = {"0", "User normal"};
        String[] s2 = {"1", "User advanced"};
        String[] s3 = {"2", "User plus"};
        String[] s4 = {"3", "Admin"};
        out.add(s1);
        out.add(s2);
        out.add(s3);
        out.add(s4);
        return out;
    }

    public static String formatValidity(String cod) {
        if (cod.equals("0")) {
            return "Unlimited";
        } else if (cod.equals("1")||cod.equals("90")) {
            return "90 Days";
        }
        return cod;
    }

    public static ArrayList<String[]> listValidity() {
        ArrayList<String[]> out = new ArrayList<>();
        String[] s1 = {"0", "Unlimited"};
        String[] s2 = {"90", "90 Days"};
        out.add(s1);
        out.add(s2);
        return out;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getFiliale() {
        return this.filiale;
    }

    public void setFiliale(String filiale) {
        this.filiale = filiale;
    }

    public String getCod() {
        return this.cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getPwd() {
        return this.pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getDe_nome() {
        return this.de_nome;
    }

    public void setDe_nome(String de_nome) {
        this.de_nome = de_nome;
    }

    public String getDe_cognome() {
        return this.de_cognome;
    }

    public void setDe_cognome(String de_cognome) {
        this.de_cognome = de_cognome;
    }

    public String getDt_mod_pwd() {
        return this.dt_mod_pwd;
    }

    public void setDt_mod_pwd(String dt_mod_pwd) {
        this.dt_mod_pwd = dt_mod_pwd;
    }

    public String getValidita() {
        return this.validita;
    }

    public void setValidita(String validita) {
        this.validita = validita;
    }

    public String getConto() {
        return this.conto;
    }

    public void setConto(String conto) {
        this.conto = conto;
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFg_tipo() {
        return this.fg_tipo;
    }

    public void setFg_tipo(String fg_tipo) {
        this.fg_tipo = fg_tipo;
    }

    public String getFg_stato() {
        return this.fg_stato;
    }

    public void setFg_stato(String fg_stato) {
        this.fg_stato = fg_stato;
    }

    public String getDt_insert() {
        return this.dt_insert;
    }

    public void setDt_insert(String dt_insert) {
        this.dt_insert = dt_insert;
    }
}


/* Location:              C:\Users\rcosco\Desktop\classes\!\entity\Users.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       0.7.1
 */
