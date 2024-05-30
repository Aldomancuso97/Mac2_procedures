/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package rc.soop.gs;

import java.util.List;

/**
 *
 * @author raf
 */
public class Entity2022 {

    String username, password;
    List<Vendite2022> vendite;

    public Entity2022() {
    }

    public Entity2022(String username, String password, List<Vendite2022> vendite) {
        this.username = username;
        this.password = password;
        this.vendite = vendite;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<Vendite2022> getVendite() {
        return vendite;
    }

    public void setVendite(List<Vendite2022> vendite) {
        this.vendite = vendite;
    }

}
