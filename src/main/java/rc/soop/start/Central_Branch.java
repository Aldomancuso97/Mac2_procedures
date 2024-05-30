/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.start;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import rc.soop.maintenance.BreakException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import static rc.soop.start.Utility.formatStringtoStringDate;
import static rc.soop.start.Utility.formatType;
import static rc.soop.start.Utility.getTime;
import static rc.soop.start.Utility.pattern1;
import static rc.soop.start.Utility.patternsqldate;
import static rc.soop.start.Utility.rb;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author rcosco
 */
public class Central_Branch {

    public static final String user_h = rb.getString("user_h");
    public static final String pwd_h = rb.getString("pwd_h");
    public static final String host_h = rb.getString("host_h");
    public static final String user_f = rb.getString("user_f");
    public static final String pwd_f = rb.getString("pwd_f");

    String filiale;

    public Central_Branch(String filiale) {
        this.filiale = filiale;
    }

    private static String formatDateStart(String dateing, Logger log) {
        if (dateing.length() == 16) {
            dateing = dateing + ":00";
        }

        if (dateing.contains("-")) {
            dateing = formatStringtoStringDate(dateing, patternsqldate, pattern1, log);
        }

        return dateing;
    }

    public boolean updateToBranch(Logger log) {

        DateTime adesso = getTime(log);
        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        if (db.getConnectionDB() == null) {
            log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
            return true;
        }
        String myip = db.getIpFiliale(filiale);
        ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod(filiale, "0");
        db.closeDB();

        //nuovo
        DBFiliale dbfiliale1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);

        if (dbfiliale1.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
            return true;
        }

        ArrayList<Aggiornamenti_mod> li_locali = dbfiliale1.list_aggiornamenti_mod_div_limit_local(filiale, "0");

        dbfiliale1.closeDB();

        log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} IN LOCALE SULLA FILIALE {1}", new Object[]{li_locali.size(), filiale});
        AtomicInteger errore = new AtomicInteger(0);

        try {

            AtomicInteger index = new AtomicInteger(1);
            li_locali.forEach(st -> {
//            for (int i = 0; i < li_locali.size(); i++) {
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            String newact = formatPS(action);
                            oper.setSql(sanitizeInput(newact));
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBFiliale db1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }

        if (errore.get() > 0) {
            return true;
        }

        try {
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} PER LA FILIALE {1}", new Object[]{li.size(), filiale});
            if (li.isEmpty()) {
                return false;
            }
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {

                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                log.log(Level.INFO, "{0} TYPE {1}", new Object[]{index.get(), type});
                log.log(Level.INFO, "{0} ACTION {1}", new Object[]{index.get(), action});
                index.addAndGet(1);
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            String newact = formatPS(action);
                            oper.setSql(sanitizeInput(newact));
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(action);
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBHost db1 = new DBHost(host_h, user_h, pwd_h, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return errore.get() > 0;
    }

    public static String sanitizeInput(String input) {
        // Rimuovi i caratteri che non sono lettere, numeri o underscore
        String sanitizedInput = input.replaceAll("[^a-zA-Z0-9_]", "");
        
        // Aggiungi un controllo per i caratteri HTML riservati
        sanitizedInput = sanitizedInput.replaceAll("&", "&amp;")
                                         .replaceAll("<", "&lt;")
                                         .replaceAll(">", "&gt;")
                                         .replaceAll("\"", "&quot;")
                                         .replaceAll("'", "&#39;");
        
        return sanitizedInput;
    }

    public boolean updateToBranch3(Logger log) {

        DateTime adesso = getTime(log);
        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        if (db.getConnectionDB() == null) {
            log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
            return true;
        }
        String myip = db.getIpFiliale(filiale);
        ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod(filiale, "0");
        db.closeDB();

        //nuovo
        DBFiliale dbfiliale1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);

        if (dbfiliale1.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
            return true;
        }

        ArrayList<Aggiornamenti_mod> li_locali = dbfiliale1.list_aggiornamenti_mod_div_limit_local(filiale, "0");

        dbfiliale1.closeDB();

        log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} IN LOCALE SULLA FILIALE {1}", new Object[]{li_locali.size(), filiale});
        AtomicInteger errore = new AtomicInteger(0);

        try {

            AtomicInteger index = new AtomicInteger(1);
            li_locali.forEach(st -> {
//            for (int i = 0; i < li_locali.size(); i++) {
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {

                            type = "ST";

                            String newact = action.substring(action.indexOf(":") + 1).trim();
                            if (newact.contains("VALUES")) {
                                newact = StringUtils.replace(newact, "('", "(\"");
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, "')", "\")");
                            } else if (newact.contains("UPDATE")) {
                                newact = StringUtils.replace(newact, "= '", "= \"");
                                newact = StringUtils.replace(newact, "', ", "\", ");
                                newact = StringUtils.replace(newact, "' , ", "\" , ");
                                newact = StringUtils.replace(newact, "' WHERE", "\" WHERE");
                                newact = StringUtils.replace(newact, "' AND", "\" AND");
                                if (newact.endsWith("'")) {
                                    newact = StringUtils.substring(newact, 0, newact.length() - 1);
                                    newact = newact + "\"";
                                }
                            } else if (newact.startsWith("INSERT INTO nc_transaction (SELECT")) {
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, ",'", ",\"");
                                newact = StringUtils.replace(newact, "' FROM", "\" FROM");
                                newact = StringUtils.replace(newact, "' ORDER BY", "\" ORDER BY");
                                newact = StringUtils.replace(newact, "= '", "= \"");
                            }
                            oper.setSql(newact);

                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBFiliale db1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }

        if (errore.get() > 0) {
            return true;
        }

        try {
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} PER LA FILIALE {1}", new Object[]{li.size(), filiale});
            if (li.isEmpty()) {
                return false;
            }
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {

                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                log.log(Level.INFO, "{0} TYPE {1}", new Object[]{index.get(), type});
                log.log(Level.INFO, "{0} ACTION {1}", new Object[]{index.get(), action});
                index.addAndGet(1);
                if (type.equalsIgnoreCase("ps")) {

                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {

                            type = "ST";
                            String newact = action.substring(action.indexOf(":") + 1).trim();
                            if (newact.contains("VALUES")) {
                                newact = StringUtils.replace(newact, "('", "(\"");
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, "')", "\")");
                            } else if (newact.contains("UPDATE") || newact.contains("INSERT INTO sito_prenotazioni SET")) {
                                newact = StringUtils.replace(newact, "= '", "= \"");
                                newact = StringUtils.replace(newact, "='", "= \"");
                                newact = StringUtils.replace(newact, "', ", "\", ");
                                newact = StringUtils.replace(newact, "' , ", "\" , ");
                                newact = StringUtils.replace(newact, "' WHERE", "\" WHERE");
                                newact = StringUtils.replace(newact, "' AND", "\" AND");
                                if (newact.endsWith("'")) {
                                    newact = StringUtils.substring(newact, 0, newact.length() - 1);
                                    newact = newact + "\"";
                                }
                            } else if (newact.startsWith("INSERT INTO nc_transaction (SELECT")) {
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, ",'", ",\"");
                                newact = StringUtils.replace(newact, "' FROM", "\" FROM");
                                newact = StringUtils.replace(newact, "' ORDER BY", "\" ORDER BY");
                                newact = StringUtils.replace(newact, "= '", "= \"");
                            }
                            oper.setSql(newact);
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(action);
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBHost db1 = new DBHost(host_h, user_h, pwd_h, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return errore.get() > 0;
    }

    public boolean updateToBranch2(Logger log) {

        DateTime adesso = getTime(log);
        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        if (db.getConnectionDB() == null) {
            log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
            return true;
        }
        String myip = db.getIpFiliale(filiale);
        ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod(filiale, "0");
        db.closeDB();

        //nuovo
        DBFiliale dbfiliale1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);

        if (dbfiliale1.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
            return true;
        }

        ArrayList<Aggiornamenti_mod> li_locali = dbfiliale1.list_aggiornamenti_mod_div_limit_local(filiale, "0");

        dbfiliale1.closeDB();

        log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} IN LOCALE SULLA FILIALE {1}", new Object[]{li_locali.size(), filiale});
        AtomicInteger errore = new AtomicInteger(0);

        try {

            AtomicInteger index = new AtomicInteger(1);
            li_locali.forEach(st -> {
//            for (int i = 0; i < li_locali.size(); i++) {
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";

                            String newact = action.substring(action.indexOf(":") + 1).trim();
                            if (newact.contains("VALUES")) {
                                newact = StringUtils.replace(newact, "('", "(\"");
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, "')", "\")");
                            } else if (newact.contains("UPDATE")) {
                                newact = StringUtils.replace(newact, "= '", "= \"");
                                newact = StringUtils.replace(newact, "', ", "\", ");
                                newact = StringUtils.replace(newact, "' , ", "\" , ");
                                newact = StringUtils.replace(newact, "' WHERE", "\" WHERE");
                                newact = StringUtils.replace(newact, "' AND", "\" AND");
                                if (newact.endsWith("'")) {
                                    newact = StringUtils.substring(newact, 0, newact.length() - 1);
                                    newact = newact + "\"";
                                }
                            } else if (newact.startsWith("INSERT INTO nc_transaction (SELECT")) {
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, ",'", ",\"");
                                newact = StringUtils.replace(newact, "' FROM", "\" FROM");
                                newact = StringUtils.replace(newact, "' ORDER BY", "\" ORDER BY");
                                newact = StringUtils.replace(newact, "= '", "= \"");
                            }
                            oper.setSql(newact);

                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBFiliale db1 = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }

        if (errore.get() > 0) {
            return true;
        }

        try {
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI {0} PER LA FILIALE {1}", new Object[]{li.size(), filiale});
            if (li.isEmpty()) {
                return false;
            }
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {

                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                log.log(Level.INFO, "{0} TYPE {1}", new Object[]{index.get(), type});
                log.log(Level.INFO, "{0} ACTION {1}", new Object[]{index.get(), action});
                index.addAndGet(1);
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            oper.setSql(action.substring(action.indexOf(":") + 1).trim());
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }

                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(action);
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);
                    if (dbfiliale.getConnectionDB() == null) {
                        log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
                        throw new BreakException();
                    }
                    boolean es = dbfiliale.execute_agg(type, oper);
                    dbfiliale.closeDB();
                    if (es) {
                        DBHost db1 = new DBHost(host_h, user_h, pwd_h, log);
                        db1.setStatus_agg(cod, "1");
                        db1.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return errore.get() > 0;
    }

    public boolean updateFromBranch(Logger log) {
        AtomicInteger errore = new AtomicInteger(0);

        DBHost db = new DBHost(host_h, user_h, pwd_h, log);
        if (db.getConnectionDB() == null) {
            log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
            return true;
        }
        String myip = db.getIpFiliale(filiale);
        db.closeDB();

        DBFiliale dbfiliale = new DBFiliale(myip, filiale, user_f, pwd_f, log);

        if (dbfiliale.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL''IP: {1}", new Object[]{filiale, myip});
            return true;
        }

        //mi prendo tutti gli aggiornamenti con filiale diversa da quella che sto aggiornando
        ArrayList<Aggiornamenti_mod> li = dbfiliale.list_aggiornamenti_mod_div_limit(filiale, "0");
        dbfiliale.closeDB();
        log.log(Level.WARNING, "NUMERO AGGIORNAMENTI DALLA FILIALE {0}: {1}", new Object[]{filiale, li.size()});

        if (li.isEmpty()) {
            return false;
        }

        DBFiliale dbfiliale2 = new DBFiliale(myip, filiale, user_f, pwd_f, log);
        if (dbfiliale2.getConnectionDB() == null) {
            log.log(Level.SEVERE, "FILIALE {0} NON RAGGIUNGIBILE ALL'IP: {1}", new Object[]{filiale, myip});
            return true;
        }
        try {
            AtomicInteger index = new AtomicInteger(1);

            li.forEach(st -> {
                log.log(Level.INFO, "Avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String filiale2 = st.getFiliale();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                String user = st.getUser();

                DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
                boolean es = db2.execute_agg_copia(cod, filiale2, dt_start, type, action, user);
                if (es) {
                    dbfiliale2.setStatus_agg(cod, "1");
                } else {
                    errore.addAndGet(1);
                    db2.closeDB();
                    throw new BreakException("ERR");
                }
                db2.closeDB();
            });
        } catch (Exception e) {
            errore.addAndGet(1);
            log.log(Level.SEVERE, "ERRORE PROCEDURA AGGIORNAMENTO updateToBranch FILIALE {0}: {1}", new Object[]{filiale, e.getMessage()});
        }
        dbfiliale2.closeDB();

        return errore.get() != 0;
    }

    public boolean updateCentral(Logger log) {
        AtomicInteger errore = new AtomicInteger(0);
        try {
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            if (db.getConnectionDB() == null) {
                log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
                return true;
            }
            ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod("000", "0");
            db.closeDB();
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI CENTRALE {0}", li.size());
            if (li.isEmpty()) {
                return false;
            }
            DateTime adesso = getTime(log);
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {
                log.log(Level.INFO, "avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();

                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            String newact = formatPS0(action);
                            oper.setSql(newact);
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
                    if (db2.getConnectionDB() == null) {
                        throw new BreakException();
                    }
                    boolean es = db2.execute_agg(type, oper);
                    db2.closeDB();
                    if (es) {
                        DBHost db3 = new DBHost(host_h, user_h, pwd_h, log);
                        if (db3.getConnectionDB() == null) {
                            throw new BreakException();
                        }
                        db3.setStatus_agg(cod, "1");
                        db3.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });

        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return errore.get() > 0;
    }

    public boolean updateCentral2(Logger log) {
        AtomicInteger errore = new AtomicInteger(0);
        try {
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            if (db.getConnectionDB() == null) {
                log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
                return true;
            }
            ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod("000", "0");
            db.closeDB();
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI CENTRALE {0}", (li.size()));
            if (li.isEmpty()) {
                return false;
            }
            DateTime adesso = Utility.getTime(log);
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {
                log.log(Level.INFO, "avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            String newact = formatPS(action);
                            oper.setSql(sanitizeInput(newact));
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(Utility.formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
                    if (db2.getConnectionDB() == null) {
                        throw new BreakException();
                    }
                    boolean es = db2.execute_agg(type, oper);
                    db2.closeDB();
                    if (es) {
                        DBHost db3 = new DBHost(host_h, user_h, pwd_h, log);
                        if (db3.getConnectionDB() == null) {
                            throw new BreakException();
                        }
                        db3.setStatus_agg(cod, "1");
                        db3.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return (errore.get() > 0);
    }

    public boolean updateCentral3(Logger log) {
        AtomicInteger errore = new AtomicInteger(0);
        try {
            DBHost db = new DBHost(host_h, user_h, pwd_h, log);
            if (db.getConnectionDB() == null) {
                log.warning("DB CENTRALE NON RAGGIUNGIBILE.");
                return true;
            }
            ArrayList<Aggiornamenti_mod> li = db.list_aggiornamenti_mod("000", "0");
            db.closeDB();
            log.log(Level.WARNING, "NUMERO AGGIORNAMENTI CENTRALE {0}", (li.size()));
            if (li.isEmpty()) {
                return false;
            }
            DateTime adesso = Utility.getTime(log);
            AtomicInteger index = new AtomicInteger(1);
            li.forEach(st -> {
                log.log(Level.INFO, "avanzamento.....  {0}", (index.get()));
                index.addAndGet(1);
                String cod = st.getCod();
                String dt_start = formatDateStart(st.getDt_start(), log);
                String type = st.getTipost();
                String action = st.getAction();
                Oper oper = new Oper();
                LinkedList<String> paramlist = new LinkedList<>();
                if (type.equalsIgnoreCase("ps")) {
                    if (action.contains("com.mysql.jdbc") || action.contains("com.mysql.cj.jdbc")) {
                        if (action.contains(":")) {
                            type = "ST";
                            String newact = action.substring(action.indexOf(":") + 1).trim();
                            if (newact.contains("VALUES")) {
                                newact = StringUtils.replace(newact, "('", "(\"");
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, "')", "\")");
                            } else if (newact.contains("UPDATE")) {
                                newact = StringUtils.replace(newact, "= '", "= \"");
                                newact = StringUtils.replace(newact, "', ", "\", ");
                                newact = StringUtils.replace(newact, "' , ", "\" , ");
                                newact = StringUtils.replace(newact, "' WHERE", "\" WHERE");
                                newact = StringUtils.replace(newact, "' AND", "\" AND");
                                if (newact.endsWith("'")) {
                                    newact = StringUtils.substring(newact, 0, newact.length() - 1);
                                    newact = newact + "\"";
                                }
                            } else if (newact.startsWith("INSERT INTO nc_transaction (SELECT")) {
                                newact = StringUtils.replace(newact, "','", "\",\"");
                                newact = StringUtils.replace(newact, ",'", ",\"");
                                newact = StringUtils.replace(newact, "' FROM", "\" FROM");
                                newact = StringUtils.replace(newact, "' ORDER BY", "\" ORDER BY");
                                newact = StringUtils.replace(newact, "= '", "= \"");
                            }
                            oper.setSql(newact);
                        }
                    } else {
                        action = action.replace("sql : '", "").trim();
                        if (action.contains("', parameters : ")) {
                            Iterable<String> parameters = Splitter.on("', parameters : ").split(action);
                            Iterator<String> it = parameters.iterator();
                            if (Iterators.size(it) == 2) {
                                it = parameters.iterator();
                                String sql = it.next();
                                String param = it.next();
                                oper.setSql(sql);
                                Iterable<String> parameters2 = Splitter.on("','").split(param);
                                Iterator<String> it2 = parameters2.iterator();
                                int length = Iterators.size(it2);
                                it2 = parameters2.iterator();
                                for (int j = 0; j < length; j++) {
                                    String val = it2.next();
                                    if (j == 0) {
                                        val = val.substring(2);
                                    } else if (j == length - 1) {
                                        val = val.substring(0, val.length() - 2);
                                    }
                                    paramlist.add(val.trim());
                                }
                                oper.setParam(paramlist);
                            }
                        }
                    }
                } else if (type.equalsIgnoreCase("st")) {
                    oper.setSql(st.getAction());
                }
                oper.setType(Utility.formatType(oper.getSql()));
                DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
                DateTime dt_start_value = formatter.parseDateTime(dt_start);
                if (dt_start_value.isBefore(adesso)) {
                    DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
                    if (db2.getConnectionDB() == null) {
                        throw new BreakException();
                    }
                    boolean es = db2.execute_agg(type, oper);
                    db2.closeDB();
                    if (es) {
                        DBHost db3 = new DBHost(host_h, user_h, pwd_h, log);
                        if (db3.getConnectionDB() == null) {
                            throw new BreakException();
                        }
                        db3.setStatus_agg(cod, "1");
                        db3.closeDB();
                    } else {
                        throw new BreakException();
                    }
                }
            });
        } catch (BreakException e) {
            errore.addAndGet(1);
        }
        return (errore.get() > 0);
    }

    public static String formatPS0(String ps) {
        try {
            ps = ps.substring(ps.indexOf(":") + 1).trim();
        } catch (Exception e) {
        }
        return ps;
    }

//    public static void main(String[] args) {
//        String s1 = "com.mysql.cj.jdbc.ClientPreparedStatement: INSERT INTO ch_transaction_temp (SELECT '063230105130044878yY1OjIl',LPAD((CAST(id AS DECIMAL(10))+1),15, '0'),'063','B','1607','001','2023-01-05 13:02:46','002','OCO230105100008206eUlQ7qn','208.80','225.77','9.50','7.43','-0.04','16.93','18.06','PC VENE'NEZIA APRILE','0','-','-','-','0','-','-','-','-','-','-','-','-','-','-','-','-','DMYHLN89T66Z138V','210807145551146IlQXGby063','0','1901-01-01 00:00:00','-','-','0','-','0','0','-','-','-' FROM ch_transaction_temp WHERE filiale = '063' ORDER BY CAST(id AS DECIMAL(10,2)) DESC LIMIT 1)";
//
//        System.out.println("rc.soop.start.Central_Branch.main() " + formatPS(s1));
//    }

    public static String formatPS(String ps) {
        ps = ps.substring(ps.indexOf(":") + 1).trim();
//        System.out.println("rc.soop.start.Central_Branch.formatPS(1) " + ps);
        try {

            ps = StringUtils.replace(ps, "\\", "-");
            ps = StringUtils.replace(ps, "\\\\", "-");
            ps = StringUtils.replace(ps, "= '", "=\"");
            ps = StringUtils.replace(ps, "='", "=\"");
            ps = StringUtils.replace(ps, "',", "\",");
            ps = StringUtils.replace(ps, "' ,", "\",");
            ps = StringUtils.replace(ps, ",'", ",\"");
            ps = StringUtils.replace(ps, "'0'", "\"0\"");
            ps = StringUtils.replace(ps, "' WHERE", "\" WHERE");
            ps = StringUtils.replace(ps, "'WHERE", "\" WHERE");
            ps = StringUtils.replace(ps, "' AND", "\" AND");
            ps = StringUtils.replace(ps, "'AND", "\" AND");
            ps = StringUtils.replace(ps, "SELECT '", "SELECT \"");
            ps = StringUtils.replace(ps, "' FROM", "\" FROM");
            ps = StringUtils.replace(ps, "' ORDER", "\" ORDER");
            ps = StringUtils.replace(ps, "VALUES ('", "VALUES (\"");
            ps = StringUtils.replace(ps, "')", "\")");
            ps = StringUtils.replace(ps, "** BYTE ARRAY DATA **", "\"\"");
            

        } catch (Exception e) {
        }
//        System.out.println("rc.soop.start.Central_Branch.formatPS(2) " + ps);

        StringBuilder ps_final = new StringBuilder();
        if ((ps.contains("UPDATE") && ps.contains("', ")
                || ps.contains("UPDATE") && ps.contains("',"))
                && !ps.contains("ch_transaction_doc") && !ps.contains("users")) {
            Splitter.on(" = ").splitToList(ps).forEach(s1 -> {
                if (!s1.contains("WHERE") && !s1.contains("AND ") && !s1.startsWith("''")) {
                    Splitter.on("', ").splitToList(s1).forEach(s2 -> {
                        String content = s2.startsWith("'") && !s2.endsWith(")") && !s2.endsWith("'")
                                && !s2.contains("UPDATE")
                                && !s2.contains("SELECT") && !s2.contains("FROM")
                                && !s2.contains("WHERE") ? StringUtils.substring(s2, 1) : s2;
                        if (!content.equals(s2)) {
                            String dest = StringUtils.replace(content, "\\", "-");
                            dest = StringUtils.replace(dest, "'", "\\'");
                            ps_final.append("'").append(dest);
                            ps_final.append("',");
                        } else {
                            ps_final.append(content);
                            ps_final.append(" = ");
                        }
                    });
                } else {
                    ps_final.append(s1).append(" = ");
                }
            });
        } else if (ps.contains("',")) {
            Splitter.on("',").splitToList(ps).forEach(s1 -> {
                String content
                        = s1.startsWith("'") && !s1.endsWith(")")
                        && !s1.contains("UPDATE") && !s1.contains("SELECT")
                        && !s1.contains("FROM") && !s1.contains("WHERE")
                        ? StringUtils.substring(s1, 1) : s1;
                if (!content.equals(s1)) {
                    String dest = StringUtils.replace(content, "\\", "-");
                    dest = StringUtils.replace(dest, "'", "\\'");
                    ps_final.append("'").append(dest);
                } else {
                    ps_final.append(content);
                }
                ps_final.append("',");
            });
        } else {
            ps_final.append(ps).append(" = ");
        }

        String psf = ps_final.toString();
        try {
            psf = StringUtils.replace(psf, "' =", "\" =");
        } catch (Exception e) {
        }
//        System.out.println("rc.soop.start.Central_Branch.formatPS(3) " + psf);
        return StringUtils.substring(psf, 0, psf.length() - 2).trim();
    }

//    private void createBat(boolean pause, Logger log) {
//        try {
//            DBHost db2 = new DBHost(host_h, user_h, pwd_h, log);
//            ArrayList<String> brlist = db2.list_branch_enabled();
//            db2.closeDB();
//            for (int i = 0; i < brlist.size(); i++) {
//                String fil = brlist.get(i);
//                File f1 = new File("PROD_FROM_" + fil + ".bat");
//                try ( FileOutputStream is = new FileOutputStream(f1);  OutputStreamWriter osw = new OutputStreamWriter(is);  BufferedWriter w = new BufferedWriter(osw)) {
//                    w.write("java -jar Mac2.0.jar " + fil + " TOCENTRAL");
//                    w.newLine();
//                    if (pause) {
//                        w.write("pause");
//                        w.newLine();
//                    }
//
//                }
//                try ( FileOutputStream is = new FileOutputStream(new File("PROD_TO_" + fil + ".bat"));  OutputStreamWriter osw = new OutputStreamWriter(is);  BufferedWriter w = new BufferedWriter(osw)) {
//                    w.write("java -jar Mac2.0.jar " + fil + " TOBRANCH");
//                    w.newLine();
//                    if (pause) {
//                        w.write("pause");
//                        w.newLine();
//                    }
//                }
//            }
//
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//    }
//    public static void main(String[] args) {
//        String filiale;
//        String metodo;
//        try {
//            filiale = args[0];
//            metodo = args[1];
//        } catch (Exception e) {
//            filiale = "000";
//            metodo = "CENTRAL";
//        }
//        if (filiale != null && metodo != null) {
//            Logger log = createLog("Mac2.0_AGG_" + metodo + "_" + filiale, rb.getString("path.log"), pattern4);
//            log.warning("START...");
//            Central_Branch cb = new Central_Branch(filiale);
//            switch (metodo) {
//                case "TOBRANCH":
//                    if (!cb.updateToBranch(log)) {
//                        log.log(Level.WARNING, "AGGIORNAMENTO VERSO LA  FILIALE: {0} COMPLETATO", filiale);
//                    } else {
//                        log.log(Level.SEVERE, "ERRORE AGGIORNAMENTO VERSO LA FILIALE: {0}", filiale);
//                    }
//                    break;
//                case "TOCENTRAL":
//                    if (!cb.updateFromBranch(log)) {
//                        log.log(Level.WARNING, "AGGIORNAMENTO DALLA  FILIALE: {0} COMPLETATO", filiale);
//                    } else {
//                        log.log(Level.SEVERE, "ERRORE AGGIORNAMENTO DALLA FILIALE: {0}", filiale);
//                    }
//                    break;
//                case "CENTRAL":
//                    if (!cb.updateCentral(log)) {
//                        log.warning("AGGIORNAMENTO CENTRALE COMPLETATO");
//                    } else {
//                        log.severe("ERRORE AGGIORNAMENTO CENTRALE");
//                    }
//                    break;
//                case "CREABAT":
//                    boolean pause = Boolean.parseBoolean(args[2]);
//                    cb.createBat(pause, log);
//                    break;
//                default:
//                    break;
//            }
//            log.warning("END.");
//        }
//    }
}
