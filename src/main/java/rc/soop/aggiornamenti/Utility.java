/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.aggiornamenti;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author rcosco
 */
public class Utility {

    public static final String patternnormdate = "dd/MM/yyyy HH:mm:ss";
    public static final String patternsqldate = "yyyy-MM-dd HH:mm:ss";
    public static final String patternsql = "yyyy-MM-dd";
    public static final String patternnormdate_filter = "dd/MM/yyyy";
    public static final SimpleDateFormat sdf_ita = new SimpleDateFormat(patternnormdate);

    public static String generaId(int length) {
        String random = RandomStringUtils.randomAlphanumeric(length - 15).trim();
        return new DateTime().toString("yyMMddHHmmssSSS") + random;
    }

    public static String visualizzaStringaMySQL(String ing) {
        if (ing == null) {
            return "";
        }
        ing = StringUtils.replace(ing, "\\'", "'");
        ing = StringUtils.replace(ing, "\'", "'");
        ing = StringUtils.replace(ing, "\"", "'");
        //ing = StringUtils.replace(ing, "€", "&#0128;");
        return ing.trim();
    }

    public static DateTime formatStringtoStringDate(String dat, String pattern1) {
        try {
            if (dat.length() == 21) {
                dat = dat.substring(0, 19);
            }
            if (dat.length() == pattern1.length()) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                return formatter.parseDateTime(dat);
            } else if (dat.length() == pattern1.length() - 3) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern1);
                return formatter.parseDateTime(dat + ":00");
            }
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        return null;
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
        } catch (IllegalArgumentException ex) {

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

    public static String formatIndex(ArrayList<String> valori) {
        String out = "";
        String start = "(";
        String end = ")";
        for (int i = 0; i < valori.size(); i++) {
            out = out + "`" + valori.get(i) + "`, ";
        }
        out = out.trim();
        if (out.contains(",")) {
            out = out.substring(0, out.length() - 1);
        }
        return start + out + end;
    }

    public static String[] formatAL(String cod, ArrayList<String[]> array) {
        for (int i = 0; i < array.size(); i++) {
            if (cod.equals(array.get(i)[0])) {
                return array.get(i);
            }
        }
        return null;
    }

    public static boolean sendMail(String subject, String text, File attach, boolean solosmartoop) {
        ResourceBundle rb = ResourceBundle.getBundle("esolver.conf");
        Properties props = new Properties();
        props.put("mail.smtp.host", rb.getString("mail.smtp"));
        props.put("mail.smtp.socketFactory.port", rb.getString("mail.smtp.port"));
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", rb.getString("mail.smtp.port"));
        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(rb.getString("mail.sender"), rb.getString("mail.pass"));
            }
        });
        try {
            Message message = new MimeMessage(session);
            InternetAddress froms = new InternetAddress(rb.getString("mail.sender"));
            froms.setPersonal("Noreply Mac2.0");
            message.setFrom(froms);
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("mac2.0@smartoop.it"));
            if (!solosmartoop) {
                message.setRecipients(Message.RecipientType.CC, InternetAddress.parse("support@maccorp.it"));
            }            
            message.setSubject(subject);
            Multipart mp = new MimeMultipart();
            MimeBodyPart mbp1 = new MimeBodyPart();
            mbp1.setContent(text, "text/html");
            mp.addBodyPart(mbp1);
            if (attach != null) {
                MimeBodyPart mbp2 = new MimeBodyPart();
                DataSource source = new FileDataSource(attach);
                mbp2.setDataHandler(new DataHandler(source));
                mbp2.setFileName("attach.txt");
                mp.addBodyPart(mbp2);
            }
            message.setContent(mp);
            Transport.send(message);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static boolean sendMail(File fileDaAllegare) {
        ResourceBundle rb = ResourceBundle.getBundle("esolver.conf");
        Properties props = new Properties();
        props.put("mail.smtp.host", rb.getString("mail.smtp"));
        props.put("mail.smtp.socketFactory.port", rb.getString("mail.smtp.port"));
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", rb.getString("mail.smtp.port"));
        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(rb.getString("mail.sender"), rb.getString("mail.pass"));
            }
        });
        try {
            Message message = new MimeMessage(session);
            InternetAddress froms = new InternetAddress(rb.getString("mail.sender"));
            froms.setPersonal("Noreply Mac2.0");
            message.setFrom(froms);
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("mac2.0@smartoop.it"));
            message.setRecipients(Message.RecipientType.CC, InternetAddress.parse("support@maccorp.it"));
            message.setSubject("AGGIORNAMENTI IN CODA");
            Multipart mp = new MimeMultipart();
            MimeBodyPart mbp1 = new MimeBodyPart();
            mbp1.setContent("In allegato il file con il dettaglio degli aggiornamenti da controllare.", "text/html");
            mp.addBodyPart(mbp1);
            if (fileDaAllegare != null) {
                MimeBodyPart mbp2 = new MimeBodyPart();
                DataSource source = new FileDataSource(fileDaAllegare);
                mbp2.setDataHandler(new DataHandler(source));
                mbp2.setFileName("aggiornamenti.txt");
                mp.addBodyPart(mbp2);
            }
            message.setContent(mp);
            Transport.send(message);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static String formatDoubleforMysql(String value) {

        String thousand = ".";
        String decimal = ",";

        if (value == null) {
            return "0.00";
        }
        if (value.equals("-") || value.equals("")) {
            return "0.00";
        }
        String add = "";
        if (value.contains("-")) {
            add = "-";
            value = value.replaceAll("-", "").trim();
        }

        if (!value.equals("0.00")) {
            if (thousand.equals(".")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(",")) {
                        value = value.replaceAll("\\.", "");
                        value = value.replaceAll(",", ".");
                    }
                } else {
                    value = value.replaceAll("\\.", "");

                    return value + ".00";
                }
            } else if (thousand.equals(",")) {
                if (value.contains(decimal)) {
                    if (decimal.equals(".")) {
                        value = value.replaceAll(",", "");
                    }
                } else {
                    value = value.replaceAll(",", "");
                    return value + "00";
                }
            }
        }

        return add + value;

    }

}
