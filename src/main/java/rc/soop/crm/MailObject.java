/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.crm;

import com.google.common.io.Files;
import static rc.soop.crm.Action.estraiAgevolazioni;
import static rc.soop.crm.Action.estraiServizi;
import static rc.soop.crm.Action.formatMysqltoDisplay;
import static rc.soop.crm.Action.formatStringtoStringDate;
import static rc.soop.crm.Action.generaId;
import static rc.soop.crm.Action.get_Settings;
import static rc.soop.crm.Action.get_descr_Branch;
import static rc.soop.crm.Action.log;
import static rc.soop.crm.Action.pat2;
import static rc.soop.crm.Action.pat4;
import static rc.soop.crm.Action.pat7;
import static rc.soop.crm.Items.all_CUR;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import static rc.soop.esolver.Util.rb;
import java.util.regex.Pattern;

/**
 *
 * @author rcosco
 */
public class MailObject {

    public static void mailerror_Cliente(String idprenot) {
        String[] dest = {"alena@maccorp.it", "mrivolta@maccorp.it"};
        String[] cc = {"mac2.0@smartoop.it"};
        String oggetto = "Attenzione: prenotazione " + idprenot + " - mancata comunicazione MAC-WEB";
        String testo = "Attenzione: per la prenotazione " + idprenot + " non è stato possibile aggiornare il WEB con la modifica dello stato della pratica";
        sendMailHtml(dest, cc, oggetto, testo);
    }

    public static boolean sendMailHtml(String[] dest, String[] cc, String oggetto, String testo) {
        try {
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
            Message message = new MimeMessage(session);
            InternetAddress froms = new InternetAddress(rb.getString("mail.sender"));
            froms.setPersonal(rb.getString("mail.personal"));
            message.setFrom(froms);
            for (String dest1 : dest) {
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(dest1));
            }
            for (String cc1 : cc) {
                message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(cc1));
            }
            message.setSubject(oggetto);
            Multipart mp = new MimeMultipart();
            MimeBodyPart mbp1 = new MimeBodyPart();
            mbp1.setContent(testo, "text/html");
            mp.addBodyPart(mbp1);
            message.setContent(mp);
            Transport.send(message);
            return true;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "sendMailHtml: {0}", ExceptionUtils.getStackTrace(ex));
        }
        return false;

    }

    public static boolean sendMailHtml(String dest, String oggetto, String testo) {
        try {
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

            Message message = new MimeMessage(session);
            InternetAddress froms = new InternetAddress(rb.getString("mail.sender"));
            froms.setPersonal(rb.getString("mail.personal"));
            message.setFrom(froms);
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(dest));
            message.setSubject(oggetto);
            Multipart mp = new MimeMultipart();
            MimeBodyPart mbp1 = new MimeBodyPart();
            mbp1.setContent(testo, "text/html");
            mp.addBodyPart(mbp1);
            message.setContent(mp);
            Transport.send(message);
            return true;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "sendMailHtml: {0}", ExceptionUtils.getStackTrace(ex));
        }
        return false;
    }

    public static boolean send_Mail_REMINDER(Booking bo, int days) {
        Items cu1 = all_CUR().stream().filter(br -> br.getCod().equalsIgnoreCase(bo.getCurrency())).distinct().findFirst().orElse(new Items(bo.getCurrency(), bo.getCurrency(), bo.getCurrency()));
        String luogo = Action.get_descr_Branch(bo.getFiliale());
        MailObject mo = new MailObject();
        mo.setOGGETTO("Ricordati la tua Prenotazione");
        mo.setIDPRENOTAZIONE(bo.getCod());
        mo.setINTRO("Gentile " + StringUtils.capitalize(bo.getCl_nome()) + " " + StringUtils.capitalize(bo.getCl_cognome()) + ",");
//        mo.setCORPO("ti ricordiamo che hai una prenotazione attiva con data di ritiro tra " + days + " giorni.");
        mo.setCORPO("Ti aspettiamo domani per consegnarti la valuta richiesta; se desideri modificare la data del ritiro o hai domande chiama il nostro numero verde 800/305357 operativo 7/7 alle 8.00  alle 22.00.");

        mo.setRIEPILOGO("Dettagli della prenotazione:<br/>"
                + "Nome: " + StringUtils.capitalize(bo.getCl_nome()) + "<br/>"
                + "Cognome: " + StringUtils.capitalize(bo.getCl_cognome()) + "<br/>"
                + "Prenotazione del: " + formatStringtoStringDate(bo.getTimestamp(), pat2, pat7) + "<br/>"
                + "Numero: " + bo.getCod() + "<br/>"
                + "Data ritiro: " + formatStringtoStringDate(bo.getDt_ritiro(), pat4, pat7) + "<br/>"
                + "Luogo ritiro: " + luogo + "<br/>"
                + "Valuta Richiesta: " + formatMysqltoDisplay(bo.getQuantity()) + " " + bo.getCurrency() + " " + cu1.getDescr() + "<br/>"
                + "Servizi Aggiuntivi: " + estraiServizi(bo) + "<br/>"
                + "Sconti: " + estraiAgevolazioni(bo) + "<br/>"
                + "Importo Euro: " + formatMysqltoDisplay(bo.getEuro()) + "<br/>"
                + "Codice Ufficio: " + bo.getFiliale());
        mo.setALTREINFO("Al momento del ritiro della valuta, porta con te Codice Fiscale e un Documento di riconoscimento valido.");
        mo.setNVERDE("Per richiedere assistenza contatta il nostro Numero Verde 800 30 53 57");
        mo.setCHIUSURA("Buonviaggio dal team di Forexchange &#38; Travel, i tuoi assistenti di viaggio.");
        try {
            String subject = mo.getOGGETTO() + ": " + mo.getIDPRENOTAZIONE();
            String pathTemp = "/mnt/mac/temp/";
            Settings mail = get_Settings("TM1");
            File temp = new File(pathTemp + generaId(75) + "_temp.html");
            FileUtils.writeByteArrayToFile(temp, Base64.decodeBase64(mail.getValue()));
            String content = Files.asCharSource(temp, StandardCharsets.UTF_8).read();
            content = StringUtils.replace(content, "\\{\\{DESC_PRENOTAZIONE\\}\\}", "Prenotazione n.");
            content = StringUtils.replace(content, "\\{\\{ID PRENOTAZIONE\\}\\}", mo.getIDPRENOTAZIONE());
            content = StringUtils.replace(content, "\\{\\{TESTO INTRO\\}\\}", mo.getINTRO());
            content = StringUtils.replace(content, "\\{\\{TESTO CORPO\\}\\}", mo.getCORPO());
            content = StringUtils.replace(content, "\\{\\{TESTO RIEPILOGO\\}\\}", mo.getRIEPILOGO());
            content = StringUtils.replace(content, "\\{\\{TESTO ALTRE INFO\\}\\}", mo.getALTREINFO());
            content = StringUtils.replace(content, "\\{\\{TESTO NUMERO VERDE\\}\\}", mo.getNVERDE());
            content = StringUtils.replace(content, "\\{\\{TESTO CHIUSURA\\}\\}", mo.getCHIUSURA());
            boolean es = sendMailHtml(bo.getCl_email(), subject, content);
            return es;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "send_Mail_REMINDER {0}", ex.getMessage());
        }
        return false;
    }

    public static boolean send_Mail_EXPIRED(Booking bo) {
        Items cu1 = all_CUR().stream()
            .filter(br -> br.getCod().equalsIgnoreCase(bo.getCurrency()))
            .distinct()
            .findFirst()
            .orElse(new Items(bo.getCurrency(), bo.getCurrency(), bo.getCurrency()));
    
        String luogo = get_descr_Branch(bo.getFiliale());
        MailObject mo = new MailObject();
        mo.setOGGETTO("Comunicazione importante sulla Prenotazione");
        mo.setIDPRENOTAZIONE(bo.getCod());
        mo.setINTRO("Gentile " + bo.getCl_nome().toUpperCase() + " " + bo.getCl_cognome().toUpperCase());
        mo.setCORPO("la tua prenotazione, dalla giornata di ieri, è pronta per essere ritirata nell'agenzia Forexchange da te indicata. "
            + "Al fine di non perdere i vantaggi ottenuti in fase di prenotazione, passa a ritirarla entro le prossime 24 ore, "
            + "altrimenti procedi alla cancellazione o modifica tramite la MyArea sul nostro sito.");
        mo.setRIEPILOGO("Dettagli della prenotazione:<br/>"
            + "Nome: " + bo.getCl_nome().toUpperCase() + "<br/>Cognome: " + bo.getCl_cognome().toUpperCase() + "<br/>"
            + "Prenotazione del: " + formatStringtoStringDate(bo.getTimestamp(), pat2, pat7) + "<br/>"
            + "Numero: " + bo.getCod() + "<br/>"
            + "Data ritiro: " + formatStringtoStringDate(bo.getDt_ritiro(), pat4, pat7) + "<br/>"
            + "Luogo ritiro: " + luogo + "<br/>"
            + "Valuta Richiesta: " + formatMysqltoDisplay(bo.getQuantity()) + " " + bo.getCurrency() + " " + cu1.getDescr() + "<br/>"
            + "Servizi Aggiuntivi: " + estraiServizi(bo) + "<br/>"
            + "Sconti: " + estraiAgevolazioni(bo) + "<br/>"
            + "Importo Euro: " + formatMysqltoDisplay(bo.getEuro()) + "<br/>"
            + "Codice Ufficio: " + bo.getFiliale());
        mo.setALTREINFO("Al momento del ritiro della valuta, porta con te Codice Fiscale e un Documento di riconoscimento valido.");
        mo.setNVERDE("Per richiedere assistenza contatta il nostro Numero Verde 800 30 53 57");
        mo.setCHIUSURA("Buonviaggio dal team di Forexchange &#38; Travel, i tuoi assistenti di viaggio.");
    
        try {
            String subject = mo.getOGGETTO() + ": " + mo.getIDPRENOTAZIONE();
            String pathTemp = "/mnt/mac/temp/";
            Settings mail = get_Settings("TM1");
            File temp = new File(pathTemp + generaId(75) + "_temp.html");
            FileUtils.writeByteArrayToFile(temp, Base64.decodeBase64(mail.getValue()));
    
            String content = Files.asCharSource(temp, StandardCharsets.UTF_8).read();
    
            content = StringUtils.replace(content, Pattern.quote("{{DESC_PRENOTAZIONE}}"), "Prenotazione n.");
            content = StringUtils.replace(content, Pattern.quote("{{ID PRENOTAZIONE}}"), mo.getIDPRENOTAZIONE());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO INTRO}}"), mo.getINTRO());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO CORPO}}"), mo.getCORPO());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO RIEPILOGO}}"), mo.getRIEPILOGO());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO ALTRE INFO}}"), mo.getALTREINFO());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO NUMERO VERDE}}"), mo.getNVERDE());
            content = StringUtils.replace(content, Pattern.quote("{{TESTO CHIUSURA}}"), mo.getCHIUSURA());
    
            boolean es = sendMailHtml(bo.getCl_email(), subject, content);
            return es;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "send_Mail_EXPIRED {0}", ex.getMessage());
        }
        return false;
    }

    String OGGETTO, IDPRENOTAZIONE, INTRO, CORPO, RIEPILOGO, ALTREINFO, NVERDE, CHIUSURA;

    public MailObject() {
    }

    public String getOGGETTO() {
        return OGGETTO;
    }

    public void setOGGETTO(String OGGETTO) {
        this.OGGETTO = OGGETTO;
    }

    public String getIDPRENOTAZIONE() {
        return IDPRENOTAZIONE;
    }

    public void setIDPRENOTAZIONE(String IDPRENOTAZIONE) {
        this.IDPRENOTAZIONE = IDPRENOTAZIONE;
    }

    public String getINTRO() {
        return INTRO;
    }

    public void setINTRO(String INTRO) {
        this.INTRO = INTRO;
    }

    public String getCORPO() {
        return CORPO;
    }

    public void setCORPO(String CORPO) {
        this.CORPO = CORPO;
    }

    public String getRIEPILOGO() {
        return RIEPILOGO;
    }

    public void setRIEPILOGO(String RIEPILOGO) {
        this.RIEPILOGO = RIEPILOGO;
    }

    public String getALTREINFO() {
        return ALTREINFO;
    }

    public void setALTREINFO(String ALTREINFO) {
        this.ALTREINFO = ALTREINFO;
    }

    public String getNVERDE() {
        return NVERDE;
    }

    public void setNVERDE(String NVERDE) {
        this.NVERDE = NVERDE;
    }

    public String getCHIUSURA() {
        return CHIUSURA;
    }

    public void setCHIUSURA(String CHIUSURA) {
        this.CHIUSURA = CHIUSURA;
    }

}
