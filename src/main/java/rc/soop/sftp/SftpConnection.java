/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.sftp;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import rc.soop.qlik.LoggerNew;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author srotella
 */
public class SftpConnection {

    public static ChannelSftp connect(String user, String ip, int port, String privateKey, LoggerNew logger) {
        try {
            JSch jsch = new JSch();
            jsch.addIdentity(privateKey);//add identity
            Session session = jsch.getSession(user, ip, port);
            Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setTimeout(30000);
            session.setConfig(config);
            session.connect(30000);
            Channel channel = session.openChannel("sftp");
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.connect(3000);
            return sftp;
        } catch (JSchException e) {
            e.printStackTrace();
            logger.log.log(Level.SEVERE, "Errore connessione Sftp: {0}", e.getMessage());
        }
        return null;
    }

    public static ChannelSftp connect(
            String user, String pwd,
            String ip, int port, LoggerNew logger
    ) {
        try {
            JSch jsch = new JSch();
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            Session session = jsch.getSession(user, ip, port);
            session.setPassword(pwd);
            session.setConfig(config);
            session.connect(30000);
            Channel channel = session.openChannel("sftp");
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.connect(3000);
            return sftp;
        } catch (JSchException e) {
            logger.log.log(Level.SEVERE, "Errore connessione Sftp: {0}", e.getMessage());
        }
        return null;
    }

    public static ChannelSftp connect(
            String user, String pwd,
            String ip, int port, Logger logger
    ) {
        try {
            JSch jsch = new JSch();
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            Session session = jsch.getSession(user, ip, port);
            session.setPassword(pwd);
            session.setConfig(config);
            session.connect(30000);
            Channel channel = session.openChannel("sftp");
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.connect(3000);
            return sftp;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Errore connessione Sftp: {0}", e.getMessage());
        }
        return null;
    }

    public static void closeConnection(ChannelSftp sftp, LoggerNew logger) {
        try {
            sftp.disconnect();
            sftp.getSession().disconnect();
        } catch (JSchException e) {
            logger.log.log(Level.SEVERE, "Errore disconnessione Sftp: {0}", e.getMessage());
        }
    }

    public static void closeConnection(ChannelSftp sftp, Logger logger) {
        try {
            sftp.disconnect();
            sftp.getSession().disconnect();
        } catch (JSchException e) {
            logger.log(Level.SEVERE, "Errore disconnessione Sftp: {0}", e.getMessage());
        }
    }

}
