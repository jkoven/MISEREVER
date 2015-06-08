/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.activation.DataSource;
import javax.json.JsonArray;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.mail.util.MimeMessageParser;
/**
 *
 * @author jkoven
 */
public class EmailContentServer {

    public static String getEmails(String[] fileNames) {
        String rString = "";
        try {
            for (String fileName : fileNames) {
                Session s = Session.getDefaultInstance(new Properties());
                InputStream is = new FileInputStream(fileName);
                MimeMessage message = new MimeMessage(s, is);
                MimeMessageParser mp = new MimeMessageParser(message);
                is.close();
                File attachmentDirectory = new File("/Users/jkoven/NetBeansProjects/IM/public_html/attachments/");
                File f = new File(fileName);
                attachmentDirectory = new File(attachmentDirectory,f.getName());
                if (attachmentDirectory.exists()){
                    attachmentDirectory.delete();
                }
                attachmentDirectory.mkdir();
                Enumeration headers = message.getAllHeaders();
                rString += "<pre>";
                while (headers.hasMoreElements()) {
                    Header h = (Header) headers.nextElement();
                    rString += h.getName() + ": " + h.getValue() + "\n";
                }
                rString += "</pre>\n\n";
                try {
                    mp.parse();
                } catch (Exception ex) {
                    Logger.getLogger(EmailContentServer.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (mp.hasHtmlContent()) {
                    rString += mp.getHtmlContent() + "\n";
                } else {
                    if (mp.hasPlainContent()) {
                        rString += "<pre>" + mp.getPlainContent() + "</pre>";
                    }
                }
                rString += "</br></br>";
                int attachmentCount = 0;
                String sName = "";
                for (DataSource ds : mp.getAttachmentList()) {
                    if (ds.getName() == null) {
                        sName = "attachment" + attachmentCount++ + "."
                                + ds.getContentType().substring(ds.getContentType().indexOf("/"));
                    } else {
                        sName = attachmentCount++ + ds.getName();
                    }
                    InputStream ist = ds.getInputStream();
                    FileOutputStream fos = new FileOutputStream(
                            new File(attachmentDirectory, sName));
                    IOUtils.copy(ist, fos);
                    ist.close();
                    fos.close();
                    switch (ds.getContentType().substring(0, ds.getContentType().indexOf("/"))) {
                        case "image": {
                            rString += "<img src='attachments/" + f.getName() + "/" + sName + "'></br>";
                            break;
                        }
                        case "audio": {
                            rString += "<audio controls title=" + f.getName() + "/" + sName + ">\n"
                                    + " <source src='attachments/" + f.getName() + "/" + sName + "' type=" + ds.getContentType() + ">\n"
                                    + "</audio></br>";
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                    rString += "<a href='attachments/" + f.getName() + "/" + sName + "' download>" + sName + "</a></br>";
                }
                rString += "</br></br>";
            }
        } catch (IOException ex) {
            System.out.println(ex);
        } catch (MessagingException ex) {
            System.out.println(ex);
        }
        return (rString);
    }

}
