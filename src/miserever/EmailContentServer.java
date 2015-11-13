/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.activation.DataSource;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.mail.util.MimeMessageParser;

/**
 *
 * @author jkoven
 */
public class EmailContentServer {

    public static String getEmails(ArrayList<String> fileNames, boolean showHeaders, String urlPath) {
        File attachmentDirectory = null;
        String rString = "";
        try {
            File attachmentBaseDirectory = new File(urlPath + "/attachments/");
            FileUtils.cleanDirectory(attachmentBaseDirectory);
            for (String fileName : fileNames) {
                Session s = Session.getDefaultInstance(new Properties());
                InputStream is = new FileInputStream(fileName);
                MimeMessage message = new MimeMessage(s, is);
                MimeMessageParser mp = new MimeMessageParser(message);
                is.close();
                File f = new File(fileName);
                if (showHeaders) {
                    Enumeration headers = message.getAllHeaders();
                    rString += "<pre>";
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                        rString += h.getName() + ": " + h.getValue() + "\n";
                    }
                    rString += "</pre>\n\n";
                }
                try {
                    mp.parse();
                } catch (Exception ex) {
                    Logger.getLogger(EmailContentServer.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (mp.hasAttachments()) {
                    attachmentDirectory = new File(attachmentBaseDirectory, f.getName());
                    attachmentDirectory.mkdir();
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
                                + ds.getContentType().substring(ds.getContentType().indexOf("/") + 1);
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
                        case "application":
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
            ex.printStackTrace();
            System.out.println(ex);
        } catch (MessagingException ex) {
            System.out.println(ex);
        }
        return (rString);
    }

    public static void mergeEmails(boolean useHeaders, String[] fileNames, File outFile, File imageDir) {
        try {
            PrintWriter mergeout
                    = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
            mergeout.println("<!DOCTYPE html>\n<html>\n");
            for (String fileName : fileNames) {
                Session s = Session.getDefaultInstance(new Properties());
                InputStream is = new FileInputStream(fileName);
                MimeMessage message = new MimeMessage(s, is);
                MimeMessageParser mp = new MimeMessageParser(message);
                is.close();
//                File attachmentDirectory = new File("/Users/jkoven/NetBeansProjects/IM/public_html/attachments/");
                File f = new File(fileName);
                Enumeration headers = message.getAllHeaders();
                mergeout.print("<div style=\"white-space: pre-wrap;\">");
                mergeout.println(fileName.substring(fileName.lastIndexOf("/") + 1) + "\n");
                if (useHeaders) {
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                        mergeout.println(h.getName() + ": " + h.getValue());
                    }
                }
                mergeout.println("</div>\n");
                try {
                    mp.parse();
                } catch (Exception ex) {
                    Logger.getLogger(EmailContentServer.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (mp.hasHtmlContent()) {
                    mergeout.println(mp.getHtmlContent());
                } else {
                    if (mp.hasPlainContent()) {
                        mergeout.println("<div style=\"white-space: pre-wrap;\">" + mp.getPlainContent() + "</div>");
                    }
                }
                File attachmentDirectory = new File(imageDir, f.getName());
                if (mp.hasAttachments()) {
                    if (attachmentDirectory.exists()) {
                        attachmentDirectory.delete();
                    }
//                System.out.println(attachmentDirectory.toString());
                    attachmentDirectory.mkdirs();
                }
                int attachmentCount = 0;
                String sName = "";
                for (DataSource ds : mp.getAttachmentList()) {
                    if (ds.getName() == null) {
                        sName = "attachment" + attachmentCount++ + "."
                                + ds.getContentType().substring(ds.getContentType().indexOf("/") + 1);
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
                        case "application":
                        case "image": {
                            mergeout.println("<img src='images/" + f.getName() + "/" + sName + "'</br>");
                            break;
                        }
                        case "audio": {
                            mergeout.println("<audio controls title=" + f.getName() + "/" + sName + ">\n"
                                    + " <source src='images/" + f.getName() + "/" + sName + "' type=" + ds.getContentType() + ">\n"
                                    + "</audio></br>");
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                    mergeout.println("<a href='images/" + f.getName() + "/" + sName + "' download>" + sName + "</a></br>");
                }
                mergeout.println("</br>");
//                if (++fileCount % 100 == 0) {
//                    System.out.println(fileCount);
//                }
            }
            mergeout.println("</html>");
            mergeout.close();
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println(ex);
        } catch (MessagingException ex) {
            System.out.println(ex);
        }
    }

    public static String saveSelected(HashMap<String, String> emailInfo, File saveDirectory, boolean merge, boolean useHeaders) {
        try {
            File htmlSaveDirectory = new File(saveDirectory, "html_messages");
            File mergeSaveDirectory = new File(saveDirectory, "html_merged_messages");
            File imageDir = new File(htmlSaveDirectory, "attachments");
            File originalDir = new File(saveDirectory, "messages");
            saveDirectory.mkdirs();
            FileUtils.cleanDirectory(saveDirectory);
            htmlSaveDirectory.mkdirs();
            mergeSaveDirectory.mkdirs();
            originalDir.mkdirs();
            for (String path : emailInfo.keySet()) {
                String fileName = path;
                File copyFile = new File(originalDir, new File(fileName).getName());
                FileUtils.copyFile(new File(fileName), copyFile);
                if (!merge) {
                    String outfileName = emailInfo.get(path).replaceAll("\\W", "_");
                    PrintWriter fileout
                            = new PrintWriter(new BufferedWriter(new FileWriter(
                                                    new File(htmlSaveDirectory + "/" + outfileName))));
                    fileout.println("<!DOCTYPE html>\n<html>\n");
                    Session s = Session.getDefaultInstance(new Properties());
                    InputStream is = new FileInputStream(fileName);
                    MimeMessage message = new MimeMessage(s, is);
                    MimeMessageParser mp = new MimeMessageParser(message);
                    is.close();
//                File attachmentDirectory = new File("/Users/jkoven/NetBeansProjects/IM/public_html/attachments/");
                    File f = new File(outfileName);
                    Enumeration headers = message.getAllHeaders();
                    fileout.print("<pre>");
                    fileout.println(fileName + "\n");
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                        fileout.println(h.getName() + ": " + h.getValue());
                    }
                    fileout.println("</pre>\n");
                    try {
                        mp.parse();
                    } catch (Exception ex) {
                        Logger.getLogger(EmailContentServer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    if (mp.hasHtmlContent()) {
                        fileout.println(mp.getHtmlContent());
                    } else {
                        if (mp.hasPlainContent()) {
                            fileout.println("<pre>" + mp.getPlainContent() + "</pre>");
                        }
                    }
                    File attachmentDirectory = new File(imageDir, f.getName());
                    if (mp.hasAttachments()) {
                        if (attachmentDirectory.exists()) {
                            attachmentDirectory.delete();
                        }
//                System.out.println(attachmentDirectory.toString());
                        attachmentDirectory.mkdirs();
                    }
                    int attachmentCount = 0;
                    String sName = "";
                    for (DataSource ds : mp.getAttachmentList()) {
                        if (ds.getName() == null) {
                            sName = "attachment" + attachmentCount++ + "."
                                    + ds.getContentType().substring(ds.getContentType().indexOf("/") + 1);
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
                            case "application":
                            case "image": {
                                fileout.println("<img src='attachments/" + f.getName() + "/" + sName + "'width=900px></br>");
                                break;
                            }
                            case "audio": {
                                fileout.println("<audio controls title=" + f.getName() + "/" + sName + ">\n"
                                        + " <source src='attachments/" + f.getName() + "/" + sName + "' type=" + ds.getContentType() + ">\n"
                                        + "</audio></br>");
                                break;
                            }
                            default: {
                                break;
                            }
                        }
                        fileout.println("<a href='attachments/" + f.getName() + "/" + sName + "' download>" + sName + "</a></br>");
                    }
                    fileout.println("</br>");
//                if (++fileCount % 100 == 0) {
//                    System.out.println(fileCount);
//                }
                    fileout.println("</html>");
                    fileout.close();
                }
            }
            if (merge) {
                String[] emails = new String[emailInfo.keySet().size()];
                int i = 0;
                for (String key : emailInfo.keySet()) {
                    emails[i++] = key;
                }
                int partCount = 1;
                int emailStart = 0;
                int totalEmails = emails.length;
                int emailEnd = Math.min(500, totalEmails);
                while (true) {
                    EmailContentServer.mergeEmails(useHeaders, Arrays.copyOfRange(emails, emailStart, emailEnd),
                            new File(mergeSaveDirectory, "Collection_" + partCount++ + ".html"),
                            new File(mergeSaveDirectory, "images"));
                    if (emailEnd == totalEmails) {
                        break;
                    }
                    emailStart = emailEnd;
                    emailEnd = Math.min(emailEnd + 500, totalEmails);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println(ex);
        } catch (MessagingException ex) {
            System.out.println(ex);
        }
        return (saveDirectory.getPath());
    }
}
