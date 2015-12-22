/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.awt.Component;
import java.awt.HeadlessException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import static miserever.CommandHandler.jMap;
import static miserever.CommandHandler.topUsersList;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

/**
 *
 * @author jkoven
 */
public class FileUtils {

    public String baseDir;

    public FileUtils(String baseDir) {
        this.baseDir = baseDir;
    }

    public void save(JsonObject jList) {
        File sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        OutputStream sout = null;
        try {
            String fileName = jList.getString("filename");
            HashMap jMap = new HashMap();
            jMap.put(JsonGenerator.PRETTY_PRINTING, true);
            sout = new FileOutputStream(new File(sourceFile.getParent(), "workingfiles/" + fileName));
            JsonWriterFactory jwf = Json.createWriterFactory(jMap);
            JsonWriter jsonWriter = jwf.createWriter(sout);
            jsonWriter.writeObject(jList);
            jsonWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                sout.close();
            } catch (IOException ex) {
                Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public JsonObject getFileList() {

        File sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        File dir = new File(sourceFile.getParent(), "workingfiles/");
        File[] filesList = dir.listFiles();
        JsonBuilderFactory jFactory = Json.createBuilderFactory(new HashMap<String, String>());
        JsonObjectBuilder jFileList = jFactory.createObjectBuilder();
        JsonArrayBuilder jFiles = jFactory.createArrayBuilder();
        for (int i = 0; i < filesList.length; i++) {
            if (filesList[i].isFile()) {
                if (!filesList[i].getName().startsWith(".")) {
                    jFiles.add(filesList[i].getName());
                }
            }
        }

        jFileList.add(
                "filelist", jFiles.build());
        return jFileList.build();
    }

    public JsonObject getFile(String fileName) {
        File sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        JsonObject jsonIn = null;
        try {
            JsonReaderFactory rFactory = Json.createReaderFactory(new HashMap<String, String>());
            JsonReader jr = rFactory.createReader(new FileReader(new File(sourceFile.getParent(), "workingfiles/" + fileName)));
            jsonIn = jr.readObject();
            jr.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(FileUtils.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return (jsonIn);
    }

    public void saveMergeFiles(boolean headers, IndexReader reader, File dumpDir) {
//        System.out.println("Table Build");
        IndexSearcher searcher = new IndexSearcher(reader);
        // First build a map of all the documents with computers and users and  timestamps
        Map<String, Map<String, ArrayList<Pair< String, Long>>>> emailTable = new HashMap<>();
        for (int i = 0; i < reader.maxDoc(); i++) {
//            if(i % 1000 == 0){
//                System.out.println(i);
//            }
            try {
                Document doc = reader.document(i);
                String p = doc.get("path");
                String c = doc.get("computer");
                String u = doc.get("user");
                String t = doc.get("timestamp");
                if (c != null && u != null && t != null) {
                    if (emailTable.containsKey(c)) {
                        if (emailTable.get(c).containsKey(u)) {
                            emailTable.get(c).get(u).add(new Pair<>(p, Long.parseLong(t)));
                        } else {
                            ArrayList<Pair<String, Long>> elist = new ArrayList<>();
                            elist.add(new Pair<>(p, Long.parseLong(t)));
                            emailTable.get(c).put(u, elist);
                        }
                    } else {
                        Map<String, ArrayList<Pair<String, Long>>> emap = new HashMap<>();
                        ArrayList<Pair<String, Long>> elist = new ArrayList<>();
                        elist.add(new Pair<>(p, Long.parseLong(t)));
                        emap.put(u, elist);
                        emailTable.put(c, emap);
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        //Sort the lists
//        System.out.println("Starting Sort");
        if (!dumpDir.exists()) {
            dumpDir.mkdirs();
        }
        for (String computer : emailTable.keySet()) {
            for (String user : emailTable.get(computer).keySet()) {
                ArrayList<Pair<String, Long>> fileList = emailTable.get(computer).get(user);
                Collections.sort(fileList);
                String[] emails = new String[fileList.size()];
                for (int i = 0; i < fileList.size(); i++) {
                    emails[i] = fileList.get(i).key;
                }
//                System.out.println("Merging " + emails.length + " files");
//                System.out.println(computer + " " + user);
                //Dump the sorted list into files
                int partCount = 1;
                int emailStart = 0;
                int totalEmails = emails.length;
                int emailEnd = Math.min(500, totalEmails);
                while (true) {
                    EmailContentServer.mergeEmails(headers, Arrays.copyOfRange(emails, emailStart, emailEnd),
                            new File(dumpDir, computer + "_" + user + "_" + partCount++ + ".html"),
                            new File(dumpDir, "images"));
                    if (emailEnd == totalEmails) {
                        break;
                    }
                    emailStart = emailEnd;
                    emailEnd = Math.min(emailEnd + 500, totalEmails);
                }
            }
        }
    }

    public String saveSelected(JsonObject jList) {
        try {
            JFileChooser fc;
            if (baseDir == null) {
                fc = new JFileChooser() {

                    @Override
                    protected JDialog createDialog(Component parent)
                            throws HeadlessException {
                        JDialog dialog = super.createDialog(parent);
                        // config here as needed - just to see a difference
                        dialog.setLocationByPlatform(true);
                        // might help - can't know because I can't reproduce the problem
                        dialog.setAlwaysOnTop(true);
                        return dialog;
                    }

                };
            } else {
                fc = new JFileChooser(baseDir) {

                    @Override
                    protected JDialog createDialog(Component parent)
                            throws HeadlessException {
                        JDialog dialog = super.createDialog(parent);
                        // config here as needed - just to see a difference
                        dialog.setLocationByPlatform(true);
                        // might help - can't know because I can't reproduce the problem
                        dialog.setAlwaysOnTop(true);
                        return dialog;
                    }

                };
            }
            fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            fc.setDialogType(JFileChooser.SAVE_DIALOG);
            fc.grabFocus();
            int resultVal = fc.showSaveDialog(null);
            if (resultVal != JFileChooser.APPROVE_OPTION) {
                return ("Selection not saved");
            }
            JsonArray emails = jList.getJsonArray("docIds");
            Set<String> hashes = new HashSet<>();
            HashMap<String, String> emailInfo = new HashMap<>();
            IndexReader reader;
            reader = DirectoryReader.open(FSDirectory.open(new File(baseDir, "index")));
            for (int i = 0; i < emails.size(); i++) {
                Document doc = reader.document(emails.getInt(i));
                if (!hashes.contains(doc.get("emailhash"))) {
                    hashes.add(doc.get("emailhash"));
                    String subject = doc.get("Subject");
                    if (subject == null || subject.length() < 1) {
                        subject = "No_Subject_" + Integer.toString(i);
                    }
                    if (subject.length() > 256) {
                        subject = subject.substring(0, 255);
                    }
                    emailInfo.put(doc.get("path"), subject);
                }
            }
            reader.close();
//            boolean merge = (jList.getJsonString("savemerge").equals("yes"));
            boolean merge = true;
            boolean headers = false;
            return (EmailContentServer.saveSelected(emailInfo, fc.getSelectedFile(), merge, headers));
        } catch (IOException ex) {
            Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ("Files not Saved");
    }
    public String saveJsonData(JsonObject jList) {
        try {
            JFileChooser fc;
            if (baseDir == null) {
                fc = new JFileChooser() {

                    @Override
                    protected JDialog createDialog(Component parent)
                            throws HeadlessException {
                        JDialog dialog = super.createDialog(parent);
                        // config here as needed - just to see a difference
                        dialog.setLocationByPlatform(true);
                        // might help - can't know because I can't reproduce the problem
                        dialog.setAlwaysOnTop(true);
                        return dialog;
                    }

                };
            } else {
                fc = new JFileChooser(baseDir) {

                    @Override
                    protected JDialog createDialog(Component parent)
                            throws HeadlessException {
                        JDialog dialog = super.createDialog(parent);
                        // config here as needed - just to see a difference
                        dialog.setLocationByPlatform(true);
                        // might help - can't know because I can't reproduce the problem
                        dialog.setAlwaysOnTop(true);
                        return dialog;
                    }

                };
            }
            fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            fc.setDialogType(JFileChooser.SAVE_DIALOG);
            fc.grabFocus();
            int resultVal = fc.showSaveDialog(null);
            if (resultVal != JFileChooser.APPROVE_OPTION) {
                return ("Selection not saved");
            }
            OutputStream sout = new FileOutputStream(fc.getSelectedFile());
            JsonWriterFactory jwf = Json.createWriterFactory(jMap);
            JsonWriter jsonWriter = jwf.createWriter(sout);
            jsonWriter.writeObject(jList);
            jsonWriter.close();
            return (fc.getSelectedFile().toString());
        } catch (IOException ex) {
            Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ("Files not Saved");
    }

    public File getNewBase() {

        JFileChooser fc;
        if (baseDir == null) {
            fc = new JFileChooser() {

                @Override
                protected JDialog createDialog(Component parent)
                        throws HeadlessException {
                    JDialog dialog = super.createDialog(parent);
                    // config here as needed - just to see a difference
                    dialog.setLocationByPlatform(true);
                    // might help - can't know because I can't reproduce the problem
                    dialog.setAlwaysOnTop(true);
                    return dialog;
                }

            };
        } else {
            fc = new JFileChooser(baseDir) {

                @Override
                protected JDialog createDialog(Component parent)
                        throws HeadlessException {
                    JDialog dialog = super.createDialog(parent);
                    // config here as needed - just to see a difference
                    dialog.setLocationByPlatform(true);
                    // might help - can't know because I can't reproduce the problem
                    dialog.setAlwaysOnTop(true);
                    return dialog;
                }

            };
        }
        fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        fc.setDialogType(JFileChooser.SAVE_DIALOG);
        fc.grabFocus();
        int resultVal = fc.showSaveDialog(null);
        if (resultVal == JFileChooser.APPROVE_OPTION) {
            baseDir = fc.getSelectedFile().toString();
        }
        return (new File(baseDir));
    }
}

