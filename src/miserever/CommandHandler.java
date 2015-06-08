/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.Socket;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.store.FSDirectory;
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
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Version;

/**
 *
 * @author jkoven
 */
public class CommandHandler implements Runnable {

    public Socket client;
    public File baseDirectory;
    public PrintWriter out;
    public BufferedReader in;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    public static Set<String> excludeSet = new HashSet<>();
    public static HashMap<String, JsonArray> userLinks = new HashMap<>();
    public static TermData[] topUsers = null;
    public static JsonArray topUsersList = null;
    public static HashMap<String, SearchCollector> topLinkEmails = new HashMap<>();
    private static HashMap<Integer, Long> timeMap = new HashMap<>();
    private static BitSet timeMapped = new BitSet();
    private static Calendar cal = new GregorianCalendar();
    private static Map<Long, Integer> weekMap = new HashMap<>();
    private static int oldestYear;
    private static int newestYear;
    private static long oldest;
    private static long newest;
    public static HashMap jMap = new HashMap();

    public CommandHandler(Socket client, File baseDirectory) throws Exception {
        this.client = client;
        this.baseDirectory = baseDirectory;
        this.out = new PrintWriter(this.client.getOutputStream(), true);
        this.in = new BufferedReader(
                new InputStreamReader(this.client.getInputStream()));
        if (!jMap.containsKey(JsonGenerator.PRETTY_PRINTING)) {
            jMap.put(JsonGenerator.PRETTY_PRINTING, true);
        }
    }

    @Override
    public void run() {
        try {
            String inputLine;
            char[] inString = null;
            String cmd = "";
            String cmdLine = "";
            String cmdL = "";
            JsonReaderFactory rFactory = Json.createReaderFactory(new HashMap<String, String>());
            JsonObject jsonIn = null;
            int contentLength = 0;
            FileUtils fileUtils = new FileUtils(baseDirectory.toString());
            // Initiate conversation with client
            while ((inputLine = this.in.readLine()) != null) {
//                System.out.println(inputLine);
                if (inputLine.toLowerCase().startsWith("get")
                        || inputLine.toLowerCase().startsWith("post")
                        || inputLine.toLowerCase().startsWith("options")) {
                    cmdLine = inputLine;
                }
                if (inputLine.toLowerCase().contains("content-length")) {
                    contentLength = Integer.parseInt(inputLine.substring(inputLine.indexOf(":") + 1).trim());
//                    System.out.println(contentLength);
                }
                if (inputLine.equals("")) {
                    if (!cmdLine.toLowerCase().contains("post")
                            && !cmdLine.toLowerCase().contains("options")) {
                        break;
                    }
                    if (cmdLine.contains("OPTIONS")) {
                        this.out.println("HTTP/1.0 200 OK");
                        this.out.println("Connection: keep-alive");
                        this.out.println("Access-Control-Allow-Origin: *");
                        this.out.println("Access-Control-Allow-Methods: POST, GET, OPTIONS");
                        this.out.println("Access-Control-Allow-Headers: content-type");
                        this.out.println("Content-Length: 0");
                        this.out.println("Accept: application/json,*/*");
                        this.out.println();
                    }
                    if (cmdLine.toLowerCase().contains("post") && contentLength > 0) {
                        inString = new char[contentLength];
                        this.in.read(inString, 0, contentLength);
                        break;
                    }
                }
            }
            if (cmdLine != null) {
                cmdLine = cmdLine.toLowerCase();
                if (cmdLine.contains("get")) {
                    for (String s : cmdLine.split(" ")) {
                        if (s.startsWith("/?")) {
                            cmdL = s.substring(2);
                            cmd = "GET";
                        }
                    }
                }
                if (cmdLine.contains("post")) {
                    JsonReader jr = rFactory.createReader(new StringReader(new String(inString)));
                    jsonIn = jr.readObject();
                    cmd = jsonIn.getString("command");
                    jr.close();
                    cmdL = "POST";
                }
            }
            if (cmdL != null) {
//                System.out.println(cmd);
                switch (cmd) {
                    case "topusers": {
//                        System.out.println(jsonIn.get("reset"));
                        boolean newList = jsonIn.getString("reset").equals("true");
                        JsonArray exclude = jsonIn.getJsonArray("excludeList");
//                        boolean newList = true;
//                        String[] exclude = null;
//                        if (parts.length > 0) {
//                            if (parts[1].equals("false")) {
//                                newList = false;
//                                if (parts.length > 1) {
//                                    exclude = Arrays.copyOfRange(parts, 2, parts.length);
//                                }
//                            }
//                        }
                        returnTopUsers(newList, exclude);
                        break;
                    }
                    case "topsenders": {
                        returnTopSenders();
                        break;
                    }
                    case "topreceivers": {
                        returnTopReceivers();
                        break;
                    }
                    case "toplinks": {
                        returnTopLinks(jsonIn.getString("user"));
                        break;
                    }
                    case "subjectsor": {
                        returnSubjects(jsonIn.getJsonArray("userList"), false, false);
                        break;
                    }
                    case "subjectsand": {
                        returnSubjects(jsonIn.getJsonArray("userList"), true, false);
                        break;
                    }
                    case "subjectsorterms": {
                        returnSubjects(jsonIn.getJsonArray("userList"), false, true);
                        break;
                    }
                    case "subjectsandterms": {
                        returnSubjects(jsonIn.getJsonArray("userList"), true, true);
                        break;
                    }
                    case "timedata": {
                        returnTimeData();
                        break;
                    }
                    case "emaillist": {
                        returnEmails(jsonIn.getJsonArray("emailList"));
                        break;
                    }
                    case "savefile": {
                        fileUtils.save(jsonIn);
                        String cText = "File Saved to " + jsonIn.getString("filename");
                        this.out.println("HTTP/1.0 200 OK");
                        this.out.println("Access-Control-Allow-Origin: *");
                        this.out.println("Content-Length: " + cText.length());
                        this.out.println("Content-Type: text/plain\n");
                        this.out.println(cText);
                        this.client.close();
                        break;
                    }
                    case "filelist": {
                        JsonObject fileNames = fileUtils.getFileList();
                        OutputStream sout = new ByteArrayOutputStream();
                        JsonWriterFactory jwf = Json.createWriterFactory(jMap);
                        JsonWriter jsonWriter = jwf.createWriter(sout);
                        jsonWriter.writeObject(fileNames);
                        jsonWriter.close();
                        String outString = sout.toString();
                        this.out.println("HTTP/1.0 200 OK");
                        this.out.println("Access-Control-Allow-Origin: *");
                        this.out.println("Content-Length: " + outString.length());
                        this.out.println("Content-Type: text/xml");
                        this.out.println("Mime-Type: application/json");
                        this.out.println();
                        this.out.println(outString);
                        this.client.close();
                        break;
                    }
                    case "getfile": {
                        JsonObject fileData = fileUtils.getFile(jsonIn.getString("filename"));
                        OutputStream sout = new ByteArrayOutputStream();
                        JsonWriterFactory jwf = Json.createWriterFactory(jMap);
                        JsonWriter jsonWriter = jwf.createWriter(sout);
                        jsonWriter.writeObject(fileData);
                        jsonWriter.close();
                        String outString = sout.toString();
                        this.out.println("HTTP/1.0 200 OK");
                        this.out.println("Access-Control-Allow-Origin: *");
                        this.out.println("Content-Length: " + outString.length());
                        this.out.println("Content-Type: text/xml");
                        this.out.println("Mime-Type: application/json");
                        this.out.println();
                        this.out.println(outString);
                        this.client.close();
                        break;
                    }
                    default: {
                        String outString = "Invalid Request";
                        this.out.println("HTTP/1.0 404 Not Found");
                        this.out.println("Access-Control-Allow-Origin: *");
                        this.out.println("Content-Length: " + outString.length());
                        this.out.println("Content-Type: text/plain");
                        this.out.println();
                        this.out.println(outString);
                        this.client.close();
                        break;
                    }
                }
            }
//            System.out.println(cmdLine + " " + cmd);
//            client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void returnTopSenders() {
        try {
            String outString = "name,count\n";
            DirectoryReader reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
            TermStats[] terms = HighFreqTerms.getHighFreqTerms(reader, 200, "From", new HighFreqTerms.TotalTermFreqComparator());
            // First add a node for each term in the set
            for (TermStats term : terms) {
                String sterm = term.termtext.utf8ToString().replaceAll("\\.", "");
                if (!sterm.contains("@")) {
                    continue;
                }
                outString += sterm + "," + term.docFreq + "\n";
            }
            reader.close();
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + outString.length());
            this.out.println("Content-Type: text/csv\n");
            this.out.println(outString);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void returnTopLinks(String user) {
        try {
            String outString = "name,count\n";
            SearchCollector sc = new SearchCollector();
            sc.masterIndexPath = new File(baseDirectory, "index").toString();
            sc.getMasterFileList(new String[]{"To", "From", "Cc", "Bcc"}, user, sc, CharArraySet.EMPTY_SET);
            ArrayList<ScTermData> toContacts = sc.getHighFreqTerms("To", 2000, new BitSet(), CharArraySet.EMPTY_SET);
            ArrayList<ScTermData> fromContacts = sc.getHighFreqTerms("From", 2000, new BitSet(), CharArraySet.EMPTY_SET);
            ArrayList<ScTermData> ccContacts = sc.getHighFreqTerms("Cc", 2000, new BitSet(), CharArraySet.EMPTY_SET);
            ArrayList<ScTermData> bccContacts = sc.getHighFreqTerms("Bcc", 2000, new BitSet(), CharArraySet.EMPTY_SET);
            HashMap<String, Long> termMap = new HashMap<>();
            for (ScTermData term : toContacts) {
                if (!term.term.contains("@") || term.term.equals(user)) {
                    continue;
                }
                if (termMap.containsKey(term.term)) {
                    termMap.put(term.term,
                            termMap.get(term.term) + term.freq);
                } else {
                    termMap.put(term.term, term.freq);
                }
            }
            for (ScTermData term : fromContacts) {
                if (!term.term.contains("@") || term.term.equals(user)) {
                    continue;
                }
                if (termMap.containsKey(term.term)) {
                    termMap.put(term.term,
                            termMap.get(term.term) + term.freq);
                } else {
                    termMap.put(term.term, term.freq);
                }
            }
            for (ScTermData term : ccContacts) {
                if (!term.term.contains("@") || term.term.equals(user)) {
                    continue;
                }
                if (termMap.containsKey(term.term)) {
                    termMap.put(term.term,
                            termMap.get(term.term) + term.freq);
                } else {
                    termMap.put(term.term, term.freq);
                }
            }
            for (ScTermData term : bccContacts) {
                if (!term.term.contains("@") || term.term.equals(user)) {
                    continue;
                }
                if (termMap.containsKey(term.term)) {
                    termMap.put(term.term,
                            termMap.get(term.term) + term.freq);
                } else {
                    termMap.put(term.term, term.freq);
                }
            }
            TermData[] terms = new TermData[termMap.size()];
            int ix = 0;
            for (String key : termMap.keySet()) {
                terms[ix++] = new TermData(key, termMap.get(key));
            }
            Arrays.sort(terms, Collections.reverseOrder());
            for (TermData term : terms) {
                outString += term.term + "," + term.freq + "\n";
            }
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + outString.length());
            this.out.println("Content-Type: text/csv\n");
            this.out.println(outString);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public JsonArray getTopLinks(String user) {
        if (!userLinks.keySet().contains(user)) {
//            System.out.println("Start: " + System.currentTimeMillis());
//            SearchCollector sc = new SearchCollector();
//            sc.masterIndexPath = new File(baseDirectory, "index").toString();
//            String[] fieldList = new String[]{"To", "From", "Cc", "Bcc"};
//            sc.getMasterFileList(fieldList, user, sc, CharArraySet.EMPTY_SET);
//            if (!topLinkEmails.containsKey(user)) {
//                topLinkEmails.put(user, sc);
//            }
            SearchCollector sc;
            String[] fieldList = new String[]{"To", "From", "Cc", "Bcc"};
            if (!topLinkEmails.containsKey(user)) {
                sc = new SearchCollector();
                sc.masterIndexPath = new File(baseDirectory, "index").toString();
                sc.getMasterFileList(fieldList, user, sc, CharArraySet.EMPTY_SET);
                topLinkEmails.put(user, sc);
            } else {
                sc = topLinkEmails.get(user);
            }
//            System.out.println("Going to Time Map: " + System.currentTimeMillis());
            updateTimeMap(sc.docIds);
//            System.out.println("Back From Time Map: " + System.currentTimeMillis());
            ArrayList<ArrayList<ScTermData>> contactList = new ArrayList<>();
            for (int i = 0; i < fieldList.length; i++) {
                contactList.add(sc.getHighFreqTerms(fieldList[i], 1000, new BitSet(), CharArraySet.EMPTY_SET));
            }
            HashMap<String, Long> termMap = new HashMap<>();
            HashMap<String, BitSet> emails = new HashMap<>();
            for (ArrayList<ScTermData> contacts : contactList) {
                for (ScTermData term : contacts) {
                    if (!term.term.contains("@") || term.term.equals(user)) {
                        continue;
                    }
                    if (termMap.containsKey(term.term)) {
                        termMap.put(term.term,
                                termMap.get(term.term) + term.freq);
                        emails.get(term.term).or(term.emails);
                    } else {
                        termMap.put(term.term, term.freq);
                        emails.put(term.term, term.emails);
                    }
                }
            }
//            System.out.println("Finished Building contact list: " + System.currentTimeMillis());
            TermData[] terms = new TermData[termMap.size()];
            int ix = 0;
            for (String key : termMap.keySet()) {
                terms[ix++] = new TermData(key, termMap.get(key));
            }
            Arrays.sort(terms, Collections.reverseOrder());
            JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
            JsonArrayBuilder jLinks = jFactory.createArrayBuilder();
            for (TermData term : terms) {
//            System.out.println(term.term + " " + term.freq);
                jLinks.add(jFactory.createObjectBuilder().add("link", term.term)
                        .add("count", term.freq)
                        .add("dates", getDateList(emails.get(term.term))).build());
            }
            userLinks.put(user, jLinks.build());
        }
//        System.out.println("Finished Building Json: " + System.currentTimeMillis());
//        System.out.println();
//        System.out.println(userLinks.get(user));
//        System.out.println(oldestYear + " " + newestYear);
        return (userLinks.get(user));
    }

    public void returnTopReceivers() {
        try {
            String outString = "name,count\n";
            DirectoryReader reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
            TermStats[] toterms = HighFreqTerms.getHighFreqTerms(reader, 200, "To", new HighFreqTerms.TotalTermFreqComparator());
            TermStats[] ccterms = HighFreqTerms.getHighFreqTerms(reader, 200, "Cc", new HighFreqTerms.TotalTermFreqComparator());
            TermStats[] bccterms = HighFreqTerms.getHighFreqTerms(reader, 200, "Bcc", new HighFreqTerms.TotalTermFreqComparator());
            // First Merge the lists and resort
            HashMap<String, Integer> termMap = new HashMap<>();
            for (TermStats term : toterms) {
                String sterm = term.termtext.utf8ToString().replaceAll("\\.", "");
                if (!sterm.contains("@")) {
                    continue;
                }
                if (termMap.containsKey(sterm)) {
                    termMap.put(sterm,
                            termMap.get(sterm) + term.docFreq);
                } else {
                    termMap.put(sterm, term.docFreq);
                }
            }
            for (TermStats term : ccterms) {
                String sterm = term.termtext.utf8ToString().replaceAll("\\.", "");
                if (!sterm.contains("@")) {
                    continue;
                }
                if (termMap.containsKey(sterm)) {
                    termMap.put(sterm,
                            termMap.get(sterm) + term.docFreq);
                } else {
                    termMap.put(sterm, term.docFreq);
                }
            }
            for (TermStats term : bccterms) {
                String sterm = term.termtext.utf8ToString().replaceAll("\\.", "");
                if (!sterm.contains("@")) {
                    continue;
                }
                if (termMap.containsKey(sterm)) {
                    termMap.put(sterm,
                            termMap.get(sterm) + term.docFreq);
                } else {
                    termMap.put(sterm, term.docFreq);
                }
            }
            TermData[] terms = new TermData[termMap.size()];
            int ix = 0;
            for (String key : termMap.keySet()) {
                terms[ix++] = new TermData(key, termMap.get(key));
            }
            Arrays.sort(terms, Collections.reverseOrder());
            for (TermData term : terms) {
                outString += term.term + "," + term.freq + "\n";
            }
            reader.close();
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + outString.length());
            this.out.println("Content-Type: text/csv\n");
            this.out.println(outString);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void returnTopUsers(boolean newSession, JsonArray exclude) {
//        System.out.println(newSession);
        try {
            SearchCollector.clearCurrentIndexedEmails();
            newest = 0;
            oldest = Long.MAX_VALUE;
            newestYear = 0;
            oldestYear = Integer.MAX_VALUE;
            timeMap.clear();
            timeMapped.clear();
            weekMap.clear();
            if (newSession || topUsersList == null) {
                excludeSet.clear();
                userLinks.clear();
                topLinkEmails.clear();
                DirectoryReader reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
                TermStats[] fromterms = HighFreqTerms.getHighFreqTerms(reader, 200, "From", new HighFreqTerms.TotalTermFreqComparator());
                TermStats[] toterms = HighFreqTerms.getHighFreqTerms(reader, 200, "To", new HighFreqTerms.TotalTermFreqComparator());
                TermStats[] ccterms = HighFreqTerms.getHighFreqTerms(reader, 200, "Cc", new HighFreqTerms.TotalTermFreqComparator());
                TermStats[] bccterms = HighFreqTerms.getHighFreqTerms(reader, 200, "Bcc", new HighFreqTerms.TotalTermFreqComparator());
                // First Merge the lists and resort
                HashMap<String, Integer> termMap = new HashMap<>();
                for (TermStats term : fromterms) {
                    String sterm = term.termtext.utf8ToString();
                    if (!sterm.contains("@")) {
                        continue;
                    }
                    if (termMap.containsKey(sterm)) {
                        termMap.put(sterm,
                                termMap.get(sterm) + term.docFreq);
                    } else {
                        termMap.put(sterm, term.docFreq);
                    }
                }
                for (TermStats term : toterms) {
                    String sterm = term.termtext.utf8ToString();
                    if (!sterm.contains("@")) {
                        continue;
                    }
                    if (termMap.containsKey(sterm)) {
                        termMap.put(sterm,
                                termMap.get(sterm) + term.docFreq);
                    } else {
                        termMap.put(sterm, term.docFreq);
                    }
                }
                for (TermStats term : ccterms) {
                    String sterm = term.termtext.utf8ToString();
                    if (!sterm.contains("@")) {
                        continue;
                    }
                    if (termMap.containsKey(sterm)) {
                        termMap.put(sterm,
                                termMap.get(sterm) + term.docFreq);
                    } else {
                        termMap.put(sterm, term.docFreq);
                    }
                }
                for (TermStats term : bccterms) {
                    String sterm = term.termtext.utf8ToString();
                    if (!sterm.contains("@")) {
                        continue;
                    }
                    if (termMap.containsKey(sterm)) {
                        termMap.put(sterm,
                                termMap.get(sterm) + term.docFreq);
                    } else {
                        termMap.put(sterm, term.docFreq);
                    }
                }
                TermData[] terms = new TermData[termMap.size()];
                int ix = 0;
                for (String key : termMap.keySet()) {
                    terms[ix++] = new TermData(key, termMap.get(key));
                }
                Arrays.sort(terms, Collections.reverseOrder());
                topUsers = terms;
                reader.close();
            } else {
                if (!newSession) {
                    for (int i = 0; i < exclude.size(); i++) {
                        excludeSet.add(exclude.getString(i));
                    }
//                    Collections.addAll(excludeSet, exclude);
                    for (String key : excludeSet) {
                        try {
                            topLinkEmails.remove(key);
                        } catch (Exception e) {
                            continue;
                        }
                    }
                }
            }
            JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
            JsonArrayBuilder jUsers = jFactory.createArrayBuilder();
            int count = 0;
            for (TermData term : topUsers) {
                if (!excludeSet.contains(term.term)) {
                    jUsers.add(jFactory.createObjectBuilder().add("name", term.term)
                            .add("count", term.freq)
                            .add("links", getTopLinks(term.term)).build());
                    if (count++ >= 100) {
                        break;
                    }
                }
            }
            topUsersList = jUsers.build();
            OutputStream sout = new ByteArrayOutputStream();
            JsonWriterFactory jwf = Json.createWriterFactory(jMap);
            JsonWriter jsonWriter = jwf.createWriter(sout);
            jsonWriter.writeArray(topUsersList);
            jsonWriter.close();
//            System.out.println(sout.toString());
            String outString = sout.toString();
//            System.out.println();
//            System.out.println(outString.length());
//            System.out.println(outString.length());
//            System.out.println(outString);
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + outString.length());
            this.out.println("Content-Type: text/xml");
            this.out.println("Mime-Type: application/json");
            this.out.println(outString);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void returnSubjects(JsonArray users, boolean all, boolean terms) {
        System.out.println("Start: " + System.currentTimeMillis());
        SearchCollector[] scList = new SearchCollector[users.size()];
        BitSet emails = new BitSet();
        try {
//            String outString = "";
            for (int i = 0; i < users.size(); i++) {
                String user = users.getString(i);
                scList[i] = topLinkEmails.get(user);
                if (i == 0 || !all) {
//                    System.out.println("or");
//                    System.out.println(scList[i-1].docIds);
                    emails.or(scList[i].docIds);
                } else {
//                    System.out.println("and");
//                    System.out.println(scList[i-1].docIds);
                    emails.and(scList[i].docIds);
                }
            }
//            System.out.println(emails.cardinality());
            if (terms) {
                SearchCollector emailsc = new SearchCollector();
                emailsc.masterIndexPath = new File(baseDirectory, "index").toString();
                emailsc.docIds = emails;
//                System.out.println(emailsc.docIds.cardinality());
                ArrayList<ScTermData> rTerms = emailsc.getHighFreqTerms("noun", 500, new BitSet(),
                        new CharArraySet(Version.LUCENE_40, 10, true));
//                System.out.println(rTerms.size());
                for (ScTermData t : rTerms) {
//                    outString += (t.term + "," + t.freq.toString() + "\n");
                }
            } else {
                IndexReader reader;
                reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
                IndexSearcher searcher = new IndexSearcher(reader);
                int nextId = 0;
                Map<String, Pair<String, List<Integer>>> subjectMap = new HashMap<>();
                while (nextId < reader.maxDoc()) {
                    nextId = emails.nextSetBit(nextId);
                    if (nextId == -1) {
                        break;
                    }
                    Document doc = searcher.doc(nextId);
                    if (doc.get("Subject") != null) {
                        String subject = StringEscapeUtils.escapeJson(doc.get("Subject"));
                        String key = doc.get("subject_hash").toString();
                        if (subjectMap.containsKey(key)) {
                            Pair<String, List<Integer>> thisSubject = subjectMap.get(key);
                            thisSubject.value.add(nextId);
                            subjectMap.put(key, thisSubject);
                        } else {
                            List<Integer> newList = new ArrayList<>();
                            newList.add(nextId);
                            subjectMap.put(key, new Pair<String, List<Integer>>(subject, newList));
                        }
                    }
                    nextId++;
                }
                reader.close();
                JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
                JsonArrayBuilder jSubjects = jFactory.createArrayBuilder();
                for (String key : subjectMap.keySet()) {
                    String subject = subjectMap.get(key).key;
                    List<Integer> idList = subjectMap.get(key).value;
                    JsonArrayBuilder jdocIdList = jFactory.createArrayBuilder();
                    for (int id : idList) {
                        jdocIdList.add(id);
                    }
                    jSubjects.add(jFactory.createObjectBuilder().add("subject", subject)
                            .add("count", idList.size())
                            .add("docidlist", jdocIdList.build()).build());
//                System.out.println(new Date(key).toString() + ": " + weekMap.get(key));
                }
                OutputStream sout = new ByteArrayOutputStream();
                JsonWriterFactory jwf = Json.createWriterFactory(jMap);
                JsonWriter jsonWriter = jwf.createWriter(sout);
                jsonWriter.writeArray(jSubjects.build());
                jsonWriter.close();
                String outString = sout.toString();
                sout.close();
//                System.out.println(outString);
                this.out.println("HTTP/1.0 200 OK");
                this.out.println("Access-Control-Allow-Origin: *");
                this.out.println("Content-Length: " + outString.length());
                this.out.println("Content-Type: text/plain");
                this.out.println("Mime-Type: application/json");
                this.out.println(outString);
                System.out.println("Returned: " + System.currentTimeMillis());
            }
            this.client.close();
//            System.out.println(outString);
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void updateTimeMap(BitSet newEmails) {
        try {
            IndexReader reader;
            reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
            for (int i = newEmails.nextSetBit(0); i >= 0;
                    i = newEmails.nextSetBit(i + 1)) {
                if (!timeMapped.get(i)) {
                    Document doc = reader.document(i);
                    if (doc.get("Date") != null) {
                        timeMap.put(i, Long.parseLong(doc.get("Date")));
                        cal.setTimeInMillis(Long.parseLong(doc.get("Date")));
//                        if (cal.get(Calendar.YEAR) == 2013) {
//                            System.out.println(doc.get("path").toString());
//                        }
                    } else {
                        timeMap.put(i, null);
                    }
                    timeMapped.set(i);
                }
            }
            reader.close();
        } catch (IOException ex) {
            System.err.println(CommandHandler.class
                    .getName().toString() + " " + ex);
        }
        oldest = Long.MAX_VALUE;
        newest = 0;
        for (int i = timeMapped.nextSetBit(0); i >= 0;
                i = timeMapped.nextSetBit(i + 1)) {
            if (timeMap.get(i) != null) {
                if (timeMap.get(i) < oldest) {
                    oldest = timeMap.get(i);
                    cal.setTimeInMillis(oldest);
                    oldestYear = cal.get(Calendar.YEAR);
                }
                if (timeMap.get(i) > newest) {
                    newest = timeMap.get(i);
                    cal.setTimeInMillis(newest);
                    newestYear = cal.get(Calendar.YEAR);
                }
            }
        }
    }

    public JsonArray getDateList(BitSet emails) {
        HashMap<Long, Integer> dateMap = new HashMap<>();
        for (int i = emails.nextSetBit(0); i >= 0; i = emails.nextSetBit(i + 1)) {
            if (timeMap.get(i) != null) {
                cal.setTimeInMillis(timeMap.get(i));
                int year = cal.getWeekYear();
                int week = cal.get(Calendar.WEEK_OF_YEAR);
                cal.clear();
                cal.setWeekDate(year, week, 1);
                long bucket = cal.getTimeInMillis();
                if (dateMap.containsKey(bucket)) {
                    dateMap.put(bucket, dateMap.get(bucket) + 1);
                } else {
                    dateMap.put(bucket, 1);
                }
            }
        }
        JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
        JsonArrayBuilder jDates = jFactory.createArrayBuilder();
        for (long bucket : dateMap.keySet()) {
//            jDates.add(jFactory.createObjectBuilder().add(date, dateMap.get(date)).build());
            jDates.add(jFactory.createObjectBuilder().add("weekdate", bucket)
                    .add("count", dateMap.get(bucket)).build());
        }
        return (jDates.build());
    }

    public void returnTimeData() {
        try {
            int minCount = Integer.MAX_VALUE;
            int maxCount = 0;
            for (int i = timeMapped.nextSetBit(0); i >= 0;
                    i = timeMapped.nextSetBit(i + 1)) {
                if (timeMap.get(i) != null) {
                    cal.setTimeInMillis(timeMap.get(i));
                    int year = cal.getWeekYear();
                    int week = cal.get(Calendar.WEEK_OF_YEAR);
                    cal.clear();
                    cal.setWeekDate(year, week, 1);
                    long bucket = cal.getTimeInMillis();
                    if (weekMap.containsKey(bucket)) {
                        weekMap.put(bucket, weekMap.get(bucket) + 1);
                    } else {
                        weekMap.put(bucket, 1);
                    }
                    maxCount = (weekMap.get(bucket) > maxCount) ? weekMap.get(bucket) : maxCount;
                    minCount = (weekMap.get(bucket) < minCount) ? weekMap.get(bucket) : minCount;
                }
            }
            JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
            JsonArrayBuilder jBuckets = jFactory.createArrayBuilder();
            int count = 0;
            for (Long key : weekMap.keySet()) {
                jBuckets.add(jFactory.createObjectBuilder().add("weekdate", key)
                        .add("count", weekMap.get(key)).build());
//                System.out.println(new Date(key).toString() + ": " + weekMap.get(key));
            }
            JsonObjectBuilder td = jFactory.createObjectBuilder()
                    .add("mindate", oldest)
                    .add("maxdate", newest)
                    .add("mincount", minCount)
                    .add("maxcount", maxCount)
                    .add("buckets", jBuckets.build());
            OutputStream sout = new ByteArrayOutputStream();
            JsonWriterFactory jwf = Json.createWriterFactory(jMap);
            JsonWriter jsonWriter = jwf.createWriter(sout);
            jsonWriter.writeObject(td.build());
            jsonWriter.close();
//            System.out.println(sout.toString());
            String outString = sout.toString();
//            System.out.println();
//            System.out.println(outString.length());
//            System.out.println(outString.length());
//            System.out.println(outString);
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + outString.length());
            this.out.println("Content-Type: text/xml");
            this.out.println("Mime-Type: application/json");
            this.out.println(outString);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void returnEmails(JsonArray emails) {
        try {
            String[] pathList = new String[emails.size()];
            IndexReader reader;
            reader = DirectoryReader.open(FSDirectory.open(new File(baseDirectory, "index")));
            for (int i = 0; i < emails.size(); i++) {
                Document doc = reader.document(Integer.parseInt(emails.getString(i)));
                pathList[i] = doc.get("path");
            }
            reader.close();
            String emailContent = EmailContentServer.getEmails(pathList);
            this.out.println("HTTP/1.0 200 OK");
            this.out.println("Access-Control-Allow-Origin: *");
            this.out.println("Content-Length: " + emailContent.length());
            this.out.println("Content-Type: text/plain\n");
            this.out.println(emailContent);
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(CommandHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
