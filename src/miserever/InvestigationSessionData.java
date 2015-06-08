/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.BitSet;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import static miserever.MISEREVER.baseDirectory;

/**
 *
 * @author jkoven
 */
public class InvestigationSessionData {

    public HashMap<String, SearchCollector> topLinkEmails = new HashMap<>();
    public HashMap<Integer, Long> timeMap = new HashMap<>();
    public BitSet timeMapped = new BitSet();
    public Map<Long, Integer> weekMap = new HashMap<>();
    public int oldestYear;
    public int newestYear;
    public long oldest;
    public long newest;
    public String indexDirectory;
    public String dataDirectory;

    public InvestigationSessionData() {
    }

    public InvestigationSessionData(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        newest = 0;
        oldest = Long.MAX_VALUE;
        newestYear = 0;
        oldestYear = Integer.MAX_VALUE;
        timeMap.clear();
        timeMapped.clear();
        weekMap.clear();
    }

    public InvestigationSessionData(String indexDirectory, String dataDirectory) {
        this.indexDirectory = indexDirectory;
        this.dataDirectory = dataDirectory;
    }

    public String getNextSessionId() {
        BufferedReader idReader = null;
        FileWriter idWriter = null;
        Integer nextId = null;
        try {
            idReader = new BufferedReader(new FileReader(new File(dataDirectory + "lastId.txt")));
            nextId = Integer.parseInt(idReader.readLine().trim());
            idReader.close();
            FileWriter idOut = new FileWriter(new File(dataDirectory + "lastId.txt"));
            nextId++;
            idOut.write(nextId.toString() + "\n");
            idOut.close();
          } catch (FileNotFoundException ex) {
            Logger.getLogger(InvestigationSessionData.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(InvestigationSessionData.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                idReader.close();
            } catch (IOException ex) {
                Logger.getLogger(InvestigationSessionData.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return ("session_" + nextId.toString());
    }

    public void save(String sessionId) {

    }

    public void load(String sessionId) {

    }
}
