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
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import static org.apache.lucene.queryparser.classic.QueryParserBase.OR_OPERATOR;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/**
 *
 * @author jkoven
 */
public class SearchCollector extends Collector {

    public int docBase;
    public BitSet docIds;
    public IndexReader subIndex;
    public IndexReader masterIndexReader;
    public String subIndexPath;
    public String masterIndexPath;
    public HashMap<Integer, Integer> indexMap;
    public static BitSet currentIndexedEmails = new BitSet();
    public static HashMap<String, HashMap<String, ScTermData>> termMaps = new HashMap<>();
    private static HashMap<String, BitSet> mappedEmails = new HashMap<>();
    private static HashMap<String, Long> totalFreqs = new HashMap<>();
    private static HashMap<Integer, Double> emailWeights;
    public static double tfHigh = 0;
    public static double tfLow = Double.MAX_VALUE;
    public static CharArraySet stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
    public static File sourceFile;

    public SearchCollector(int size) {
        sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        docIds = new BitSet(size);
        docBase = 0;
        indexMap = new HashMap<Integer, Integer>();
        subIndexPath = "";
        masterIndexPath = "";
    }

    public SearchCollector() {
        sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        docIds = new BitSet();
        docBase = 0;
        indexMap = new HashMap<Integer, Integer>();
        subIndexPath = "";
        masterIndexPath = "";
    }

    public SearchCollector(String sip, String mip) {
        sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        indexMap = new HashMap<Integer, Integer>();
        BufferedReader inFile = null;
        try {
            docIds = new BitSet();
            docBase = 0;
            subIndexPath = sip;
            masterIndexPath = mip;
            stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            File file = new File(sourceFile.getParent(), "datafiles/stopwords_en.txt");
            inFile = new BufferedReader(new FileReader(file));
            String word;
            while ((word = inFile.readLine()) != null) {
                stopWords.add(word.trim());
            }
            inFile.close();
//            masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                inFile.close();
            } catch (IOException ex) {
                System.err.println(SearchCollector.class.getName() + " " + ex);
            }
        }
    }

    @Override
    // We don't car about order
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    // ignore scorer
    @Override
    public void setScorer(Scorer scorer) {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.docBase = context.docBase;
    }

    // The meat of this collector we take the incoming doc and stuff it into a
    // new index
    @Override
    public void collect(int doc) {
        docIds.set(doc + docBase);
    }

    // Create a new index from the ids collected.  We need the searcher so
    // we can find the actual docs
    public void createIndex(String sip, String mip) {
        // First create a new index in the directory passed and erase any exisiting
        // documents that might have been there.
        subIndexPath = sip;
        masterIndexPath = mip;
        indexMap = new HashMap<>();
        HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
        try {
            CharArraySet stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            File file = new File(sourceFile.getParent(), "datafiles/stopwords_en.txt");
            BufferedReader inFile = new BufferedReader(new FileReader(file));
            String word;
            while ((word = inFile.readLine()) != null) {
                stopWords.add(word.trim());
            }
            inFile.close();
            masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
            IndexSearcher searcher = new IndexSearcher(masterIndexReader);
            System.out.println("Indexing to directory '" + subIndexPath + "'...");
            Directory dir = FSDirectory.open(new File(subIndexPath));
            Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40, stopWords);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);

            // Create a new index in the directory, removing any
            // previously indexed documents:
            if (currentIndexedEmails.cardinality() == 0) {
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            } else {
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            }

            IndexWriter writer = new IndexWriter(dir, iwc);
            // Index the docs in the bitset
            int nextId = 0;
            while (nextId < docIds.length()) {
                nextId = docIds.nextSetBit(nextId);
                if (nextId == -1) {
                    break;
                }
                Document foundDoc = searcher.doc(nextId);
                tempMap.put(foundDoc.get("path"), nextId++);
//                System.out.println(nextId - 1 + ": " + foundDoc.get("path"));
                writer.addDocument(foundDoc);
            }
            writer.close();
            // Set up the mapping between sub index and master index
            IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(subIndexPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document doc = reader.document(i);
                indexMap.put(i, tempMap.get(doc.get("path")));
            }
//            System.out.println(reader.maxDoc());
//            for (Integer key : indexMap.keySet()) {
//                Document doc = reader.document(key);
//                System.out.println(key + "/ " + indexMap.get(key) + "/  " + doc.get("path"));
//            }
            reader.close();
        } catch (IOException e) {
            System.err.println(" caught a " + e.getClass()
                    + "\n with message: " + e.getMessage());
        }
        try {
            subIndex = DirectoryReader.open(FSDirectory.open(new File(subIndexPath)));
        } catch (IOException e) {
            System.err.println("Caught exception in build index " + e);
            subIndex = null;
        }
    }

    public IndexReader getIndex() {
        return (masterIndexReader);
    }

    public void getSubFileList(String[] fields, String term, SearchCollector sc) {
        // We search the subfiles but we need to convert to master file bits
        BitSet newDocIds = new BitSet();
        Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40);
        QueryParser parser = new QueryParser(Version.LUCENE_40, "contents", analyzer);
        String qString = "";
        for (String field : fields) {
            qString += field + ":\"" + term + "\" ";
        }
        try {
            Query q = parser.parse(qString);
            SearchFiles.getCollection(subIndex, q, sc);
            int nextId = 0;
//            System.out.println("entering");
            while (nextId < sc.docIds.length()) {
                nextId = sc.docIds.nextSetBit(nextId);
                if (nextId == -1) {
                    break;
                }
                newDocIds.set(indexMap.get(nextId++));
//                System.out.println(masterReader.document(hits[0].doc).get("uid") + " " + foundDoc.get("uid"));
            }
        } catch (Exception ex) {
            System.err.println(SearchCollector.class.getName() + " " + ex);
        }
        sc.docIds = newDocIds;
    }

    public void getMasterFileList(String[] fields, String term, SearchCollector sc, CharArraySet skipWords) {
        term = term.replace('/', '-');
        if (term.trim().equals("")) {
            return;
        }
        if (stopWords == null) {
            BufferedReader inFile = null;
            stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            try {
                File file = new File(sourceFile.getParent(), "datafiles/stopwords_en.txt");
                inFile = new BufferedReader(new FileReader(file));
                String word;
                while ((word = inFile.readLine()) != null) {
                    stopWords.add(word.trim());
                }
                inFile.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    inFile.close();
                } catch (IOException ex) {
                    Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        if (masterIndexReader == null) {
            try {
                masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
            } catch (IOException ex) {
                System.err.println(SearchCollector.class.getName() + " " + ex);
            }
        }
        Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40);
        QueryParser parser = new QueryParser(Version.LUCENE_40, "contents", analyzer);
        String qString = "";
        String[] terms = term.split(" ");
        if (terms.length < 1) {
            return;
        }
        int stopCount = 0;
        for (String t : terms) {
            if (stopWords.contains(t.trim().toCharArray()) || skipWords.contains(t.trim().toCharArray())) {
                stopCount++;
            }
        }
        if (stopCount == terms.length) {
            return;
        }
        for (String field : fields) {
            if (!qString.equals("")) {
                qString += " OR ";
            }
            qString += "(";
            int tcount = 0;
            for (int i = 0; i < terms.length; i++) {
                boolean isOr;
                if (terms[i].equalsIgnoreCase("or") && i + 1 < terms.length) {
                    isOr = true;
                    i++;
                } else if (terms[i].equalsIgnoreCase("and") && i + 1 < terms.length) {
                    isOr = false;
                    i++;
                } else {
                    isOr = false;
                }
                if (!stopWords.contains(terms[i].trim().toCharArray()) || !skipWords.contains(terms[i].trim().toCharArray())) {
                    if (tcount > 0) {
                        if (isOr) {
                            qString += " OR ";
                        } else {
                            qString += " AND ";
                        }
                    }
                    qString += field + ": " + terms[i] + " ";
                    tcount++;
                }
            }
            qString += ")";
        }
        try {
//            System.out.println(qString);
            Query q = parser.parse(qString);
//            System.out.println(q.toString());

            SearchFiles.getCollection(masterIndexReader, q, sc);
//            System.out.println(term + " " + sc.docIds.cardinality());
            masterIndexReader.close();
            masterIndexReader = null;
        } catch (Exception e) {
            System.err.println("Caught exception in getmaster " + e);
            e.printStackTrace();
        }
    }

    public void multiFieldSearch(String[] fields, String terms) {
        if (terms.trim().equals("")) {
            return;
        }
        if (stopWords == null) {
            BufferedReader inFile = null;
            stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            try {
                File file = new File(sourceFile.getParent(), "datafiles/stopwords_en.txt");
                inFile = new BufferedReader(new FileReader(file));
                String word;
                while ((word = inFile.readLine()) != null) {
                    stopWords.add(word.trim());
                }
                inFile.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    inFile.close();
                } catch (IOException ex) {
                    Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        if (masterIndexReader == null) {
            try {
                masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
            } catch (IOException ex) {
                System.err.println(SearchCollector.class.getName() + " " + ex);
            }
        }
        MultiFieldQueryParser mParser
                = new MultiFieldQueryParser(Version.LUCENE_40, fields,
                        new ClassicAnalyzer(Version.LUCENE_40, SearchCollector.stopWords));
        mParser.setDefaultOperator(OR_OPERATOR);
        try {
//            System.out.println(qString);
            Query q = mParser.parse(terms);
//            System.out.println(q.toString());

            SearchFiles.getCollection(masterIndexReader, q, this);
//            System.out.println(this.docIds.cardinality());
            masterIndexReader.close();
            masterIndexReader = null;
        } catch (Exception e) {
            System.err.println("Caught exception in getmaster " + e);
            e.printStackTrace();
        }
    }

//    public ArrayList<String> getTopTerms(String field, int numTerms) {
//        ArrayList<String> termList = new ArrayList<>();
//        TermStats[] terms;
//        try {
//            IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(subIndexPath)));
////            System.out.println(reader.maxDoc());
//            try {
//                //            terms = HighFreqTerms.getHighFreqTerms(reader, 50, "contents", new DocFreqComparator());
//                terms = HighFreqTerms.getHighFreqTerms(reader, numTerms, field, new HighFreqTerms.TotalTermFreqComparator());
//            } catch (Exception ex) {
//                terms = new TermStats[0];
//            }
//            reader.close();
//        } catch (IOException ex) {
//            terms = new TermStats[0];
//            System.err.println(SearchCollector.class.getName().toString() + " " + ex);
//        }
//        for (TermStats term : terms) {
//            if ((!field.contains("To") && !field.contains("From") && !field.contains("Cc"))
//                    || (term.termtext.utf8ToString().contains("@"))) {
//                termList.add(term.termtext.utf8ToString());
//            }
//        }
//        return (termList);
//    }
    public synchronized ArrayList<ScTermData> getHighFreqTerms(String field, int count, BitSet tagged,
            CharArraySet skipWords) {
        if (docIds.cardinality() == 0) {
            termMaps = new HashMap<>();
            mappedEmails = new HashMap<>();
            totalFreqs = new HashMap<>();
            return (new ArrayList<ScTermData>());
        }
        if (!termMaps.containsKey(field)) {
            termMaps.put(field, new HashMap<String, ScTermData>());
            mappedEmails.put(field, new BitSet());
            totalFreqs.put(field, new Long(0));
        }
        if (masterIndexReader == null) {
            try {
                masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
            } catch (IOException ex) {
                System.err.println(SearchCollector.class.getName() + " " + ex);
            }
        }
        if (stopWords == null) {
            BufferedReader inFile = null;
            stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            try {
                File file = new File(sourceFile.getParent(), "datafiles/stopwords_en.txt");
                inFile = new BufferedReader(new FileReader(file));
                String word;
                while ((word = inFile.readLine()) != null) {
                    stopWords.add(word.trim());
                }
                inFile.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    inFile.close();
                } catch (IOException ex) {
                    Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        HashMap<String, ScTermData> termMap = termMaps.get(field);
        BitSet currentMappedEmails = mappedEmails.get(field);
        long totalTermFreq = totalFreqs.get(field);
        IndexReader reader;
        ArrayList<ScTermData> termList = null;
        try {
            BitSet changedEmails = (BitSet) currentMappedEmails.clone();
            changedEmails.xor(docIds);
//            System.out.println("Start");
//            System.out.println(docIds.cardinality());
//            System.out.println(currentMappedEmails.cardinality());
//            System.out.println(changedEmails.cardinality());
            for (int i = changedEmails.nextSetBit(0); i >= 0;
                    i = changedEmails.nextSetBit(i + 1)) {
//            for (int i = docIds.nextSetBit(0); i >= 0;
//                    i = docIds.nextSetBit(i + 1)) {
                // operate on index i here
                if (masterIndexReader == null) {
                    try {
                        masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
                    } catch (IOException ex) {
                        System.err.println(SearchCollector.class.getName() + " " + ex);
                    }
                }
                Terms tv = masterIndexReader.getTermVector(i, field);
                if (tv == null) {
                    continue;
                }
                TermsEnum termsEnum = tv.iterator(null);
                BytesRef text;
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    if ((field.equals("To") || field.equals("From") || field.equals("Cc"))
                            && ((!term.contains("@")))) {
                        continue;
                    }
                    if (stopWords.contains(term.toCharArray())
                            || skipWords.contains(term.toCharArray())) {
                        continue;
                    }
                    long freq = termsEnum.totalTermFreq();
                    if (!currentMappedEmails.get(i) || (termMap.get(term) == null)) {
                        if (termMap.containsKey(term)) {
                            ScTermData t = termMap.get(term);
                            t.freq += freq;
                            totalTermFreq += freq;
                            t.totalDocFreq++;
                            t.emails.set(i);
                            termMap.put(term, t);
                            if (t.totalDocFreq == 0L) {
                                termMap.remove(term);
                            }
                        } else {
                            totalTermFreq += freq;
                            termMap.put(term, new ScTermData(term, freq, i));
                        }
                    } else {
                        ScTermData t = termMap.get(term);
                        t.freq -= freq;
                        totalTermFreq -= freq;
                        t.totalDocFreq--;
                        t.emails.clear(i);
                        termMap.put(term, t);
                        if (t.totalDocFreq == 0L) {
                            termMap.remove(term);
                        }
                    }
                }
            }
            termMaps.put(field, termMap);
            mappedEmails.put(field, (BitSet) docIds.clone());
            totalFreqs.put(field, totalTermFreq);
            termList = sortTermMap(termMap, tagged, totalTermFreq);
            if (termList.size() > count) {
                for (int i = termList.size() - 1; i >= count; i--) {
                    termList.remove(i);
                }
            }
        } catch (IOException ex) {
            System.err.println(SearchCollector.class
                    .getName().toString() + " " + ex);
        }
        return (termList);
    }

    public synchronized ArrayList<ScTermData> getExpansionTerms(String field, int count, BitSet tagged) {
        HashMap<String, ScTermData> termMap = new HashMap<String, ScTermData>();
        ArrayList<ScTermData> termList = null;
        try {
            long totalTermFreq = 0;
            for (int i = docIds.nextSetBit(0); i >= 0;
                    i = docIds.nextSetBit(i + 1)) {
                // operate on index i here
                if (masterIndexReader == null) {
                    try {
                        masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
                    } catch (IOException ex) {
                        System.err.println(SearchCollector.class.getName() + " " + ex);
                    }
                }
                Terms tv = masterIndexReader.getTermVector(i, field);
                if (tv == null) {
                    continue;
                }
                TermsEnum termsEnum = tv.iterator(null);
                BytesRef text;
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    long freq = termsEnum.totalTermFreq();
                    totalTermFreq += freq;
                    if (termMap.containsKey(term)) {
                        ScTermData t = termMap.get(term);
                        t.freq += freq;
                        t.totalDocFreq++;
                        termMap.put(term, t);
                    } else {
                        termMap.put(term, new ScTermData(term, freq));
                    }
                }
            }
            termList = sortTermMap(termMap, tagged, totalTermFreq);
            if (termList.size() > count) {
                for (int i = termList.size() - 1; i >= count; i--) {
                    termList.remove(i);
                }
            }
        } catch (IOException ex) {
            System.err.println(SearchCollector.class
                    .getName().toString() + " " + ex);
        }
        return (termList);
    }

    public synchronized ArrayList<ScTermData> getExpansionTermsMultiField(String[] fields, int count, BitSet tagged) {
        HashMap<String, ScTermData> termMap = new HashMap<String, ScTermData>();
        ArrayList<ScTermData> termList = null;
        try {
            long totalTermFreq = 0;
            for (int i = docIds.nextSetBit(0); i >= 0;
                    i = docIds.nextSetBit(i + 1)) {
                // operate on index i here
                if (masterIndexReader == null) {
                    try {
                        masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));
                    } catch (IOException ex) {
                        System.err.println(SearchCollector.class.getName() + " " + ex);
                    }
                }
                int group = 0;
                for (String field : fields) {
                    Terms tv = masterIndexReader.getTermVector(i, field);
                    if (tv == null) {
                        continue;
                    }
                    TermsEnum termsEnum = tv.iterator(null);
                    BytesRef text;
                    while ((text = termsEnum.next()) != null) {
                        String term = text.utf8ToString();
                        long freq = termsEnum.totalTermFreq();
                        totalTermFreq += freq;
                        if (termMap.containsKey(term)) {
                            ScTermData t = termMap.get(term);
                            t.freq += freq;
                            t.totalDocFreq++;
                            t.group = group;
                            termMap.put(term, t);
                        } else {
                            ScTermData std = new ScTermData(term, freq);
                            std.group = group;
                            termMap.put(term, std);
                        }
                    }
                    group++;
                }
            }
            termList = sortTermMap(termMap, tagged, totalTermFreq);
            if (termList.size() > count) {
                for (int i = termList.size() - 1; i >= count; i--) {
                    termList.remove(i);
                }
            }
        } catch (IOException ex) {
            System.err.println(SearchCollector.class
                    .getName().toString() + " " + ex);
        }
        return (termList);
    }

    private ArrayList<ScTermData> sortTermMap(HashMap<String, ScTermData> termMap,
            BitSet tagged, long totalTermFreq) {
        ArrayList<ScTermData> termList = new ArrayList<>();
        for (String key : termMap.keySet()) {
            try {
                ScTermData t = termMap.get(key);
                t.tfIdf = ((double) t.freq / (double) totalTermFreq)
                        * Math.log((double) docIds.cardinality() / (double) t.totalDocFreq);
//                System.out.println(t.tfIdf + " " + (double)t.freq + " " + (double)totalTermFreq);
//                System.out.println(t.term + " " + t.emails.cardinality() + " " + docIds.cardinality());
//                if ((double) t.emails.cardinality() / (double) docIds.cardinality() < 0.2 && docIds.cardinality() > 1) {
//                    t.tfIdf = (double) t.emails.cardinality() * Math.log((double) docIds.cardinality() / 
//                            (double) t.totalDocFreq);
//                } else {
//                    t.tfIdf = 0.0000;
//                }
//* Math.log((double) masterIndexReader.maxDoc() / 
//                                (double) masterIndexReader.docFreq(new Term(field, t.term))//                if (t.emails.intersects(tagged)) {
//                    t.tfIdf *= 2;
//                }

                termList.add(t);
            } catch (Exception ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Collections.sort(termList, Collections.reverseOrder());
        return (termList);
    }

    public void rankEmails(String[] fields, CharArraySet skipWords) {
        if (docIds.isEmpty()) {
            return;
        }
        tfLow = Double.MAX_VALUE;
        tfHigh = 0;
        emailWeights = new HashMap<>();
        if (masterIndexReader == null) {
            try {
                masterIndexReader = DirectoryReader.open(FSDirectory.open(new File(masterIndexPath)));

            } catch (IOException ex) {
                System.err.println(SearchCollector.class
                        .getName() + " " + ex);
            }
        }
        for (int i = docIds.nextSetBit(0); i >= 0;
                i = docIds.nextSetBit(i + 1)) {
            emailWeights.put(i, new Double(0.0));
            for (String field : fields) {
                try {
                    Terms tv = masterIndexReader.getTermVector(i, field);
                    if (tv == null) {
                        continue;
                    }
                    HashMap<String, ScTermData> termMap = termMaps.get(field);
                    TermsEnum termsEnum = tv.iterator(null);
                    BytesRef text;
                    while ((text = termsEnum.next()) != null) {
                        String term = text.utf8ToString();
                        if (stopWords.contains(term.toCharArray())
                                || skipWords.contains(term.toCharArray())) {
                            continue;
                        }
                        if (termMap.containsKey(term)) {
                            tfHigh = Math.max(tfHigh, termMap.get(term).tfIdf);
                            tfLow = Math.min(tfLow, termMap.get(term).tfIdf);
                            emailWeights.put(i, emailWeights.get(i)
                                    + (termMap.get(term).tfIdf * termsEnum.totalTermFreq()));
                        }
                    }
                } catch (IOException ex) {
                    System.err.println(SearchCollector.class
                            .getName() + " " + ex);
                } catch (NullPointerException e) {
                    System.err.println(field + " " + field + " " + emailWeights.get(i));
                    e.printStackTrace();
                }

            }
//            System.out.printf("Doc: %d Weight %f\n", i, emailWeights.get(i));
        }

    }

    public double getWeight(int id) {
        return (emailWeights.get(id));
    }

    public static void clearCurrentIndexedEmails() {
        currentIndexedEmails.clear();
        termMaps.clear();
        totalFreqs.clear();
        mappedEmails.clear();
    }

    public String[] getUids(BitSet emails) {
        int index = 0;
        String[] uidList = new String[emails.cardinality()];
        for (int i = emails.nextSetBit(0); i >= 0;
                i = emails.nextSetBit(i + 1)) {
            try {
                Document doc = masterIndexReader.document(i);
                String[] values = doc.getValues("uid");
                uidList[index++] = values[0];
            } catch (IOException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return uidList;
    }

    public BitSet uidListToBitSet(ArrayList<String> uidList) {
        SearchCollector emails = new SearchCollector();
        BitSet rEmails = new BitSet();
        Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40);
        QueryParser parser = new QueryParser(Version.LUCENE_40, "contents", analyzer);
        IndexSearcher searcher = new IndexSearcher(masterIndexReader);
        for (String uid : uidList) {
            try {
                Query q = NumericRangeQuery.newLongRange("uid", Long.parseLong(uid),
                        Long.parseLong(uid), true, true);
                searcher.search(q, emails);
            } catch (IOException ex) {
                Logger.getLogger(SearchCollector.class.getName()).log(Level.SEVERE, null, ex);
            }
            rEmails.or(emails.docIds);
        }
        return (rEmails);
    }
}
