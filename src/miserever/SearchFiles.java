/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import static java.lang.Math.log;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.HighFreqTerms.TotalTermFreqComparator;
import org.apache.lucene.misc.HighFreqTerms.DocFreqComparator;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.util.BytesRef;

/**
 * Simple command-line based search demo.
 */
public class SearchFiles {

    public SearchFiles() {
    }

    /**
     * Lucene search of passed directory and query.
     */
    public static void search(String[] args) throws Exception {
        String usage
                = "Usage:\tjava org.apache.lucene.demo.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw][-collect] [-paging hitsPerPage]\n\nSee http://lucene.apache.org/core/4_1_0/demo/ for details.";
        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        String index = "index";
        String field = "contents";
        String queries = null;
        int repeat = 0;
        boolean raw = false;
        boolean collect = false;
        String queryString = null;
        int hitsPerPage = 10;

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                index = args[i + 1];
                i++;
            } else if ("-field".equals(args[i])) {
                field = args[i + 1];
                i++;
            } else if ("-queries".equals(args[i])) {
                queries = args[i + 1];
                i++;
            } else if ("-query".equals(args[i])) {
                queryString = args[i + 1];
                i++;
            } else if ("-repeat".equals(args[i])) {
                repeat = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-raw".equals(args[i])) {
                raw = true;
            } else if ("-collect".equals(args[i])) {
                collect = true;
            } else if ("-paging".equals(args[i])) {
                hitsPerPage = Integer.parseInt(args[i + 1]);
                if (hitsPerPage <= 0) {
                    System.err.println("There must be at least 1 hit per page.");
                    System.exit(1);
                }
                i++;
            }
        }

        IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40);

        BufferedReader in = null;
        if (queries != null) {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(queries), "UTF-8"));
        } else {
            in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        }
        QueryParser parser = new QueryParser(Version.LUCENE_40, field, analyzer);
        while (true) {
            if (queries == null && queryString == null) {                        // prompt the user
                System.out.println("Enter query: ");
            }

            String line = queryString != null ? queryString : in.readLine();

            if (line == null || line.length() == -1) {
                break;
            }

            line = line.trim();
            if (line.length() == 0) {
                break;
            }

            Query query = parser.parse(line);
            System.out.println("Searching for: " + query.toString(field));

            if (repeat > 0) {                           // repeat & time as benchmark
                Date start = new Date();
                for (int i = 0; i < repeat; i++) {
                    searcher.search(query, null, 100);
                }
                Date end = new Date();
                System.out.println("Time: " + (end.getTime() - start.getTime()) + "ms");
            }
            if (collect) {
                // Collecting and creating a subset index if there is no query string exit
                if (queryString == null) {
                    break;
                }
                System.out.println("Creating new index");
                SearchCollector sc = new SearchCollector();
                searcher.search(query, sc);
                sc.createIndex(index + "/working",
                        index);
//                sc.createIndex("/Users/jkoven/Documents/PolyResearch/PixelViz/v2/Index/working",
//                        "/Users/jkoven/Documents/PolyResearch/PixelViz/v2/Index");
                reader = sc.getIndex();
                searcher = new IndexSearcher(reader);
                collect = false;
                queryString = null;
                System.out.println(reader.maxDoc());
                TermStats[] terms;
                //            terms = HighFreqTerms.getHighFreqTerms(reader, 50, "contents", new DocFreqComparator());
                terms = HighFreqTerms.getHighFreqTerms(reader, 50, "contents", new TotalTermFreqComparator());
                for (TermStats term : terms) {
                    double tfIdf = ((double) term.docFreq * log((double) reader.maxDoc()
                            / (double) term.totalTermFreq));
                    System.out.println(term.termtext.utf8ToString() + " " + tfIdf);
                }
            } else {
                doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null && queryString == null);
            }

            if (queryString != null) {
                break;
            }
        }
        reader.close();
    }

    /**
     * This demonstrates a typical paging search scenario, where the search
     * engine presents pages of size n to the user. The user can then go to the
     * next page if interested in the next hits.
     *
     * When the query is executed for the first time, then only enough results
     * are collected to fill 5 result pages. If the user wants to page beyond
     * this limit, then the query is executed another time and all hits are
     * collected.
     *
     */
    public static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query,
            int hitsPerPage, boolean raw, boolean interactive) throws IOException {

        // Collect enough docs to show 5 pages
        TopDocs results = searcher.search(query, 5 * hitsPerPage);
        ScoreDoc[] hits = results.scoreDocs;

        int numTotalHits = results.totalHits;
        System.out.println(numTotalHits + " total matching documents");

        int start = 0;
        int end = Math.min(numTotalHits, hitsPerPage);

        while (true) {
            if (end > hits.length) {
                System.out.println("Only results 1 - " + hits.length + " of " + numTotalHits + " total matching documents collected.");
                System.out.println("Collect more (y/n) ?");
                String line = in.readLine();
                if (line.length() == 0 || line.charAt(0) == 'n') {
                    break;
                }

                hits = searcher.search(query, numTotalHits).scoreDocs;
            }

            end = Math.min(hits.length, start + hitsPerPage);

            for (int i = start; i < end; i++) {
                if (raw) {                              // output raw format
//          System.out.println("doc="+searcher.doc(hits[i].doc).get("path")+" score="+hits[i].score);
                    System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);
                    continue;
                }

                Document doc = searcher.doc(hits[i].doc);
                String path = doc.get("path");
                if (path != null) {
                    System.out.println((i + 1) + ". " + path);
                    String title = doc.get("title");
                    if (title != null) {
                        System.out.println("   Title: " + doc.get("title"));
                    }
                } else {
                    System.out.println((i + 1) + ". " + "No path for this document");
                }

            }

            if (!interactive || end == 0) {
                break;
            }

            if (numTotalHits >= end) {
                boolean quit = false;
                while (true) {
                    System.out.print("Press ");
                    if (start - hitsPerPage >= 0) {
                        System.out.print("(p)revious page, ");
                    }
                    if (start + hitsPerPage < numTotalHits) {
                        System.out.print("(n)ext page, ");
                    }
                    System.out.println("(q)uit or enter number to jump to a page.");

                    String line = in.readLine();
                    if (line.length() == 0 || line.charAt(0) == 'q') {
                        quit = true;
                        break;
                    }
                    if (line.charAt(0) == 'p') {
                        start = Math.max(0, start - hitsPerPage);
                        break;
                    } else if (line.charAt(0) == 'n') {
                        if (start + hitsPerPage < numTotalHits) {
                            start += hitsPerPage;
                        }
                        break;
                    } else {
                        int page = Integer.parseInt(line);
                        if ((page - 1) * hitsPerPage < numTotalHits) {
                            start = (page - 1) * hitsPerPage;
                            break;
                        } else {
                            System.out.println("No such page");
                        }
                    }
                }
                if (quit) {
                    break;
                }
                end = Math.min(numTotalHits, start + hitsPerPage);
            }
        }
    }

    public static void getCollection(IndexReader reader, Query query, SearchCollector sc) {
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            
//        System.out.println(query.toString() + " " + line);
//        System.out.println("Searching for: " + query.toString(field));
            // Collecting and creating a subset index if there is no query string exit
//        System.out.println("starting collection search");
            if (sc == null) {
                System.out.println("Fix search collector call");                
                sc = new SearchCollector();
            }
            searcher.search(query, sc);
//        System.out.println(sc.docIds.cardinality());
        } catch (IOException ex) {
            System.err.println(SearchFiles.class.getName().toString() + " " + ex);
        }
    }
}
