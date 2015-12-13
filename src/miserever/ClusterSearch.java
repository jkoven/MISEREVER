/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import static miserever.CommandHandler.jMap;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 *
 * @author jkoven
 */
public class ClusterSearch {

    File indexDirectory;

    public ClusterSearch(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }

    public JsonObject clusterSearch(JsonObject jIn) {
        
        SearchCollector sc = new SearchCollector();
        sc.masterIndexPath = new File(indexDirectory, "index").toString();
        String[] fields = {"Subject", "contents"};
        if (jIn.containsKey("searchString")){
                    sc.multiFieldSearch(fields, jIn.getString("searchString"));
        } else {
            if (jIn.containsKey("emails")){
                sc.docIds.clear();
                JsonArray emails = jIn.getJsonArray("emails");
                for (int i = 0; i < emails.size(); i++){
                    sc.docIds.set(Integer.parseInt(emails.getString(i)));
                }
            }
        }
        ArrayList<ScTermData> tdl = sc.getExpansionTerms("contents", 100, new BitSet());
        JsonBuilderFactory jFactory = Json.createBuilderFactory(jMap);
        JsonArrayBuilder jTerms = jFactory.createArrayBuilder();
        for (ScTermData term : tdl){
            jTerms.add(jFactory.createObjectBuilder().add("term", term.term)
            .add("freq", term.freq)
            .add("doccount", term.emails.cardinality()).build());
        }
        JsonObjectBuilder jResults = jFactory.createObjectBuilder();
        jResults.add("results", jTerms.build());
        return (jResults.build());
    }
}
