/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;
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
        OutputStream sout = null;
        try {
            String fileName = jList.getString("filename");
            HashMap jMap = new HashMap();
            jMap.put(JsonGenerator.PRETTY_PRINTING, true);
            sout = new FileOutputStream(new File("./workingfiles/" + fileName));
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

        File dir = new File("./workingfiles/");
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
        JsonObject jsonIn = null;
        try {
            JsonReaderFactory rFactory = Json.createReaderFactory(new HashMap<String, String>());
            JsonReader jr = rFactory.createReader(new FileReader(new File("./workingfiles/" + fileName)));
            jsonIn = jr.readObject();
            jr.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(FileUtils.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return (jsonIn);
    }
}
