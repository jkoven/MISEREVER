/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.swing.JFileChooser;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 *
 * @author jkoven
 */
public class MISEREVER {

    public static String bindexPath;
    public static String bhashIndexPath;
    public static String bmessagePath;
    public static File baseDirectory = null;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        int portNumber = 4444;
        int acceptCount = 5;

        try {
            System.setOut(new PrintStream(new File("output-file.txt")));
            System.setErr(new PrintStream(new File("error-file.txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        File sourceFile = new File(MISEREVER.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        try {
            BufferedReader credReader = new BufferedReader(new FileReader(new File(sourceFile.getParent(), "datafiles/basedirectory.txt")));
            baseDirectory = new File(passwordDecode(credReader.readLine().trim()));
            credReader.close();
        } catch (IOException ex) {

        }
        JFileChooser fc;
        if (baseDirectory == null) {
            fc = new JFileChooser();
        } else {
            fc = new JFileChooser(baseDirectory);
        }
        fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int resultVal = fc.showOpenDialog(null);
        if (resultVal != JFileChooser.APPROVE_OPTION) {
            System.exit(0);
        }
        baseDirectory = fc.getSelectedFile();
        bindexPath = new File(baseDirectory, "index").toString();
        bhashIndexPath = new File(baseDirectory, "hashindex").toString();
        bmessagePath = new File(baseDirectory, "messages").toString();

        try {
            FileWriter credOut = new FileWriter(new File(sourceFile.getParent(), "datafiles/basedirectory.txt"));
            credOut.write(passwordEncode(baseDirectory.getParent()) + "\n");
            credOut.close();
        } catch (IOException e) {
            System.err.println("Cant write to the base directory file");
        }
        boolean reload = true;
        int restartCount = 0;
        while (reload) {
            try {
                reload = false;
                ServerSocket serverSocket = new ServerSocket(portNumber, acceptCount);
                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(new CommandHandler(client, baseDirectory)).start();
                    Thread.sleep(10);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Attempting ServerSocket Restart");
                if (restartCount++ < 10) {
                    reload = true;
                }
            }
        }
    }

    private static String passwordEncode(String password) {
        try {
            // only the first 8 Bytes of the constructor argument are used
// as material for generating the keySpec
            DESKeySpec keySpec = new DESKeySpec("NotReallySecure".getBytes("UTF8"));
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey key = keyFactory.generateSecret(keySpec);
            sun.misc.BASE64Encoder base64encoder = new BASE64Encoder();
            sun.misc.BASE64Decoder base64decoder = new BASE64Decoder();
// ENCODE plainTextPassword String
            byte[] cleartext = password.getBytes("UTF8");

            Cipher cipher = Cipher.getInstance("DES"); // cipher is not thread safe
            cipher.init(Cipher.ENCRYPT_MODE, key);
            String encrypedPwd = base64encoder.encode(cipher.doFinal(cleartext));
            // now you can store it
            return (encrypedPwd);

        } catch (InvalidKeyException | UnsupportedEncodingException |
                NoSuchAlgorithmException | InvalidKeySpecException |
                NoSuchPaddingException | IllegalBlockSizeException |
                BadPaddingException ex) {
            ex.printStackTrace();
        }
        return (null);
    }

    private static String passwordDecode(String password) {
        try {
            // only the first 8 Bytes of the constructor argument are used
// as material for generating the keySpec
            DESKeySpec keySpec = new DESKeySpec("NotReallySecure".getBytes("UTF8"));
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey key = keyFactory.generateSecret(keySpec);
            sun.misc.BASE64Encoder base64encoder = new BASE64Encoder();
            sun.misc.BASE64Decoder base64decoder = new BASE64Decoder();
// DECODE encryptedPwd String
            byte[] encrypedPwdBytes = base64decoder.decodeBuffer(password);

            Cipher cipher = Cipher.getInstance("DES");// cipher is not thread safe
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] plainTextPwdBytes = (cipher.doFinal(encrypedPwdBytes));
            return (new String(plainTextPwdBytes));
        } catch (InvalidKeyException | UnsupportedEncodingException |
                NoSuchAlgorithmException | InvalidKeySpecException |
                NoSuchPaddingException | IllegalBlockSizeException |
                BadPaddingException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return (null);
    }
}
