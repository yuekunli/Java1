package Experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.*;
import java.util.Arrays;

public class Crypto
{
    static void tenThousandZerosHash()
    {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            byte [] chunk = new byte[10000];
            md.reset();
            md.update(chunk);
            byte[] hash = md.digest();
            System.out.println(Arrays.toString(hash));

            StringBuilder s = new StringBuilder();

            for (byte b : hash)
            {
                s.append(String.format("%8s", Integer.toBinaryString(((b+256)%256))).replace(' ', '0')).append("  ");
            }

            System.out.println(s);
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Get a byte array by calling PrivateKey.getEncoded.
     * Save the bytes in a file. The file won't be human-readable.
     * Later have OpenSSL open the file and read it and see if OpenSSL can recognize this is an RSA private key.
     * The purpose of this function is to verify that PrivateKey.getEncoded encodes an RSA private key in a format
     * that is recognizable to OpenSSL.
     */
    public static void saveRsaKeysInFile()
    {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(512);
            KeyPair keyPair = g.generateKeyPair();
            PrivateKey privKey = keyPair.getPrivate();
            byte[] encodedPrivKey = privKey.getEncoded(); // this is supposed to be PKCS#8 format and then DER encoded

            StringBuilder s = new StringBuilder();
            for (byte b : encodedPrivKey)
            {
                String binaryForm = String.format("%2s", Integer.toHexString(b)).replace(' ', '0');
                s.append(binaryForm.substring(binaryForm.length()-2)).append("  ");
            }
            System.out.println(s);

            String java_key_file_path = "C:\\ws\\openssl_output\\Lib\\x64\\Debug\\bin\\java_key_file";
            File java_key_file = new File(java_key_file_path);
            boolean r = java_key_file.createNewFile();

            RandomAccessFile java_key_raf = new RandomAccessFile(java_key_file_path, "rw");
            FileChannel java_key_fc = java_key_raf.getChannel();

            ByteBuffer bb2 = ByteBuffer.wrap(encodedPrivKey);
            java_key_fc.write(bb2);
            java_key_fc.close();
            java_key_raf.close();
        }
        catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void readKeyFileFromOpenSSL(String path)
    {
        //String opensslKeyFilePath = "C:\\ws\\openssl_output\\Lib\\x64\\Debug\\bin\\rsa512keys_pkcs8_der";
        RandomAccessFile f;
        try {
            f = new RandomAccessFile(path, "r");
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            return;
        }
        FileChannel fc = f.getChannel();

        ByteBuffer bb;
        try {
            bb = ByteBuffer.allocate((int) fc.size());
            fc.read(bb);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }

        byte[] openssl_key = bb.array();
        StringBuilder s = new StringBuilder();
        for (byte b : openssl_key)
        {
            String binaryForm = String.format("%2s", Integer.toHexString(b)).replace(' ', '0');
            s.append(binaryForm.substring(binaryForm.length()-2)).append("  ");
        }
        System.out.println(s);
        try {
            fc.close();
            f.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
