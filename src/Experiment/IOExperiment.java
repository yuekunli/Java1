package Experiment;

import java.io.File;
import java.io.IOException;

public class IOExperiment
{
    static void createFile()
    {
        String folderPath = "C:\\Temp";
        String filename = "test-file.txt";

        try {
            File folder = new File(folderPath);
            if (folder.exists()) {
                System.out.println("folder exists");
            } else {
                boolean ret = folder.mkdir();
                System.out.println("create folder result: " + ret);
            }
            String filepath = folderPath + File.pathSeparator + filename;
            File file = new File(filepath);
            boolean ret = file.createNewFile();
            System.out.println("Create file result: " + ret);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
