package Poems;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SequenceFileWriter {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: SequenceFileWriter <input directory> <path to output file>");
        }

        File directory = new File(args[0]);
        if (!directory.isDirectory()) {
            System.out.println("Path must lead to a directory");
            System.exit(-1);
        }

        File[] listOfFiles = directory.listFiles();

        List<File> files = new ArrayList<File>();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                files.add(listOfFiles[i]);
            }
        }
        if (files.size() != 0) {
            File outputFile = new File(args[1]);
            createSequenceFile(files, outputFile);
        } else {
            System.out.println("No files to read");
            System.exit(-1);
        }
    }

    private static void createSequenceFile(List<File> files, File outputFile) {
        BufferedWriter writer;
        try {
            FileOutputStream outputStream = new FileOutputStream(outputFile.toPath().toString());
             writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        } catch (FileNotFoundException e) {
            System.err.append("File \"" + outputFile.toPath() + "\" not found\n");
            e.printStackTrace();
            System.out.println("Can Open?: " + outputFile.canRead());
            return;
        }

        for (File sourceFile : files) {
            try {
                FileReader fileReader = new FileReader(sourceFile.toPath().toString());
                BufferedReader buffer = new BufferedReader(fileReader);
                String line;
                while((line = buffer.readLine()) != null) {
                    System.out.println("Line: " + line);
                    writer.write(sourceFile.toString() + "\t" + line + "\n");
                }
            } catch (FileNotFoundException e) {
                System.err.append("File \"" + sourceFile.toPath() + "\" not found\n");
                e.printStackTrace();
                System.out.println("Can Open?: " + sourceFile.canRead());
            } catch (IOException e) {
                System.err.append("Error while reading file \"" + sourceFile.toPath() + "\" not found\n");
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
