package Poems;

import java.io.*;
import java.util.List;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SequenceFileReader {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: SequenceFileWriter <input file>");
        }

        File inputFile = new File(args[0]);
        if (!inputFile.isFile()) {
            System.out.println("Path must lead to a file");
            System.exit(-1);
        }

        readSequenceFile(inputFile);
    }

    private static void readSequenceFile(File sourceFile) {
        try {
            FileReader fileReader = new FileReader(sourceFile.toPath().toString());
            BufferedReader buffer = new BufferedReader(fileReader);
            String line;
            while ((line = buffer.readLine()) != null) {
                String[] entries = line.split("\t");
                System.out.println("Key: " + entries[0] + "\nValue: " + entries[1]);
            }
        } catch (FileNotFoundException e) {
            System.err.append("File \"" + sourceFile.toPath() + "\" not found\n");
            e.printStackTrace();
            System.out.println("Can Open?: " + sourceFile.canRead());
        } catch (IOException e) {
            System.err.append("Error while reading file \"" + sourceFile.toPath() + "\" not found\n");
        }
    }
    }
