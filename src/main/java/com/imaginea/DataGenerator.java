package com.imaginea;

import main.java.com.imaginea.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by sudheerp on 30/6/16.
 * <p>
 * This class will generate the bigger files based on the user requirement
 */
public class DataGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        final IOUtil util = new IOUtil();
        DataGenerator dataGenerator = new DataGenerator();

        LOGGER.info("Started time -{} ", startTime);
        util.setStartTime(startTime);

        /**
         *  Declare file variables with default values
         */
        int numberOfFiles = 2;
        long fileMInSize = 0;
        long fileMaxSize = 0;

        /**
         * Initialize file variables based on the user chosen arguments
         */
        if (args.length >= 3) {
            LOGGER.info("Number of files -{} ", args[0], " , minumum size -{} ", args[1], ", maximum size -{} ", args[2]);
            numberOfFiles = Integer.parseInt(args[0]);
            fileMInSize = Long.parseLong(args[1]);
            fileMaxSize = Long.parseLong(args[2]);
        } else if (args.length >= 2) {
            LOGGER.info("Number of files -{} ", args[0] + ", minumum size -{} ", args[1]);
            numberOfFiles = Integer.parseInt(args[0]);
            fileMInSize = Long.parseLong(args[1]);
        } else if (args.length >= 1) {
            LOGGER.info("Number of files -{} ", args[0]);
            numberOfFiles = Integer.parseInt(args[0]);
        }

        // Read the config properties to get the properties
        Properties configProperties = new Properties();
        configProperties.load(dataGenerator.getFileFromResource("config.properties"));

        // Read the file from path using Buffered Reader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataGenerator.getFileFromResource(configProperties.getProperty("fileName"))));

        // Skip the first 3 lines of file
        int numberOfLinesTobeSkip = Integer.parseInt(configProperties.getProperty("numberOfLinesToSkip"));

        // create skipLine file to calculate the sample file length
        splitFiles(bufferedReader, numberOfLinesTobeSkip);
        // create DataGenerator Initial File
        File dataGeneratorInitFile = util.createDataGeneratorInitFile();

        long finalNumberOfTimesToIterate;
        if (fileMaxSize == 0 && fileMInSize == 0) {
            finalNumberOfTimesToIterate = getFileSize(fileMInSize, fileMaxSize) / dataGeneratorInitFile.length();
        } else if (fileMaxSize != 0) {
            finalNumberOfTimesToIterate = fileMaxSize / dataGeneratorInitFile.length();
        } else {
            finalNumberOfTimesToIterate = fileMInSize / dataGeneratorInitFile.length();
        }

        //methods create large file and then zips it
        util.createFiles(finalNumberOfTimesToIterate, numberOfFiles);
    }

    /**
     * @param bufferedReader
     * @param numberOfLinesTobeSkipp
     * @throws IOException This method is used to create a skippedlinefile,that is used
     *                     to calculate samplefile size
     */
    private static void splitFiles(BufferedReader bufferedReader, int numberOfLinesTobeSkipp)
            throws IOException {
        FileWriter skippedFileWriter = new FileWriter("skipFile.log");
        BufferedWriter bufferedWriter = new BufferedWriter(skippedFileWriter);
        for (int i = 0; i < numberOfLinesTobeSkipp; i++) {
            String skipplIne = bufferedReader.readLine();
            bufferedWriter.write(skipplIne);
            bufferedWriter.write("\r\n");
        }
        bufferedWriter.close();
        skippedFileWriter.close();


        String line = "";
        FileWriter sampleFileWriter = new FileWriter("sample.log");
        BufferedWriter sampleBufferredWriter = new BufferedWriter(sampleFileWriter);
        while ((line = bufferedReader.readLine()) != null) {
            sampleFileWriter.write(line);
            sampleBufferredWriter.write("\r\n");
        }
        sampleBufferredWriter.close();
        sampleFileWriter.close();
    }

    private InputStream getFileFromResource(String fileName) throws FileNotFoundException {
        InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
        return in;
    }

    private static long getFileSize(long minSIze, long maxSize) {
        long fileSize = 0;
        if (maxSize == 0 && minSIze == 0) {
            fileSize = (1024 * 1024 * 1024) + (1024 * 1024 * 512);
        }
        return fileSize;
    }

}