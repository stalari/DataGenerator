package com.imaginea;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sudheerp on 30/6/16.
 *
 * This class will generate the bigger files based on the user requirement
 */
public class DataGenerator {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DataGenerator.class);

    public static void main(String[] args) throws IOException,
            InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Started time-{}", startTime);

        int numberOfFiles = 2;
        long fileMInSize = 0;
        long fileMaxSize = 0;
        if (args.length >= 3) {
            LOGGER.info("Number of files-{}" ,args[0] , "   , minumum size-{}"
                    ,args[1] , "   ,maximum size-{}" , args[2]);
            numberOfFiles = Integer.parseInt(args[0]);
            fileMInSize = Long.parseLong(args[1]);
            fileMaxSize = Long.parseLong(args[2]);
        } else if (args.length >= 2) {
            LOGGER.info("Number of filess-{}" , args[0]
                    + "   , minumum size-{}" , args[1]);
            numberOfFiles = Integer.parseInt(args[0]);
            fileMInSize = Long.parseLong(args[1]);
        } else if (args.length >= 1) {
            LOGGER.info("Number of files-{}" , args[0]);
            numberOfFiles = Integer.parseInt(args[0]);
        }

        DataGenerator dataGenerator = new DataGenerator();
        // read the config properties to get the properties
        Properties configPropertis = new Properties();
        configPropertis.load(dataGenerator
                .getFileFromResource("config.properties"));
        // Read the file from path using Buffered Reader

        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(
                        dataGenerator.getFileFromResource("sample-s3.log")));
        // Skipp first 3 lines of file
        int numberOfLinesTobeSkip = Integer.parseInt(configPropertis
                .getProperty("numberOfLinesTobeSkip"));
        // create skippedline file to calculate the sample file length
        File sampleFile = new File(configPropertis.getProperty("fileName"));
        File skippedLinesFile = new File("skipFile.log");
        splitFiles(bufferedReader, skippedLinesFile, numberOfLinesTobeSkip);
        long fileLength = sampleFile.length() - skippedLinesFile.length();
        File file = new File("datageneratorfile.log");
        Path outFile = Paths.get(file.getAbsolutePath());
        try (FileChannel out = FileChannel.open(outFile, CREATE, WRITE)) {

            Path inFile = Paths.get("sample.log");
            try (FileChannel in = FileChannel.open(inFile, READ)) {
                while (file.length() < (1024 * 1024 * 4)) {
                   for (long p = 0, l = in.size(); p < l;) {
                        p += in.transferTo(p, l - p, out);
                    }
                }
            }
        }
        long numberOfTimesToRotate =0;
        if(fileMaxSize==0 && fileMInSize==0){

            numberOfTimesToRotate = getFileSize(fileMInSize, fileMaxSize)
                    / file.length();
        }else if(fileMaxSize!=0){
            numberOfTimesToRotate=fileMaxSize/file.length();
        }else{
            numberOfTimesToRotate=fileMInSize/file.length();
        }
        String fileName = "";
        byte[] fileSize = new byte[1024 * 1024 * 512];
        for (int i = 0; i < numberOfFiles; i++) {
            fileName = "datageneratorfile" + i;
            file = new File(fileName + ".log");
            outFile = Paths.get(file.getAbsolutePath());
            LOGGER.info("TO -{}", outFile);
            try (FileChannel out = FileChannel.open(outFile, CREATE, WRITE)) {
                Path inFile = Paths.get("datageneratorfile.log");
                LOGGER.info("Copying data from...-{} ", inFile);
                for (int j = 0; j < numberOfTimesToRotate / 16; j++) {
                    try (FileChannel in = FileChannel.open(inFile, READ)) {
                        for (int k = 0; k <= 16; k++) {
                            for (long p = 0, l = in.size(); p < l;) {
                                p += in.transferTo(p, l - p, out);
                            }
                        }
                    }
                    Thread.sleep(200);
                }
            }
            LOGGER.info("Successfully created the file -{}",fileName);
            // create zip file
            Thread.sleep(800);
            LOGGER.info("Creating zip file for the file -{}",fileName);
            WritableByteChannel channel = null;
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(
                    new FileOutputStream(fileName + ".gz"));

            channel = Channels.newChannel(gzipOutputStream);
            FileInputStream inputStream = new FileInputStream(file);
            int length;
            while ((length = inputStream.read(fileSize)) > 0) {
                gzipOutputStream.write(fileSize, 0, length);
                Thread.sleep(300);
            }
            gzipOutputStream.finish();
            gzipOutputStream.close();
            LOGGER.info("Successfully created gzipped file for the fie -{}",fileName);
            Runtime.getRuntime().gc();
        }

        LOGGER.info("Total time taken-{}",
                (System.currentTimeMillis() - startTime));
    }

    /**
     *
     * @param bufferedReader
     * @param skippedLinesFile
     * @param numberOfLinesTobeSkipp
     * @throws IOException
     *             This method is used to create a skippedlinefile,that is used
     *             to calculate samplefile size
     */
    private static void splitFiles(BufferedReader bufferedReader,
                                   File skippedLinesFile, int numberOfLinesTobeSkipp)
            throws IOException {
        FileWriter skippedFileWriter = new FileWriter(
                skippedLinesFile.getAbsoluteFile());
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
        BufferedWriter sampleBufferredWriter = new BufferedWriter(
                sampleFileWriter);
        while ((line = bufferedReader.readLine()) != null) {
            sampleFileWriter.write(line);
            sampleBufferredWriter.write("\r\n");
        }

        sampleBufferredWriter.close();
        sampleFileWriter.close();

    }

    private String getSamplePath(String fileName) {
        return DataGenerator.class.getResource(fileName).getPath();
    }

    private InputStream getFileFromResource(String fileName) throws FileNotFoundException {

        InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
        return in;
    }

    private static long getFileSize(long minSIze, long maxSize) {
        long fileSize = 0;
        if (maxSize == 0 && minSIze == 0) {
            fileSize = (1024 * 1024 * 1024) +(1024 * 1024 * 512);
        }
        return fileSize;
    }
}
