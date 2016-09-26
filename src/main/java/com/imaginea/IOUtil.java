package main.java.com.imaginea;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Created by sriram on 26/9/16.
 */
public class IOUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);

    static int fileCounter = 0;
    static int zipCounter = 0;
    static FileChannel sourceFileChannel = null;
    static String fileType = null;
    static long startTime = 0l;

    public static File createDataGeneratorInitFile() throws IOException {
        Path inFile = Paths.get("sample.log");
        final FileChannel inFileChannel = new FileInputStream(inFile.getFileName().toFile()).getChannel();
        File outFile = new File("datageneratorfile.log");
        final WritableByteChannel outputChannel = new FileOutputStream(outFile).getChannel();
        try {
            while (outFile.length() < (1024 * 1024 * 4)) {
                for (long p = 0, l = inFileChannel.size(); p < l; ) {
                    //Transfer data from input channel to output channel
                    p += inFileChannel.transferTo(p, l - p, outputChannel);
                }
            }
            return outFile;
        } finally {
            if (inFileChannel != null) {
                inFileChannel.close();
            }
            if (outputChannel != null) {
                outputChannel.close();
            }
            LOGGER.info("Successfully created the file  -{} ", outFile.getName());
        }
    }

    public static void createFiles(long finalNumberOfTimesToRotate, int numberOfFiles) throws IOException {
        fileType = "log";
        File outFile = new File(getFileName() + ".log");
        Path inFileForHeader = Paths.get("skipFile.log");
        Path inFile = Paths.get("datageneratorfile.log");
        final FileChannel inputChannelForHeader = new FileInputStream(inFileForHeader.getFileName().toFile()).getChannel();
        final FileChannel inFileChannel = new FileInputStream(inFile.getFileName().toFile()).getChannel();
        final OutputStream outputStream = new FileOutputStream(outFile);
        final WritableByteChannel outputChannel = Channels.newChannel(outputStream);
        try {
            inputChannelForHeader.transferTo(0, inputChannelForHeader.size(), outputChannel);
            for (int j = 0; j < finalNumberOfTimesToRotate / 16; j++) {
                for (int k = 0; k <= 16; k++) {
                    //Transfer data from input channel to output channel
                    inFileChannel.transferTo(0, inFileChannel.size(), outputChannel);
                }
            }
        } finally {
            if (inputChannelForHeader != null) {
                inputChannelForHeader.close();
            }
            if (inFileChannel != null) {
                inFileChannel.close();
            }
            if (outputChannel != null) {
                outputChannel.close();
            }
            LOGGER.info("Successfully created the file  -{} ", outFile.getName());

            /*
            * createZipFIles will only create zip(compressed .log.gz)
            * copyMultipleFiles will create both uncompressed base file i.e. .log  and zip(compressed .log.gz)
            **/

            createZipFIles(numberOfFiles);
            //copyMultipleFiles(outFile, numberOfFiles, fileType);
        }
    }

    public static void createZipFIles(int numberOfZipFiles) throws IOException {
        String fileName = getZipFileName();
        File inputFile = new File(fileName + ".log");
        String zipFileName = fileName + ".log" + ".gz";
        File zipFile = new File(zipFileName);
        fileType = "zip";

        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new FileOutputStream(zipFileName));
        FileInputStream inputStream = new FileInputStream(inputFile);
        int length;
        byte[] fileSize = new byte[1024 * 1024 * 512];
        try {
            while ((length = inputStream.read(fileSize)) > 0) {
                gzipOutputStream.write(fileSize, 0, length);
            }
        } finally {
            inputStream.close();
            gzipOutputStream.finish();
            gzipOutputStream.close();
            gzipOutputStream.flush();
            LOGGER.info("Successfully created the file  -{} ", zipFileName);
            copyMultipleFiles(zipFile, numberOfZipFiles, fileType);
        }
    }

    private static void copyMultipleFiles(File sourceFile, int numberOfDistFiles, String destinationFileType) throws IOException {
        sourceFileChannel = new FileInputStream(sourceFile).getChannel();
        fileType = destinationFileType;
        try {
            final ExecutorService executor = Executors.newWorkStealingPool();
            for (int i = 1; i < numberOfDistFiles; i++) {
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            File destinationFile = null;
                            if (fileType.equalsIgnoreCase("log")) {
                                destinationFile = new File(getFileName() + ".log");
                            } else if (fileType.equalsIgnoreCase("zip")) {
                                destinationFile = new File(getZipFileName() + ".log" + ".gz");
                            }
                            WritableByteChannel outputFileChannel = new FileOutputStream(destinationFile).getChannel();
                            try {
                                sourceFileChannel.transferTo(0, sourceFileChannel.size(), outputFileChannel);
                            } finally {
                                outputFileChannel.close();
                            }
                            LOGGER.info("Successfully created the file  -{} ", destinationFile.getName());
                            Runtime.getRuntime().gc();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
            if (fileType.equalsIgnoreCase("log")) {
                createZipFIles(numberOfDistFiles);
            }
        } finally {
            sourceFileChannel.close();
            LOGGER.info("Finished time for all threads -{}", (System.currentTimeMillis()));
            LOGGER.info("Total time taken -{} ", (System.currentTimeMillis()) - startTime);
        }
    }

    static synchronized String getFileName() {
        fileCounter++;
        return "datageneratorfile" + fileCounter;
    }

    static synchronized String getZipFileName() {
        zipCounter++;
        return "datageneratorfile" + zipCounter;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

}