package com.hdfs.crud;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsMain {

    private FileSystem fileSystem; /* HDFS file system */
    private static Configuration conf;
    private String DEST_PATH = "/user/hdfs-examples";
    private String FILE_NAME = "test.txt";

    private String HDFS_PATH="hdfs://zoe01:9000/";
    private static String PATH_TO_HDFS_SITE_XML = HdfsMain.class.getClassLoader().getResource("hdfs-site.xml").getPath();
    private static String PATH_TO_CORE_SITE_XML = HdfsMain.class.getClassLoader().getResource("core-site.xml").getPath();


    public static void main(String[] args) throws Exception {
        HdfsMain hdfsMain = new HdfsMain();
        hdfsMain.confLoad();
        hdfsMain.getFileSystem();
        hdfsMain.examples();
    }

    /**
     * HDFS operator instance
     *
     * @throws Exception
     */
    public void examples() throws Exception {

        // create directory
        mkdir();

        // write file
        //write();
        //
        // append file
        //append();
        //
        // read file
        //read();
        //
        // delete file
        //delete();
        //
        // delete directory
        //rmdir();
    }


    /**
     * Add configuration file if the application run on the linux ,then need
     * make the path of the core-site.xml and hdfs-site.xml to in the linux
     * client file
     */
    private void confLoad() throws IOException {
        conf = new Configuration();
        // conf file
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        // conf.addResource(new Path(PATH_TO_SMALL_SITE_XML));
    }


    /**
     * build HDFS instance
     */
    private void getFileSystem() throws Exception {
        // get filesystem
        //fileSystem = FileSystem.get(conf);
        fileSystem=FileSystem.get(new URI(HDFS_PATH), conf, "root");
    }

    /**
     * delete directory
     *
     * @throws IOException
     */
    private void rmdir() throws IOException {
        Path destPath = new Path(DEST_PATH);
        if (!deletePath(destPath)) {
            System.err.println("failed to delete destPath " + DEST_PATH);
            return;
        }

        System.out.println("success to delete path " + DEST_PATH);

    }

    /**
     * create directory
     *
     * @throws IOException
     */
    private void mkdir() throws IOException {
        Path destPath = new Path(DEST_PATH);
        if (!createPath(destPath)) {
            System.err.println("failed to create destPath " + DEST_PATH);
            return;
        }

        System.out.println("success to create path " + DEST_PATH);
    }


    /**
     * create file,write file
     *
     * @throws IOException
     * @throws
     */
    private void write() throws IOException, ParameterException {
        final String content = "hi, I am bigdata. It is successful if you can see me.";
        InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
        try {
            HdfsWriter writer = new HdfsWriter(fileSystem, DEST_PATH + File.separator + FILE_NAME);
            writer.doWrite(in);
            System.out.println("success to write.");
        } finally {
            // make sure the stream is closed finally.
            close(in);
        }
    }

    /**
     * append file content
     *
     * @throws IOException
     */
    private void append() throws Exception {
        final String content = "I append this content.";
        InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
        try {
            HdfsWriter writer = new HdfsWriter(fileSystem, DEST_PATH + File.separator + FILE_NAME);
            writer.doAppend(in);
            System.out.println("success to append.");
        } finally {
            // make sure the stream is closed finally.
            close(in);
        }
    }

    /**
     * read file
     *
     * @throws IOException
     */
    private void read() throws IOException {
        String strPath = DEST_PATH + File.separator + FILE_NAME;
        Path path = new Path(strPath);
        FSDataInputStream in = null;
        BufferedReader reader = null;
        StringBuffer strBuffer = new StringBuffer();

        try {
            in = fileSystem.open(path);
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;

            // write file
            while ((sTempOneLine = reader.readLine()) != null) {
                strBuffer.append(sTempOneLine);
            }

            System.out.println("result is : " + strBuffer.toString());
            System.out.println("success to read.");

        } finally {
            // make sure the streams are closed finally.
            close(reader);
            close(in);
        }
    }

    /**
     * delete file
     *
     * @throws IOException
     */
    private void delete() throws IOException {
        Path beDeletedPath = new Path(DEST_PATH + File.separator + FILE_NAME);
        if (fileSystem.delete(beDeletedPath, true)) {
            System.out.println("success to delete the file " + DEST_PATH + File.separator + FILE_NAME);
        } else {
            System.err.println("failed to delete the file " + DEST_PATH + File.separator + FILE_NAME);
        }
    }

    /**
     * close stream
     *
     * @param stream
     * @throws IOException
     */
    private void close(Closeable stream) throws IOException {
        stream.close();
    }

    /**
     * create file path
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    private boolean createPath(final Path filePath) throws IOException {
        if (!fileSystem.exists(filePath)) {
            fileSystem.mkdirs(filePath);
        }
        return true;
    }

    /**
     * delete file path
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    private boolean deletePath(final Path filePath) throws IOException {
        if (!fileSystem.exists(filePath)) {
            return false;
        }
        // fSystem.delete(filePath, true);
        return fileSystem.delete(filePath, true);
    }


}
