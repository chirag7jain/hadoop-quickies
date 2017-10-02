package me.chirag7jain;

// SRC - https://developer.yahoo.com/hadoop/tutorial/module2.html

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class HDFSHelloWorld {
    private static final String theFilename = "hello.txt";
    private static final String message = "Hello, world!\n";

    public static void main (String [] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filenamePath = new Path(theFilename);

        try(FSDataOutputStream out = fs.create(filenamePath);FSDataInputStream in = fs.open(filenamePath);) {
            // remove the file first
            if (fs.exists(filenamePath)) fs.delete(filenamePath);

            out.writeUTF(message);

            String messageIn = in.readUTF();
            System.out.print(messageIn);
        }
        catch (IOException ioe) {
            System.err.println("IOException during operation: " + ioe.toString());
            System.exit(1);
        }
    }
}

