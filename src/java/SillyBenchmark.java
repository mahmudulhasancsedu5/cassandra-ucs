import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.TimeUnit;

import sun.nio.ch.DirectBuffer;


public class SillyBenchmark {
    static boolean syncPeriodically = false;
    public static void main(String[] args) throws Exception {
        System.out.println("Testing with sync at end");
        tests();
        syncPeriodically = true;
        System.out.println("Testing with periodic syncing");
        tests();

    }
    
    static void tests() throws Exception {
        long size =  Integer.MAX_VALUE;
        File f;
        
        f = getFile(null);
        try (RandomAccessFile fos = new RandomAccessFile(f, "rw")) {
            MappedByteBuffer mbb = fos.getChannel().map(MapMode.READ_WRITE, 0, size);
            try {
                measureMapped(mbb, false);
            } finally {
                ((DirectBuffer)mbb).cleaner().clean();
            }
        } finally {
            f.delete();
        }
        
        f = getFile(size);
        try (RandomAccessFile fos = new RandomAccessFile(f, "rw")) {
            MappedByteBuffer mbb = fos.getChannel().map(MapMode.READ_WRITE, 0, size);
            try {
                measureMapped(mbb, true);
            } finally {
                ((DirectBuffer)mbb).cleaner().clean();
            }       } finally {
            f.delete();
        }
        
        f = getFile(null);
        try (FileOutputStream fos = new FileOutputStream(f)) {
            measureChannel(fos.getChannel(), size, false);
        } finally {
            f.delete();
        }
        
        f = getFile(size);
        try (FileOutputStream fos = new FileOutputStream(f)) {
            measureChannel(fos.getChannel(), size, true);
        } finally {
            f.delete();
        }
    }
    
    static void measureChannel(FileChannel fc, long size, boolean preallocated) throws Exception {
        long start = System.nanoTime();

        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 2);
        int i = 1234567890;
        while (buf.remaining() > 0)
            buf.putInt(i++);
        ByteBuffer b = ByteBuffer.allocateDirect(1024 * 1024 * 2);
        
        long startSize = size;
        int counter = 0;
        while (startSize > 0) {
            buf.clear();
            b.clear();
            startSize -= buf.remaining();
            b.put(buf);
            b.flip();
            fc.write(b);      
            
            counter++;
            if (syncPeriodically && counter % 16 == 0) {
                fc.force(true);
            }
        }
        fc.force(true);
        long delta = System.nanoTime() - start;
        System.out.println((preallocated ? "Preallocated " : "") +"Channel took " + TimeUnit.NANOSECONDS.toMillis(delta));
    }
    
    static void measureMapped(MappedByteBuffer b, boolean preallocated) throws Exception {
        long start = System.nanoTime();
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 2);
        int i = 1234567890;
        while (buf.remaining() > 0)
            buf.putInt(i++);
        int counter = 0;

        while (b.hasRemaining()) {
            buf.clear();
            buf.limit(Math.min(b.remaining(), buf.remaining()));
            b.put(buf);
            
            counter++;
            if (syncPeriodically && counter % 16 == 0) {
                b.force();
            }
        }
        b.force();
        long delta = System.nanoTime() - start;
        System.out.println((preallocated ? "Preallocated " : "") + "Mapped took " + TimeUnit.NANOSECONDS.toMillis(delta));
    }
    
    static File getFile(Long size) throws Exception {
        File f = File.createTempFile("bar", "foo");
//      System.out.println("File path is " + f.getPath());
        f.deleteOnExit();
        if (size != null) {
            try (FileOutputStream fos = new FileOutputStream(f)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
                long start = size;
                while (start > 0) {
                    buf.clear();
                    start -= buf.remaining();
                    fos.getChannel().write(buf);
                }
                fos.getChannel().force(false);
            }
        }
        return f;
    }
}