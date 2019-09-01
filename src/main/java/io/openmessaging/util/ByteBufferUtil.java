package io.openmessaging.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yanghuiwei on 2019-09-01
 */
public class ByteBufferUtil {
    public static void readInBuf(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
        } catch (IOException e) {
            Utils.print(e.getMessage() + " method:readInBuf");
            System.exit(-1);
        }
    }

    public static void flush(ByteBuffer byteBuffer, FileChannel fc){
        byteBuffer.flip();
        try {
            while (byteBuffer.hasRemaining()){
                fc.write(byteBuffer);
            }
        } catch (IOException e) {
            Utils.print("func=flush " + e.getMessage());
            System.exit(-1);
        }
        byteBuffer.clear();
    }
}
