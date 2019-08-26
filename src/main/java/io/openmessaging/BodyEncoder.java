package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-26
 */
public class BodyEncoder extends AbstractEncoder {
    private byte[] prevBody;

    public BodyEncoder(ByteBuffer buf) {
        super(buf);
    }


}
