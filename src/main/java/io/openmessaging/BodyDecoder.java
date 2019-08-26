package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-26
 */
public class BodyDecoder extends AbstractEncoder {
    public BodyDecoder(ByteBuffer buf) {
        super(buf);
    }
}
