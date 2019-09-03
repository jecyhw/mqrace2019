package io.openmessaging.model;

import io.openmessaging.Const;
import io.openmessaging.Message;
import io.openmessaging.codec.TDecoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetMsgItem {
    public ByteBuffer readBuf = ByteBuffer.allocateDirect(Const.MAX_GET_AT_SIZE * Const.MSG_BYTES);
    public final long[] as = new long[Const.MAX_GET_AT_SIZE];
    public final long[] ts = new long[Const.MAX_GET_AT_SIZE];
    public List<Message> messages;
    public TDecoder tDecoder = new TDecoder();
}
