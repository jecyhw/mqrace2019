package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetStat {
    long maxT = Long.MIN_VALUE;
    long minT = Long.MAX_VALUE;
    long diffMaxT = Long.MIN_VALUE;
    long diffMinT = Long.MAX_VALUE;
    long maxA = Long.MIN_VALUE;
    long minA = Long.MAX_VALUE;
    long diffMaxA = Long.MIN_VALUE;
    long diffMinA = Long.MAX_VALUE;
    AtomicInteger count = new AtomicInteger(0);
}
