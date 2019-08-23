package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class IntervalSum {
    long sum;
    int count;

    void reset() {
        sum = 0;
        count = 0;
    }

    void add(long sum, int count) {
        this.sum += sum;
        this.count += count;
    }

    public long avg() {
        return count == 0 ? 0 : sum / count;
    }
}
