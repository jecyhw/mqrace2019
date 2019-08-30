package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class IntervalSum {
    public long sum;
    public int count;

    public void reset() {
        sum = 0;
        count = 0;
    }

    public void add(long sum, int count) {
        this.sum += sum;
        this.count += count;
    }

    public long avg() {
        return count == 0 ? 0 : sum / count;
    }
}
