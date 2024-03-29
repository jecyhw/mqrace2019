package io.openmessaging.model;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class IntervalSum {
    public long sum = 0;
    public int count = 0;

    public void reset() {
        sum = 0;
        count = 0;
    }

    public void add(long sum, int count) {
        this.sum += sum;
        this.count += count;
    }

    public void remove(long sum, int count) {
        this.sum -= sum;
        this.count -= count;
    }

    public long avg() {
        return count == 0 ? 0 : sum / count;
    }
}
