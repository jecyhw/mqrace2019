package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class MemoryIndex {
    int firstValue;
    int count = 0;
    List<Memory> memories = new ArrayList<>();
    Memory curMemory = new Memory();

    public MemoryIndex() {
        memories.add(curMemory);
    }

    public void putRelativeT(int t, int prevT) {
        int diffT = prevT - t;
        if (!curMemory.put(diffT)) {
            curMemory = new Memory();
            memories.add(curMemory);
            curMemory.put(diffT);
        }
        count++;
    }

    public void setFirstValue(int firstValue) {
        this.firstValue = firstValue;
    }
}
