package org.apache.crail.namenode;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class MinHeapScheduler implements Scheduler {
    private static Logger LOG = CrailUtils.getLogger();
    List<SchedDataInfo> heap;

    MinHeapScheduler() {
        this.heap = new ArrayList<>();
    }

    private void swap(int a, int b) {
        SchedDataInfo tmp = heap.get(a);
        heap.set(a, heap.get(b));
        heap.set(b, tmp);
    }

    public void delete(int index) {
        heap.set(index, heap.get(heap.size() - 1));
        heap.remove(heap.size() - 1);
        adjust(index);
    }

    private void heapUp(int index) {
        LOG.debug("MinHeapScheduler: heapUp at index: {}", index);
        if (index >= 1) {
            int parent = index / 2;
            double parentValue = heap.get(parent).getTime();
            double indexValue = heap.get(index).getTime();

            if (parentValue > indexValue) {
                swap(parent, index);
                heapUp(parent);
            }
        }
    }

    private void heapDown(int index) {
        LOG.debug("MinHeapScheduler: heapDown at index: {}", index);
        int n = heap.size() - 1;
        double indexValue = heap.get(index).getTime();

        if (2 * index >= n) {  // 不存在子节点, 结束
            return;
        } else if (2 * index + 1 == n) {  // 只有一个左子节点
            double childValue = heap.get(2 * index + 1).getTime();
            if (indexValue > childValue) {
                swap(index, 2 * index + 1);
                heapDown(2 * index + 1);
            }
        } else {
            double leftChildValue = heap.get(2 * index + 1).getTime();
            double rightChildValue = heap.get(2 * index + 2).getTime();
            if (leftChildValue > rightChildValue) {
                if (indexValue > leftChildValue || indexValue > rightChildValue) {
                    swap(2 * index + 2, index);
                    heapDown(2 * index + 2);
                }
            } else {
                if (indexValue > rightChildValue || indexValue > leftChildValue) {
                    swap(2 * index + 1, index);
                    heapDown(2 * index + 1);
                }
            }
        }
    }

    private void add(SchedDataInfo value) {
        LOG.debug("MinHeapScheduler: add {}", value);
        heap.add(value);
        heapUp(heap.size() - 1);
    }

    // 调用这个即可，如果没有则增加
    public void update(DataNodeBlocks val) {
        SchedDataInfo value = new SchedDataInfo(val);
        if (heap.contains(value)) {
            int index = heap.indexOf(value);
            heap.get(index).update(val);
            adjust(index);
        } else {
            add(value);
        }
    }

    private void adjust(int index) {
        LOG.debug("MinHeapScheduler: adjust index: {}", index);
        double indexValue = heap.get(index).getTime();
        if (index == 0) {
            heapDown(index);
        } else {
            int parent = index / 2;
            if (heap.get(parent).getTime() > indexValue) {
                heapUp(index);
            } else {
                heapDown(index);
            }
        }
    }

    public void build() {
        for (int i = 1; i < heap.size(); i++) {
            heapUp(i);
        }
    }

    private String printHeap() {
        return this.heap.toString();
    }

    public SchedDataInfo getResult(double Mt) {
        LOG.debug("MinHeapScheduler: printHeap: {}", printHeap());
        return heap.get(0);
    }


}
