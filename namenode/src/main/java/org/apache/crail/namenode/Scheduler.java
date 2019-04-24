package org.apache.crail.namenode;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public interface Scheduler {
    abstract SchedDataInfo getResult(double Mt);

    abstract void update(DataNodeBlocks dataNodeInfo);
}

class SchedDataInfo {
    private double time;
    private int remainingBlockNum;
    private int blockNum;
    private int storageClass;
    private int locationClass;

    private long key;

    // 使用DataNodeBlocks初始化而不是DataNodeInfo
    // 因为里面有剩余块信息
    SchedDataInfo(DataNodeBlocks dataNodeInfo) {
        storageClass = dataNodeInfo.getStorageClass();
        locationClass = dataNodeInfo.getLocationClass();

        // 无需区分storageType，因为用的同一个模型
        // 在getNetworkConsumption中会区分网络
        time = CrailUtils.getStorageConsumption(dataNodeInfo.getM(), dataNodeInfo.getW(), CrailConstants.BLOCK_SIZE)
                + CrailUtils.getNetworkConsumption(dataNodeInfo.getH(), dataNodeInfo.getD(), CrailConstants.BLOCK_SIZE, CrailConstants.BLOCK_SIZE, dataNodeInfo.getNetType());
        blockNum = dataNodeInfo.getBlockCount();  // 初始化时的BlockCount，之后不更新
        remainingBlockNum = dataNodeInfo.getBlockCount();
        key = dataNodeInfo.key();
    }

    SchedDataInfo(int storageClass, int locationClass) {
        this.storageClass = storageClass;
        this.locationClass = locationClass;
        this.time = 0;
        this.blockNum = 0;
        this.remainingBlockNum = 0;
        this.key = 0;
    }

    void update(DataNodeBlocks dataNodeInfo) {
        this.remainingBlockNum = dataNodeInfo.getBlockCount();
        this.time = CrailUtils.getStorageConsumption(dataNodeInfo.getM(), dataNodeInfo.getW(), CrailConstants.BLOCK_SIZE)
                + CrailUtils.getNetworkConsumption(dataNodeInfo.getH(), dataNodeInfo.getD(), CrailConstants.BLOCK_SIZE, CrailConstants.BLOCK_SIZE, dataNodeInfo.getNetType());
    }

    public double getTime() {
        return time;
    }

    public int getLocationClass() {
        return locationClass;
    }

    public int getStorageClass() {
        return storageClass;
    }

    public int getRemainingBlockNum() {
        return remainingBlockNum;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public long getKey() {
        return key;
    }

    public void setTime(double time) {
        this.time = time;
    }

    public void setStorageClass(int storageClass) {
        this.storageClass = storageClass;
    }

    public void setLocationClass(int locationClass) {
        this.locationClass = locationClass;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SchedDataInfo) {
            return ((SchedDataInfo) obj).getKey() == this.key;
        }
        return false;
    }

    @Override
    public String toString() {
        return "[storageClass=" + storageClass + ", locationClass=" + locationClass + ", time=" + time +
                ", blockNum=" + blockNum + ", remainBlockNum=" + remainingBlockNum + ", key=" + key + "]";
    }
}