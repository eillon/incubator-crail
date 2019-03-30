package org.apache.crail.namenode;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public interface Scheduler {
    abstract SchedDataInfo getRoot();

    abstract void update(SchedDataInfo schedDataInfo);
}

class SchedDataInfo {
    private double time;
    private int storageClass;
    private int locationClass;

    private long key;

    SchedDataInfo(DataNodeInfo dataNodeInfo) {
        storageClass = dataNodeInfo.getStorageClass();
        locationClass = dataNodeInfo.getLocationClass();

        // 无需区分storageType，因为用的同一个模型
        // 在getNetworkConsumption中会区分网络
        time = CrailUtils.getStorageConsumption(dataNodeInfo.getM(), dataNodeInfo.getW(), CrailConstants.BLOCK_SIZE)
                + CrailUtils.getNetworkConsumption(dataNodeInfo.getH(), dataNodeInfo.getD(), CrailConstants.BLOCK_SIZE, CrailConstants.BLOCK_SIZE, dataNodeInfo.getNetType());
        key = dataNodeInfo.key();
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

    public long getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SchedDataInfo) {
            return ((SchedDataInfo) obj).key == this.key;
        }
        return false;
    }

    @Override
    public String toString() {
        return "[storageClass=" + storageClass + ", locationClass=" + locationClass + ", time=" + time + ", key=" + key + "]";
    }
}