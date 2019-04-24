package org.apache.crail.namenode;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.util.*;

public class DPScheduler implements Scheduler {
    private static Logger LOG = CrailUtils.getLogger();
    List<SchedDataInfo> schedDataInfos;

    DPScheduler() {
        this.schedDataInfos = new ArrayList<>();
    }

    // 更新SchedDataInfo 项的内容
    // 或者新增数据
    public void update(DataNodeBlocks val) {
        SchedDataInfo value = new SchedDataInfo(val);
        if (schedDataInfos.contains(value)) {
            int index = schedDataInfos.indexOf(value);
            schedDataInfos.get(index).update(val);
        } else {
            schedDataInfos.add(value);
        }
    }

    @Override
    public SchedDataInfo getResult(double Mt) {
        LOG.debug("DPScheduler : {}", schedDataInfos);

        SchedDataInfo[] arr = new SchedDataInfo[schedDataInfos.size()];
        schedDataInfos.toArray(arr);

        System.out.println("DPScheduler Info: " + arr);

        int size = arr.length;
        int[] n = new int[size];
        int[] r = new int[size];
        double[] t = new double[size];

        // 从schedDataInfo中获取有用数据
        for (int i = 0; i < size; i++) {
            n[i] = arr[i].getBlockNum();
            r[i] = arr[i].getRemainingBlockNum();
            t[i] = arr[i].getTime();
        }

        //-------------之后的实现与crail无关-------------
        //-------------使用的变量有：Mt,size,n,r,t -------

        double[] v = new double[size];  // 各节点价值
        double W = 0;
        int meshSize = 10;

        if (size == 0) return null;

        // 初始化数据
        for (int i = 0; i < size; i++) {
            v[i] = Mt / t[i];
            if (r[i] == 0) continue;
            W += (double) 2 * n[i] / r[i];
        }

        // 记录各节点消耗，随存储量的变化而变化
        int[][] w = new int[size][];
        for (int i = 0; i < size; i++) {
            w[i] = new int[r[i]+1];
            for (int j = 0; j < r[i]; j++) {
                w[i][j] = n[i] * meshSize / (r[i] - j);  // 不会出现除0的情况
            }
            w[i][r[i]] = 2147483647;
        }

//        // 找出所有可能的重量变化并排序
//        Double[] weightNumber = new Double[weightSet.size()];
//        int weightSize = weightNumber.length;
//        weightSet.toArray(weightNumber);
//        Arrays.sort(weightNumber);
//        for (int i = 0; i < weightSize; i++) {
//            if (weightNumber[i] > W) {
//                weightSize = i + 1;
//            }
//        }
//
//        System.out.println("weightNumber: " + weightNumber);

        // 以0.1为粒度整型化
        int weightSize = (int) (W * meshSize);
        double[] M = new double[weightSize + 1];
        int[] tmp = new int[weightSize + 1]; // 记录这样分配时，使用的最小层级

        for (int i = 0; i <= weightSize; i++) {
            M[i] = 0;
            tmp[i] = 0;
        }

        for (int i = 0; i < size; i++) {
            for (int j = weightSize; j >= w[i][0]; j--) {
                for (int k = 0; j - w[i][k] >= 0; k++) {
                    if (M[j] < k * v[i] + M[j - w[i][k]]) {
                        M[j] = k * v[i] + M[j - w[i][k]];

                        if (M[j - w[i][k]] == 0) {
                            tmp[j] = i;  // 记录使用的最小层
                        }
                    }
                }
            }
        }

        return new SchedDataInfo(arr[tmp[weightSize]].getStorageClass(), arr[tmp[weightSize]].getLocationClass());
    }

    // 实现起来有点麻烦
    private int mergeSort(double[] weightNumber, double[][] w, int weightSize, int size) {
        for (int i = 0; i < weightSize; i++) {
            int[] point = new int[size];  //记录各数组合并到的位置
            for (int j = 0; j < size; j++) point[j] = 0;
        }
        return 0;
    }

}
