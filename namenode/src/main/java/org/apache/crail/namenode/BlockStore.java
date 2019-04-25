/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.namenode;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.AtomicIntegerModulo;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

/**
 * BlockStore 负责整体的块管理；里面分层存储着storageClass；
 * StorageClass: 管理这一层内块的分配
 * DataNodeArray：多个DataNodeBlocks的ArrayList集合。这里的多个DataNodeBlocks可以是同一层的，也可以是同层同主机的
 * DataNodeBlocks：一个storageTiers（dataNode）上所有的块，是一个队列。
 */
public class BlockStore {
    private static final Logger LOG = CrailUtils.getLogger();

    private StorageClass[] storageClasses;
    private Scheduler scheduler;

    public BlockStore() {
        storageClasses = new StorageClass[CrailConstants.STORAGE_CLASSES];
        for (int i = 0; i < CrailConstants.STORAGE_CLASSES; i++) {
            this.storageClasses[i] = new StorageClass(i);
        }
        scheduler = new DPScheduler();
    }

    public short addBlock(NameNodeBlockInfo blockInfo) throws UnknownHostException {
        int storageClass = blockInfo.getDnInfo().getStorageClass();
        LOG.debug("BlockStore: addBlock {} to storageClass {}", blockInfo, storageClass);
        short err = storageClasses[storageClass].addBlock(blockInfo);
        scheduler.update(getDataNode(blockInfo.getDnInfo()));
        return err;
    }

    public boolean regionExists(BlockInfo region) {
        int storageClass = region.getDnInfo().getStorageClass();
        return storageClasses[storageClass].regionExists(region);
    }

    public short updateRegion(BlockInfo region) {
        int storageClass = region.getDnInfo().getStorageClass();
        short err = storageClasses[storageClass].updateRegion(region);
        scheduler.update(getDataNode(region.getDnInfo()));
        return err;
    }

    // -- 严重怀疑，locationClass的值传不到这里：不能为负
    public NameNodeBlockInfo getBlock(int storageClass, int locationAffinity, double Mt) throws InterruptedException {
        NameNodeBlockInfo block = null;
        if (storageClass > 0) {
            if (storageClass < storageClasses.length) {
                block = storageClasses[storageClass].getBlock(locationAffinity);
            } else {
                //TODO: warn if requested storage class is invalid
                LOG.warn("requested storage class is invalid");
            }
        }

        if (storageClass == 0 && locationAffinity == 0) {
            LOG.debug("DPScheduler begin");
            SchedDataInfo schedDataInfo = scheduler.getResult(Mt);

            if (schedDataInfo != null) {
                storageClass = schedDataInfo.getStorageClass();
                locationAffinity = schedDataInfo.getLocationClass();
                LOG.debug("BlockStore: scheduler: schedDataInfo is {}", schedDataInfo);
                block = storageClasses[storageClass].getBlock(locationAffinity);
            }
        }

        if (block == null) {
            for (int i = 0; i < storageClasses.length; i++) {
                block = storageClasses[i].getBlock(locationAffinity);
                if (block != null) {
                    break;
                }
            }
        }

        if (block != null){
            scheduler.update(getDataNode(block.getDnInfo()));
        }

        LOG.debug("BlockStore: getBlock {}， storageClass is {}, locationAffinity is {}, real storageClass is {}, real locationClass is {}", block, storageClass, locationAffinity, block.getDnInfo().getStorageClass(), block.getDnInfo().getLocationClass());
        return block;
    }

    public DataNodeBlocks getDataNode(DataNodeInfo dnInfo) {
        int storageClass = dnInfo.getStorageClass();
        return storageClasses[storageClass].getDataNode(dnInfo);
    }

}

class StorageClass {
    private static final Logger LOG = CrailUtils.getLogger();

    private int storageClass;
    private ConcurrentHashMap<Long, DataNodeBlocks> membership;
    private ConcurrentHashMap<Integer, DataNodeArray> affinitySets;
    private DataNodeArray anySet;
    private BlockSelection blockSelection;

    public StorageClass(int storageClass) {
        this.storageClass = storageClass;
        this.membership = new ConcurrentHashMap<Long, DataNodeBlocks>();
        this.affinitySets = new ConcurrentHashMap<Integer, DataNodeArray>();
        if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("roundrobin")) {
            this.blockSelection = new RoundRobinBlockSelection();
        } else {
            this.blockSelection = new RandomBlockSelection();
        }
        this.anySet = new DataNodeArray(blockSelection);
    }

    public short updateRegion(BlockInfo region) {
        long dnAddress = region.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            return RpcErrors.ERR_ADD_BLOCK_FAILED;
        } else {
            return current.updateRegion(region);
        }
    }

    public boolean regionExists(BlockInfo region) {
        long dnAddress = region.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            return false;
        } else {
            return current.regionExists(region);
        }
    }

    short addBlock(NameNodeBlockInfo block) throws UnknownHostException {
        long dnAddress = block.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            current = DataNodeBlocks.fromDataNodeInfo(block.getDnInfo());
            addDataNode(current);
        }

        current.touch();
        current.addFreeBlock(block);
        return RpcErrors.ERR_OK;
    }

    NameNodeBlockInfo getBlock(int affinity) throws InterruptedException {
        NameNodeBlockInfo block = null;
        if (affinity == 0) {
            block = anySet.get();
            LOG.debug("StroageClass: get Block from anySet, {}, affinity is {}", block, affinity);
        } else {
            block = _getAffinityBlock(affinity);
            if (block == null) {
                block = anySet.get();
                LOG.debug("StroageClass: get Block from anySet, {} affinity is {}", block, affinity);
            } else {
            }
        }
        return block;
    }

    DataNodeBlocks getDataNode(DataNodeInfo dataNode) {
        return membership.get(dataNode.key());
    }

    short addDataNode(DataNodeBlocks dataNode) {
        DataNodeBlocks current = membership.putIfAbsent(dataNode.key(), dataNode);
        if (current != null) {
            return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
        }

        // current == null, datanode not in set, adding it now
        _addDataNode(dataNode);

        return RpcErrors.ERR_OK;

    }

    //---------------

    private void _addDataNode(DataNodeBlocks dataNode) {
        LOG.info("adding datanode {}:{} of type {} to storage class {}; \n\t datanode info : {}", CrailUtils.getIPAddressFromBytes(dataNode.getIpAddress()),
                dataNode.getPort(), dataNode.getStorageType(), storageClass, dataNode);
        DataNodeArray hostMap = affinitySets.get(dataNode.getLocationClass());
        if (hostMap == null) {
            hostMap = new DataNodeArray(blockSelection);
            DataNodeArray oldMap = affinitySets.putIfAbsent(dataNode.getLocationClass(), hostMap);
            if (oldMap != null) {
                hostMap = oldMap;
            }
        }
        hostMap.add(dataNode);
        anySet.add(dataNode);
    }

    private NameNodeBlockInfo _getAffinityBlock(int affinity) throws InterruptedException {
        NameNodeBlockInfo block = null;
        DataNodeArray affinitySet = affinitySets.get(affinity);
        if (affinitySet != null) {
            block = affinitySet.get();
            LOG.debug("BlockStore: get block from affinitySet, block is {}", block);
        } else {
            LOG.warn("affinity {} is null, affinitySets is {}", affinity, affinitySets);
        }
        return block;
    }

    public static interface BlockSelection {
        int getNext(int size);
    }

    private class RoundRobinBlockSelection implements BlockSelection {
        private AtomicIntegerModulo counter;

        public RoundRobinBlockSelection() {
            LOG.info("round robin block selection");
            counter = new AtomicIntegerModulo();
        }

        @Override
        public int getNext(int size) {
            LOG.debug("--------------- getNext size: {} --------------------", size);
            return counter.getAndIncrement() % size;
        }
    }

    private class RandomBlockSelection implements BlockSelection {
        public RandomBlockSelection() {
            LOG.info("random block selection");
        }

        @Override
        public int getNext(int size) {
            return ThreadLocalRandom.current().nextInt(size);
        }
    }

    private class DataNodeArray {
        private ArrayList<DataNodeBlocks> arrayList;
        private ReentrantReadWriteLock lock;
        private BlockSelection blockSelection;

        public DataNodeArray(BlockSelection blockSelection) {
            this.arrayList = new ArrayList<DataNodeBlocks>();
            this.lock = new ReentrantReadWriteLock();
            this.blockSelection = blockSelection;
        }

        public void add(DataNodeBlocks dataNode) {
            lock.writeLock().lock();
            try {
                arrayList.add(dataNode);
            } finally {
                lock.writeLock().unlock();
            }
        }

        private NameNodeBlockInfo get() throws InterruptedException {
            lock.readLock().lock();
            try {
                NameNodeBlockInfo block = null;
                int size = arrayList.size();
                LOG.debug("BlockStore: StorageClass: DataNodeArray: get: DataNodeArray's size is {}", size);
                if (size > 0) {
                    // 这里决定了同一个DataNodeArray内怎么分配。DataNodeArray可以是一整个storageClass层，也可以是指定locationAffinity的、同层同主机DataNodeArray
                    // 同层同主机可以有多个storageTier  但一般是一个
                    int startIndex = blockSelection.getNext(size);
                    for (int i = 0; i < size; i++) {
                        int index = (startIndex + i) % size;
                        DataNodeBlocks anyDn = arrayList.get(index);
                        if (anyDn.isOnline()) {
                            block = anyDn.getFreeBlock();
                        }
                        if (block != null) {
                            break;
                        }
                    }
                }
                return block;
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}

