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

package org.apache.crail.metadata;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/*
	DataNodeStatistics不仅应该有freeblocks，还应该有M、w、H、d
 */
public class DataNodeStatistics {
    public static final int CSIZE = 48;

    private long serviceId;
    private int freeBlockCount;
    private double M;
    private double w;
    private double H;
    private double d;

    private int netType;

    public DataNodeStatistics() {
        this.serviceId = 0;
        this.freeBlockCount = 0;
        this.M = 0;
        this.H = 0;
        this.w = 0;
        this.d = 0;
        this.netType = 0;
    }

    public int write(ByteBuffer buffer) {
        buffer.putLong(serviceId);
        buffer.putInt(freeBlockCount);
        buffer.putDouble(M);
        buffer.putDouble(w);
        buffer.putDouble(H);
        buffer.putDouble(d);
        buffer.putInt(netType);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) throws UnknownHostException {
        this.serviceId = buffer.getLong();
        this.freeBlockCount = buffer.getInt();
        this.M = buffer.getDouble();
        this.w = buffer.getDouble();
        this.H = buffer.getDouble();
        this.d = buffer.getDouble();
        this.netType = buffer.getInt();
    }

    public int getFreeBlockCount() {
        return freeBlockCount;
    }

    public void setFreeBlockCount(int blockCount) {
        this.freeBlockCount = blockCount;
    }

    public void setStatistics(DataNodeStatistics statistics) {
        this.serviceId = statistics.getServiceId();
        this.freeBlockCount = statistics.getFreeBlockCount();
        this.M = statistics.getM();
        this.w = statistics.getW();
        this.H = statistics.getH();
        this.d = statistics.getD();
        this.netType = statistics.getNetType();
    }

    public void setServiceId(long serviceId) {
        this.serviceId = serviceId;
    }

    public long getServiceId() {
        return serviceId;
    }


    public double getM() {
        return M;
    }

    public void setM(double m) {
        M = m;
    }

    public double getW() {
        return w;
    }

    public void setW(double w) {
        this.w = w;
    }

    public double getH() {
        return H;
    }

    public void setH(double h) {
        H = h;
    }

    public double getD() {
        return d;
    }

    public void setD(double d) {
        this.d = d;
    }

    public int getNetType() {
        return netType;
    }

    public void setNetType(int netType) {
        this.netType = netType;
    }

    @Override
    public String toString() {
        return "DataNodeStatistics [serviceId=" + serviceId + ", freeBlockCount="
                + freeBlockCount + ", M=" + M + ", w=" + w + ", H=" + H + ", d=" + d + ", netType=" + netType + "]";
    }
}
