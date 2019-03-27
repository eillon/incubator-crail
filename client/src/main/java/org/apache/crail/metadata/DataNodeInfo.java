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
import java.util.Arrays;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class DataNodeInfo {
    private static final Logger LOG = CrailUtils.getLogger();
    public static final int CSIZE = 52;

    private int storageType;
    private int storageClass;
    private int locationClass;
    private byte[] ipAddress;
    private int port;
    private String[] storageName;
    private double M;
    private double w;
    private double H;
    private double d;

    private long key;

    public DataNodeInfo() {
        this.storageType = 0;
        this.storageClass = 0;
        this.locationClass = 0;
        this.ipAddress = new byte[4];
        this.port = 0;
        this.key = 0;
        this.M = 0;
        this.w = 0;
        this.H = 0;
        this.d = 0;
    }

    public DataNodeInfo(int storageType, int storageClass, int locationClass, byte[] ipAddress, int port, double M, double w, double H, double d) {
        this();

        this.storageType = storageType;
        this.storageClass = storageClass;
        this.locationClass = locationClass;
        for (int i = 0; i < ipAddress.length; i++) {
            this.ipAddress[i] = ipAddress[i];
        }
        this.port = port;
        this.M = M;
        this.w = w;
        this.H = H;
        this.d = d;
    }

    void setDataNodeInfo(DataNodeInfo info) {
        this.storageType = info.getStorageType();
        this.storageClass = info.getStorageClass();
        this.locationClass = info.getLocationClass();
        for (int i = 0; i < ipAddress.length; i++) {
            this.ipAddress[i] = info.getIpAddress()[i];
        }
        this.port = info.getPort();
        this.M = info.getM();
        this.w = info.getW();
        this.H = info.getH();
        this.d = info.getD();

        this.key = 0;
    }

    public int write(ByteBuffer buffer) {
        buffer.putInt(storageType);
        buffer.putInt(storageClass);
        buffer.putInt(locationClass);
        buffer.put(ipAddress);
        buffer.putInt(port);
        buffer.putDouble(M);
        buffer.putDouble(w);
        buffer.putDouble(H);
        buffer.putDouble(d);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) throws UnknownHostException {
        this.storageType = buffer.getInt();
        this.storageClass = buffer.getInt();
        this.locationClass = buffer.getInt();
        buffer.get(ipAddress);
        this.port = buffer.getInt();
        this.M = buffer.getDouble();
        this.w = buffer.getDouble();
        this.H = buffer.getDouble();
        this.d = buffer.getDouble();
        this.key = 0;
    }

    public byte[] getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public int getLocationClass() {
        return locationClass;
    }

    public int getStorageType() {
        return storageType;
    }

    public int getStorageClass() {
        return storageClass;
    }

    public double getM() {
        return M;
    }

    public double getW() {
        return w;
    }

    public double getH() {
        return H;
    }

    public double getD() {
        return d;
    }

    public long key() {
        if (key == 0) {
            int a = java.util.Arrays.hashCode(ipAddress);
            key = (((long) a) << 32) | (port & 0xffffffffL);
        }
        return key;
    }

    @Override
    public String toString() {
        return "DataNodeInfo [storageType=" + storageType + ", storageClass="
                + storageClass + ", locationClass=" + locationClass
                + ", ipAddress=" + Arrays.toString(ipAddress) + ", port="
                + port + ", M=" + M + ", w=" + w + ", H=" + H + ", d="
                + d + ", key=" + key() + "]";
    }
}
