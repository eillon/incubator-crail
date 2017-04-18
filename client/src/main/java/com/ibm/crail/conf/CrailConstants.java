/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.conf;

import java.io.IOException;
import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;

public class CrailConstants {
	private static final Logger LOG = CrailUtils.getLogger();
	
	public static final String VERSION_KEY = "crail.version";
	public static int VERSION = 2815;
	
	public static final String STORAGE_TYPES_KEY = "crail.storage.types";
	public static String STORAGE_TYPES = "com.ibm.crail.storage.rdma.RdmaDataNode";		

	public static final String DIRECTORY_DEPTH_KEY = "crail.directory.depth";
	public static int DIRECTORY_DEPTH = 16;
	
	public static final String TOKEN_EXPIRATION_KEY = "crail.token.expiration";
	public static long TOKEN_EXPIRATION = 10;
	
	public static final String BLOCK_SIZE_KEY = "crail.blocksize";
	public static long BLOCK_SIZE = 67108864;
	
	public static final String CACHE_LIMIT_KEY = "crail.cachelimit";
	public static long CACHE_LIMIT = 1073741824;
	
	public static final String CACHE_PATH_KEY = "crail.cachepath";
	public static String CACHE_PATH = "/home/stu/craildata/cache";	
	
	public static final String USER_KEY = "crail.user";
	public static String USER = "stu";
	
	public static final String SHADOW_REPLICATION_KEY = "crail.shadow.replication";
	public static int SHADOW_REPLICATION = 1;	
	
	public static final String DEBUG_KEY = "crail.debug";
	public static boolean DEBUG = false;
	
	public static final String STATISTICS_KEY = "crail.statistics";
	public static boolean STATISTICS = true;	
	
	public static final String RPC_TIMEOUT_KEY = "crail.rpc.timeout";
	public static int RPC_TIMEOUT = 1000;
	
	public static final String DATA_TIMEOUT_KEY = "crail.data.timeout";
	public static int DATA_TIMEOUT = 1000;	
	
	public static final String BUFFER_SIZE_KEY = "crail.buffersize";
	public static int BUFFER_SIZE = 1048576;
	
	public static final String SINGLETON_KEY = "crail.singleton";
	public static boolean SINGLETON = false;	
	
	public static final String REGION_SIZE_KEY = "crail.regionsize";
	public static long REGION_SIZE = 1073741824;
	
	public static final String DIRECTORY_RECORD_KEY = "crail.directoryrecord";
	public static int DIRECTORY_RECORD = 512;	
	
	public static final String DIRECTORY_RANDOMIZE_KEY = "crail.directoryrandomize";
	public static boolean DIRECTORY_RANDOMIZE = true;
	
	public static final String CACHE_IMPL_KEY = "crail.cacheimpl";
	public static String CACHE_IMPL = "com.ibm.crail.utils.MappedBufferCache";		
	
	public static final String NAMENODE_ADDRESS_KEY = "crail.namenode.address";
	public static String NAMENODE_ADDRESS = "";
	
	public static final String NAMENODE_FILEBLOCKS_KEY = "crail.namenode.fileblocks";
	public static int NAMENODE_FILEBLOCKS = 16;	
	
	public static final String NAMENODE_BLOCKSELECTION_KEY = "crail.namenode.blockselection";
	public static String NAMENODE_BLOCKSELECTION = "roundrobin";	
	
	public static final String NAMENODE_RPC_TYPE_KEY = "crail.namenode.rpc.type";
	public static String NAMENODE_RPC_TYPE = "com.ibm.crail.namenode.rpc.darpc.DaRPCNameNode";	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(STORAGE_TYPES_KEY) != null) {
			STORAGE_TYPES = conf.get(STORAGE_TYPES_KEY);
		}			
		if (conf.get(DIRECTORY_DEPTH_KEY) != null) {
			DIRECTORY_DEPTH = Integer.parseInt(conf.get(DIRECTORY_DEPTH_KEY));
		}			
		if (conf.get(TOKEN_EXPIRATION_KEY) != null) {
			TOKEN_EXPIRATION = Long.parseLong(conf.get(TOKEN_EXPIRATION_KEY));
		}		
		if (conf.get(BLOCK_SIZE_KEY) != null) {
			BLOCK_SIZE = Long.parseLong(conf.get(BLOCK_SIZE_KEY));
		}			
		if (conf.get(CACHE_LIMIT_KEY) != null) {
			CACHE_LIMIT = Long.parseLong(conf.get(CACHE_LIMIT_KEY));
		}			
		if (conf.get(CACHE_PATH_KEY) != null) {
			CACHE_PATH = conf.get(CACHE_PATH_KEY);
		}
		if (conf.get(USER_KEY) != null) {
			USER = conf.get(CrailConstants.USER_KEY);
		}
		if (conf.get(SHADOW_REPLICATION_KEY) != null) {
			SHADOW_REPLICATION = Integer.parseInt(conf.get(SHADOW_REPLICATION_KEY));
		}	
		if (conf.get(DEBUG_KEY) != null) {
			DEBUG = Boolean.parseBoolean(conf.get(DEBUG_KEY));
		}	
		if (conf.get(STATISTICS_KEY) != null) {
			STATISTICS = Boolean.parseBoolean(conf.get(STATISTICS_KEY));
		}			
		if (conf.get(RPC_TIMEOUT_KEY) != null) {
			RPC_TIMEOUT = Integer.parseInt(conf.get(RPC_TIMEOUT_KEY));
		}	
		if (conf.get(DATA_TIMEOUT_KEY) != null) {
			DATA_TIMEOUT = Integer.parseInt(conf.get(DATA_TIMEOUT_KEY));
		}	
		if (conf.get(BUFFER_SIZE_KEY) != null) {
			BUFFER_SIZE = Integer.parseInt(conf.get(BUFFER_SIZE_KEY));
		}	
		if (conf.get(CrailConstants.SINGLETON_KEY) != null) {
			SINGLETON = conf.getBoolean(CrailConstants.SINGLETON_KEY, false);
		}	
		if (conf.get(REGION_SIZE_KEY) != null) {
			REGION_SIZE = Integer.parseInt(conf.get(REGION_SIZE_KEY));
		}	
		if (conf.get(DIRECTORY_RECORD_KEY) != null) {
			DIRECTORY_RECORD = Integer.parseInt(conf.get(DIRECTORY_RECORD_KEY));
		}	
		if (conf.get(CrailConstants.DIRECTORY_RANDOMIZE_KEY) != null) {
			DIRECTORY_RANDOMIZE = conf.getBoolean(CrailConstants.DIRECTORY_RANDOMIZE_KEY, false);
		}
		if (conf.get(CACHE_IMPL_KEY) != null) {
			CACHE_IMPL = conf.get(CACHE_IMPL_KEY);
		}			
		if (conf.get(NAMENODE_ADDRESS_KEY) != null) {
			NAMENODE_ADDRESS = conf.get(NAMENODE_ADDRESS_KEY);
		} 
		if (conf.get(NAMENODE_BLOCKSELECTION_KEY) != null) {
			NAMENODE_BLOCKSELECTION = conf.get(NAMENODE_BLOCKSELECTION_KEY);
		}		
		if (conf.get(NAMENODE_FILEBLOCKS_KEY) != null) {
			NAMENODE_FILEBLOCKS = Integer.parseInt(conf.get(NAMENODE_FILEBLOCKS_KEY));
		}		
		if (conf.get(NAMENODE_RPC_TYPE_KEY) != null) {
			NAMENODE_RPC_TYPE = conf.get(NAMENODE_RPC_TYPE_KEY);
		}
	}
	
	public static void printConf(){
		LOG.info(VERSION_KEY + " " + VERSION);
		LOG.info(STORAGE_TYPES_KEY + " " + STORAGE_TYPES);
		LOG.info(DIRECTORY_DEPTH_KEY + " " + DIRECTORY_DEPTH);
		LOG.info(TOKEN_EXPIRATION_KEY + " " + TOKEN_EXPIRATION);
		LOG.info(BLOCK_SIZE_KEY + " " + BLOCK_SIZE);
		LOG.info(CACHE_LIMIT_KEY + " " + CACHE_LIMIT);
		LOG.info(CACHE_PATH_KEY + " " + CACHE_PATH);
		LOG.info(USER_KEY + " " + USER);
		LOG.info(SHADOW_REPLICATION_KEY + " " + SHADOW_REPLICATION);
		LOG.info(DEBUG_KEY + " " + DEBUG);
		LOG.info(STATISTICS_KEY + " " + STATISTICS);
		LOG.info(RPC_TIMEOUT_KEY + " " + RPC_TIMEOUT);
		LOG.info(DATA_TIMEOUT_KEY + " " + DATA_TIMEOUT);
		LOG.info(BUFFER_SIZE_KEY + " " + BUFFER_SIZE);
		LOG.info(SINGLETON_KEY + " " + SINGLETON);
		LOG.info(REGION_SIZE_KEY + " " + REGION_SIZE);
		LOG.info(DIRECTORY_RECORD_KEY + " " + DIRECTORY_RECORD);
		LOG.info(DIRECTORY_RANDOMIZE_KEY + " " + DIRECTORY_RANDOMIZE);		
		LOG.info(CACHE_IMPL_KEY + " " + CACHE_IMPL);
		LOG.info(NAMENODE_ADDRESS_KEY + " " + NAMENODE_ADDRESS);
		LOG.info(NAMENODE_BLOCKSELECTION_KEY + " " + NAMENODE_BLOCKSELECTION);
		LOG.info(NAMENODE_FILEBLOCKS_KEY + " " + NAMENODE_FILEBLOCKS);
		LOG.info(NAMENODE_RPC_TYPE_KEY + " " + NAMENODE_RPC_TYPE);
	}
	
	public static void verify() throws IOException {
		if (CrailConstants.BUFFER_SIZE % CrailConstants.DIRECTORY_RECORD != 0){
			throw new IOException("crail.buffersize must be multiple of " + CrailConstants.DIRECTORY_RECORD);
		}	
	}
}
