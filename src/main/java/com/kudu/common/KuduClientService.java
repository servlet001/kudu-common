/**
 * 
 */
package com.kudu.common;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.SessionConfiguration;

/**
 * @author lizhong
 * @email lizhong@163.com
 * 2017年3月24日
 */
public class KuduClientService {
	/**
	 * kudu masters地址
	 */
	private String kuduMasters;
	
	/**
	 * 根据表名获取kudu批量客户端信息
	 * @param tableName 表名
	 * @param timeout 超时时间 / 毫秒
	 * @param bufferSpace 缓冲区大小 / byte
	 * @return KuduClientInfo
	 * @throws KuduException 
	 * 
	 */
	public KuduClientInfo getKuduClientInfo(String tableName, Integer timeout, Integer bufferSpace) throws KuduException{
		KuduClient client = new KuduClient.KuduClientBuilder(this.kuduMasters).build();
		KuduSession session = client.newSession();
		session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
		session.setMutationBufferSpace(bufferSpace);
		session.setTimeoutMillis(timeout);
		session.setIgnoreAllDuplicateRows(true);
		KuduTable table = client.openTable(tableName);
		return new KuduClientInfo(client, session, table);
	}
	
	
	/**
	 * 根据表名获取kudu批量客户端信息
	 * @param tableName
	 * @return KuduClientInfo
	 * @throws KuduException 
	 * 
	 */
	public KuduClientInfo getKuduClientInfo(String tableName) throws KuduException{
		KuduClient client = new KuduClient.KuduClientBuilder(this.kuduMasters).build();
		KuduSession session = client.newSession();
		session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
		session.setIgnoreAllDuplicateRows(true);
		KuduTable table = client.openTable(tableName);
		return new KuduClientInfo(client, session, table);
	}
	
	/**
	 * 根据表名获取kudu单个客户端信息
	 * @param tableName
	 * @return KuduClientInfo
	 * @throws KuduException 
	 * 
	 */
	public KuduClientInfo getSimpleKuduClientInfo(String tableName, Integer timeout) throws KuduException{
		KuduClient client = new KuduClient.KuduClientBuilder(this.kuduMasters).build();
		KuduSession session = client.newSession();
		session.setTimeoutMillis(timeout);
		session.setIgnoreAllDuplicateRows(true);
		KuduTable table = client.openTable(tableName);
		return new KuduClientInfo(client, session, table);
	}


	public String getKuduMasters() {
		return kuduMasters;
	}


	public void setKuduMasters(String kuduMasters) {
		this.kuduMasters = kuduMasters;
	}
	
	

}
