/**
 * 
 */
package com.kudu.common;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

/**
 * kudu客户端信息
 * @author lizhong
 * @email lizhong0815@163.com
 * 2017年3月24日
 */
public class KuduClientInfo {
	
	private KuduClient client;

	private KuduSession session;

	private KuduTable table;
	
	public KuduClientInfo(){
		
	}
	
	public KuduClientInfo(KuduClient client, KuduSession session, KuduTable table) {
		this.client = client;
		this.session = session;
		this.table = table;
	}

	public KuduClient getClient() {
		return client;
	}

	public void setClient(KuduClient client) {
		this.client = client;
	}

	public KuduSession getSession() {
		return session;
	}

	public void setSession(KuduSession session) {
		this.session = session;
	}

	public KuduTable getTable() {
		return table;
	}

	public void setTable(KuduTable table) {
		this.table = table;
	}
	
	public void close(){
		if (client != null) {
			try {
				client.shutdown();
			} catch (KuduException e) {
				e.printStackTrace();
			}
		}
		client = null;
		table = null;
		session = null;
	}
}
