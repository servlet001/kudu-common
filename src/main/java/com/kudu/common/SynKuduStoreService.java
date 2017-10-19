/**
 * 
 */
package com.kudu.common;

import java.util.List;
import java.util.Map;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kudu.common.KuduStoreProducer.ColumnFieldType;


/**
 * @author lizhong
 * @email lizhong@163.com
 * 2017年7月26日
 */
public class SynKuduStoreService implements KuduStoreService {
	
	private static final Logger logger = LoggerFactory.getLogger(SynKuduStoreService.class);

	private KuduClient client;

	private KuduSession session;

	private KuduTable table;

	private KuduOperationsProducer<JSONObject> operationsProducer;
	
	
	/**
	 * json中字段与kudu表列字段映射关系
	 */
	private Map<String, ColumnFieldType> columnMapping;


	/**
	 * 忽略插入kudu异常
	 */
	private Boolean isIgnoreException = true;

	/**
	 * @param columnMapping
	 * @param client
	 * @param session
	 * @param table
	 */
	public SynKuduStoreService(Map<String, ColumnFieldType> columnMapping, KuduClient client, KuduSession session, KuduTable table) {
		this.columnMapping = columnMapping;
		this.client = client;
		this.session = session;
		this.table = table;
	}
	
	
	/**
	 * 
	 * @param columnMapping
	 * @param client
	 * @param session
	 * @param table
	 * @param batchSize 批量大小
	 */
	public SynKuduStoreService(Map<String, ColumnFieldType> columnMapping, KuduClient client, KuduSession session, KuduTable table, int batchSize) {
		this.columnMapping = columnMapping;
		this.client = client;
		this.session = session;
		this.table = table;
	}

	/* (non-Javadoc)
	 * @see com.fcbox.tibet.common.kudu.KuduStoreService2#init(com.fcbox.tibet.common.kudu.OperationType)
	 */
	@Override
	public void init(OperationType type) {
		operationsProducer = new KuduStoreProducer(columnMapping, type);
		operationsProducer.initialize(table);
	}

	/* (non-Javadoc)
	 * @see com.fcbox.tibet.common.kudu.KuduStoreService2#execute(java.util.List)
	 */
	@Override
	public void execute(Object object) {
		List<JSONObject> datas = (List<JSONObject>)object;
		if (datas.size() > 0) {
			for (JSONObject data : datas) {
				if(logger.isDebugEnabled()){
					logger.debug("要保存的数据：{}", JSON.toJSONString(data));
				}
				try {
					Operation op = operationsProducer.getOperations(data);
					session.apply(op);
				} catch (Exception e) {
					logger.error("出现异常的数据: {}", JSON.toJSONString(data));
					if(isIgnoreException){
						logger.info("忽略kudu存储失败，异常信息：{}", e);
					}else{
						logger.error("存储kudu失败，异常信息：" + e);
						throw new RuntimeException(e);
					}
				}
			}
			// 批量提交
			try {
				List<OperationResponse> responses = session.flush();
				if (responses != null) {
					for (OperationResponse response : responses) {
						if (response.hasRowError()) {
							if(isIgnoreException){
								logger.info("忽略kudu存储失败，异常信息：{}", response.getRowError().toString());
							}else{
								throw new RuntimeException("Failed to flush one or more changes. "
										+ "store kudu failed: " + response.getRowError().toString());
							}
						}
					}
				}
				logger.info("存储kudu数据成功.....,保存{}条数据", datas.size());
			} catch (KuduException e) {
				logger.error("不能处理  " + datas.size() + " tuples", e);
			}
		}
	}


	@Override
	public void stop() {
		Exception ex = null;
		try {
			operationsProducer.close();
		} catch (Exception e) {
			ex = e;
			logger.error("Error closing operations producer", e);
		}
		try {
			if (client != null) {
				client.shutdown();
			}
			client = null;
			table = null;
			session = null;
		} catch (KuduException e) {
			ex = e;
			logger.error("Error closing client", e);
		}
		if (ex != null) {
			throw new RuntimeException("Error stopping kuduStore", ex);
		}
	}

	public Boolean getIsIgnoreException() {
		return isIgnoreException;
	}

	public void setIsIgnoreException(Boolean isIgnoreException) {
		this.isIgnoreException = isIgnoreException;
	}
}
