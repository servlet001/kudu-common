package com.kudu.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.client.KuduException;

import com.alibaba.fastjson.JSONObject;
import com.kudu.common.KuduStoreProducer.ColumnFieldType;

public class KuduTest {
	
	public void testSave() throws KuduException{
		KuduClientService kuduClientService = new KuduClientService();
		kuduClientService.setKuduMasters("10.118.165.215:7051,10.118.165.216:7051,10.118.165.214:7051");
		KuduClientInfo client = kuduClientService.getKuduClientInfo("test");
		
		//数据映射关系
		Map<String, ColumnFieldType> columnMapping = new HashMap<>();
		columnMapping.put("name", new ColumnFieldType("name", Type.STRING));
		columnMapping.put("address", new ColumnFieldType("address", Type.STRING));
		columnMapping.put("email", new ColumnFieldType("email", Type.STRING));
		columnMapping.put("amount", new ColumnFieldType("amount", Type.INT64X100));
		columnMapping.put("age", new ColumnFieldType("age", Type.INT8));
		columnMapping.put("orderStatus", new ColumnFieldType("order_status", Type.INT8));
		
		
		KuduStoreService kuduStoreService = new SynKuduStoreService(columnMapping, client.getClient(), client.getSession(), client.getTable());
		kuduStoreService.init(OperationType.INSERT);
		
		List<JSONObject> datas = new ArrayList<>();
		kuduStoreService.execute(datas);
		
	}
	
}
