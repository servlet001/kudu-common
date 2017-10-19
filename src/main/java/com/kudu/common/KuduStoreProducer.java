/**
 * 
 */
package com.kudu.common;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import com.alibaba.fastjson.JSONObject;

/**
 * @author lizhong
 * @email lizhong@163.com 2017年7月26日
 */
public class KuduStoreProducer implements KuduOperationsProducer<JSONObject> {

	private KuduTable table;
	private OperationType operation;
	private Map<String, ColumnFieldType> columnMapping;

	public static class ColumnFieldType {
		private String field;
		private Type type;
		
		public ColumnFieldType(String field, Type type){
			this.field = field;
			this.type = type;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}

		public Type getType() {
			return type;
		}

		public void setType(Type type) {
			this.type = type;
		}
	}

	@Override
	public void initialize(KuduTable table) {
		this.table = table;
	}

	/**
	 * 
	 * @param columnType
	 * @param columnMapping
	 * @param operation
	 */
	public KuduStoreProducer(Map<String, ColumnFieldType> columnMapping, OperationType operation) {
		this.columnMapping = columnMapping;
		this.operation = operation;
	}

	@Override
	public Operation getOperations(JSONObject object) throws RuntimeException {
		Operation op = null;
		switch (operation.name()) {
		case "INSERT":
			op = table.newInsert();
			break;
		case "UPSERT":
			op = table.newUpsert();
			break;
		case "UPDATE":
			op = table.newUpdate();
			break;
		default:
			throw new RuntimeException(String.format("Unexpected operation %s", operation.name()));
		}
		PartialRow row = op.getRow();
		for (String field : object.keySet()) {
			handler(row, object, field);
		}
		return op;
	}

	private void handler(PartialRow row, JSONObject json, String field) {
		ColumnFieldType column = columnMapping.get(field);
		if (column != null) {
			Type type = column.getType();
			String columnName = column.getField();
			if (type == Type.STRING && StringUtils.isNotBlank(json.getString(field))) {
				row.addString(columnName, json.getString(field));
			} else if (type == Type.BOOL && json.getBoolean(field) != null) {
				row.addBoolean(columnName, json.getBooleanValue(field));
			} else if (type == Type.DOUBLE && json.getDouble(field) != null) {
				row.addDouble(columnName, json.getDouble(field));
			} else if (type == Type.FLOAT && json.getFloat(field) != null) {
				row.addFloat(columnName, json.getFloat(field));
			} else if (type == Type.INT32 && json.getInteger(field) != null) {
				row.addInt(columnName, json.getInteger(field));
			} else if (type == Type.INT32X100 && json.getBigDecimal(field) != null) {
				BigDecimal value = json.getBigDecimal(field);
				value = value.multiply(new BigDecimal(100));
				row.addInt(columnName, value.intValue());
			} else if (type == Type.INT64 && json.getLong(field) != null) {
				row.addLong(columnName, json.getLong(field));
			} else if (type == Type.INT64X100 && json.getBigDecimal(field) != null) {
				BigDecimal value = json.getBigDecimal(field);
				value = value.multiply(new BigDecimal(100));
				row.addLong(columnName, value.longValue());
			} else if (type == Type.BINARY && json.getByte(field) != null) {
				row.addBinary(columnName, json.getBytes(field));
			} else if (type == Type.INT16 && json.getShort(field) != null) {
				row.addShort(columnName, json.getShort(field));
			} else if (type == Type.INT8 && json.getByte(field) != null) {
				row.addByte(columnName, json.getByteValue(field));
			}
		}
	}

	@Override
	public void close() {

	}

}
