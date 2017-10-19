/**
 * 
 */
package com.kudu.common;

import java.io.Serializable;

import org.apache.kudu.Type;

/**
 * @author lizhong
 * 列名称类型对象
 *
 */
public class KuduColumnSchema implements Serializable{
	
	private static final long serialVersionUID = -6718658353834283716L;

	/**
	 * 列名
	 */
	private String name;
	
	/**
	 * 列类型
	 */
	private Type type;
	
	public KuduColumnSchema(){
		
	}

	public KuduColumnSchema(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
	
	
}
