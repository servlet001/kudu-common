/**
 * 
 */
package com.kudu.common;

/**
 * kudu列的数据类型
 * @author lizhong
 * @email lizhong@163.com 2017年3月28日
 */
public enum Type {
	INT8("int8"), 
	INT16("int16"), 
	INT32("int32"),
	/**这种类型的value = 数值  * 100*/
	INT32X100("int32x100"),
	INT64("int64"),
	/**这种类型的value = 数值  * 100*/
	INT64X100("int64x100"),
	BINARY("binary"), 
	STRING("string"), 
	BOOL("bool"), 
	FLOAT("float"), 
	DOUBLE("double"), 
	UNIXTIME_MICROS("unixtime_micros");

	private final String name;

	/**
	 * Private constructor used to pre-create the types
	 * 
	 * @param dataType
	 *            DataType from the common's pb
	 * @param name
	 *            string representation of the type
	 */
	private Type(String name) {
		this.name = name;
	}
}
