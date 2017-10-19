package com.kudu.common;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

public interface KuduOperationsProducer<T> {
	/**
	   * Initializes the operations producer. Called between configure and
	   * getOperations.
	   * @param table the KuduTable used to create Kudu Operation objects
	   */
	  void initialize(KuduTable table);

	  /**
	   * Returns the operations that should be written to Kudu as a result of this Object.
	   * @param 
	   * @return List of Operations that should be written to Kudu
	 * @throws Exception 
	   */
	  Operation getOperations(T object) throws RuntimeException;

	  /**
	   * Cleans up any state. Called when the sink is stopped.
	   */
	  void close();
}
