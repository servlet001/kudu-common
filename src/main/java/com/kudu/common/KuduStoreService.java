package com.kudu.common;

public interface KuduStoreService {

	public void init(OperationType type);

	public void execute(Object datas);

	public void stop();

}