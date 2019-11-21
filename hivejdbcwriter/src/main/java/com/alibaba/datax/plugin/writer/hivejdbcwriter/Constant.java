package com.alibaba.datax.plugin.writer.hivejdbcwriter;

public class Constant {


	public static final int DEFAULT_BATCH_SIZE = 2048;

	public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;

	public static String TABLE_NAME_PLACEHOLDER = "@table";

	public static String CONN_MARK = "connection";

	public static String TABLE_NUMBER_MARK = "tableNumber";

	public static String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";
}
