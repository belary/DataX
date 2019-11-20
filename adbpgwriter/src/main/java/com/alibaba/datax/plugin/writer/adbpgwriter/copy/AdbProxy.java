package com.alibaba.datax.plugin.writer.adbpgwriter.copy;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.sql.Connection;
/**
 * @author yuncheng
 */
public interface AdbProxy {
    void startWriteWithConnection(RecordReceiver recordReceiver, Connection connection);

    void closeResource();
}
