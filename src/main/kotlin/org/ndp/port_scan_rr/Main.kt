package org.ndp.port_scan_rr

import org.ndp.port_scan_rr.bean.BatchInsertPort
import org.ndp.port_scan_rr.bean.Task
import org.ndp.port_scan_rr.utils.DatabaseHandler
import org.ndp.port_scan_rr.utils.IPConverter
import org.ndp.port_scan_rr.utils.Logger.logger
import org.ndp.port_scan_rr.utils.RedisHandler

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("start result recycling...")
        val results = RedisHandler.consumeResult(RedisHandler.generateNonce(5))
        val updateTasks = ArrayList<Task>()
        val updateIPs = ArrayList<Long>()
        val insertPorts = ArrayList<BatchInsertPort>()
        for (result in results) {
            // task status update
            if (result.status == 1) {
                updateTasks.add(Task(result.taskID, 21000, result.desc))
                continue
            }
            updateTasks.add(Task(result.taskID, 20030, ""))
            // host update and port insert
            for (host in result.result) {
                val intIP = IPConverter.iNetString2Number(host.address)
                updateIPs.add(intIP)
                for (port in host.ports) {
                    insertPorts.add(
                        BatchInsertPort(
                            intIP,
                            Integer.parseInt(port.portID),
                            port.protocol,
                            port.service,
                            port.product
                        )
                    )
                }
            }
        }
        DatabaseHandler.batchUpdateTaskStatus(updateTasks)
        DatabaseHandler.batchUpdateIPFlag(updateIPs)
        DatabaseHandler.batchInsertPort(insertPorts)
        RedisHandler.returnACK()
    }
}