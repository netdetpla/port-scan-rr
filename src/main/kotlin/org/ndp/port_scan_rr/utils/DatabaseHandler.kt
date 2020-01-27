package org.ndp.port_scan_rr.utils

import me.liuwj.ktorm.database.Database
import me.liuwj.ktorm.dsl.*
import org.ndp.port_scan_rr.bean.BatchInsertPort
import org.ndp.port_scan_rr.bean.KafkaResult
import org.ndp.port_scan_rr.bean.Port
import org.ndp.port_scan_rr.bean.Task
import org.ndp.port_scan_rr.table.IP
import org.ndp.port_scan_rr.table.Task as TableTask
import org.ndp.port_scan_rr.table.Port as TablePort

object DatabaseHandler {
    private val dbUrl = Settings.setting["dbUrl"] as String
    private val dbDriver = Settings.setting["dbDriver"] as String
    private val dbUser = Settings.setting["dbUser"] as String
    private val dbPassword = Settings.setting["dbPassword"] as String

    init {
        Database.Companion.connect(
            dbUrl,
            dbDriver,
            dbUser,
            dbPassword
        )
    }

    fun parsePortScanResult(results: List<KafkaResult>) {
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
        // database handle
        TableTask.batchUpdate {
            for (task in updateTasks) {
                item {
                    it.taskStatus to task.status
                    it.desc to task.desc
                    where {
                        TableTask.id eq task.id
                    }
                }
            }
        }
        IP.batchUpdate {
            for (ip in updateIPs) {
                item {
                    it.portScanFlag to 1
                }
            }
        }
        TablePort.batchInsert {
            for (port in insertPorts) {
                item {
                    it.ipID to port.ipID
                    it.port to port.portID
                    it.protocol to port.protocol
                    it.service to port.service
                    it.product to port.product
                }
            }
        }
    }
}