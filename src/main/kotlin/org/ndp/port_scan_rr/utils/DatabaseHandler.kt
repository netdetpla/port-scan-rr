package org.ndp.port_scan_rr.utils

import me.liuwj.ktorm.database.Database
import me.liuwj.ktorm.dsl.*
import org.ndp.port_scan_rr.bean.BatchInsertPort
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

    fun batchUpdateTaskStatus(updateTasks: List<Task>) {
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
    }

    fun batchUpdateIPFlag(updateIPs: List<Long>) {
        IP.batchUpdate {
            for (ip in updateIPs) {
                item {
                    it.portScanFlag to 1
                }
            }
        }
    }

    fun batchInsertPort(insertPorts: List<BatchInsertPort>) {
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