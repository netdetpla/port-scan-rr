package org.ndp.port_scan_rr.table

import me.liuwj.ktorm.schema.Table
import me.liuwj.ktorm.schema.int
import me.liuwj.ktorm.schema.long
import me.liuwj.ktorm.schema.varchar

object Port : Table<Nothing>("port") {
    val id by int("id").primaryKey()
    val ipID by long("ip_id")
    val port by int("port")
    val protocol by varchar("protocol")
    val service by varchar("service")
    val product by varchar("product")
    val version by varchar("version")
}
