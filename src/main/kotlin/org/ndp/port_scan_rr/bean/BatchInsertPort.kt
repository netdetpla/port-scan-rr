package org.ndp.port_scan_rr.bean

data class BatchInsertPort(
    val ipID: Long,
    val portID: Int,
    val protocol: String,
    val service: String,
    val product: String
)