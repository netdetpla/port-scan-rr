package org.ndp.port_scan_rr.bean

class Port(
        val protocol: String,
        val portID: String,
        val state: String,
        val service: String,
        val product: String
)