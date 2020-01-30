package org.ndp.port_scan_rr.bean

import com.squareup.moshi.Json

data class MQResult(
    @Json(name = "task-id") val taskID: Int,
    val result: List<Host>,
    val status: Int,
    val desc: String
)