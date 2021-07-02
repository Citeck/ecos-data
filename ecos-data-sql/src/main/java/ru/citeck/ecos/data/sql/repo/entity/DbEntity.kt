package ru.citeck.ecos.data.sql.repo.entity

import ru.citeck.ecos.commons.data.MLText
import java.time.Instant

class DbEntity {

    companion object {
        const val NEW_REC_ID = -1L

        const val ID = "id"
        const val EXT_ID = "__ext_id"
        const val UPD_VERSION = "__upd_version"
        const val MODIFIED = "__modified"
        const val MODIFIER = "__modifier"
        const val CREATED = "__created"
        const val CREATOR = "__creator"
        const val DELETED = "__deleted"
        const val TYPE = "__type"
        const val STATUS = "__status"
        const val NAME = "__name"
        const val DOC_NUM = "__doc_num"
        const val TENANT = "__tenant"
    }

    @Constraints("PRIMARY KEY")
    var id: Long = NEW_REC_ID

    @Constraints("NOT NULL")
    var extId: String = ""

    @Constraints("NOT NULL")
    var updVersion: Long = 0

    @Constraints("NOT NULL")
    var modified: Instant = Instant.EPOCH
    @Constraints("NOT NULL")
    var modifier: String = ""

    @Constraints("NOT NULL")
    var created: Instant = Instant.EPOCH
    @Constraints("NOT NULL")
    var creator: String = ""

    @Constraints("NOT NULL")
    var deleted: Boolean = false

    @Constraints("NOT NULL")
    var type: String = ""
    @Constraints("NOT NULL")
    var status: String = ""
    @Constraints("NOT NULL")
    var name: MLText = MLText.EMPTY

    @Constraints("NOT NULL")
    var tenant: String = ""

    var docNum: Long? = null

    var attributes: MutableMap<String, Any?> = LinkedHashMap()
}
