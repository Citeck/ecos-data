package ru.citeck.ecos.data.sql.meta.table.dto

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import ru.citeck.ecos.commons.data.ObjectData
import java.time.Instant

@JsonDeserialize(builder = DbTableChangeSet.Builder::class)
class DbTableChangeSet(
    val startTime: Instant,
    val durationMs: Long,
    val type: String,
    val params: ObjectData,
    val commands: List<String>
) {

    companion object {

        @JvmField
        val EMPTY = create {}

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }

        @JvmStatic
        fun create(builder: Builder.() -> Unit): DbTableChangeSet {
            val builderObj = Builder()
            builder.invoke(builderObj)
            return builderObj.build()
        }
    }

    class Builder() {

        var startTime: Instant = Instant.EPOCH
        var durationMs: Long = 0
        var type: String = ""
        var params: ObjectData = ObjectData.create()
        var commands: List<String> = emptyList()

        constructor(base: DbTableChangeSet) : this() {
            this.startTime = base.startTime
            this.durationMs = base.durationMs
            this.type = base.type
            this.params = base.params.deepCopy()
            this.commands = base.commands
        }

        fun withStartTime(startTime: Instant?): Builder {
            this.startTime = startTime ?: Instant.EPOCH
            return this
        }

        fun withDurationMs(durationMs: Long?): Builder {
            this.durationMs = durationMs ?: 0
            return this
        }

        fun withType(type: String?): Builder {
            this.type = type ?: ""
            return this
        }

        fun withParams(config: ObjectData?): Builder {
            this.params = config ?: ObjectData.create()
            return this
        }

        fun withCommands(commands: List<String>?): Builder {
            this.commands = commands ?: emptyList()
            return this
        }

        fun build(): DbTableChangeSet {
            return DbTableChangeSet(
                startTime,
                durationMs,
                type,
                params,
                commands
            )
        }
    }
}
