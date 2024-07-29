package ru.citeck.ecos.data.sql.content.storage

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder
import ru.citeck.ecos.commons.data.ObjectData
import ru.citeck.ecos.webapp.api.entity.EntityRef

@JsonDeserialize(builder = EcosContentStorageConfig.Builder::class)
data class EcosContentStorageConfig(
    val ref: EntityRef,
    val config: ObjectData
) {

    companion object {

        @JvmStatic
        fun create(): Builder {
            return Builder()
        }
    }

    @JsonPOJOBuilder
    class Builder {

        var ref: EntityRef = EntityRef.EMPTY
        var config: ObjectData = ObjectData.create()

        fun withRef(ref: EntityRef?): Builder {
            this.ref = ref ?: EntityRef.EMPTY
            return this
        }

        fun withConfig(config: ObjectData?): Builder {
            this.config = config ?: ObjectData.create()
            return this
        }

        fun build(): EcosContentStorageConfig {
            return EcosContentStorageConfig(ref, config)
        }
    }
}
