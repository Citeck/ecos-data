package ru.citeck.ecos.data.sql.records.dao.atts

import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.AttValuesContainer
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbMultiAssocAttValuesContainer(
    private val ctx: DbRecordsDaoCtx,
    originalValue: Any?,
    val child: Boolean,
    val multiple: Boolean
) : AttValuesContainer<String> {

    val value: LinkedHashSet<String>
    val added = ArrayList<String>()
    val removed = ArrayList<String>()

    init {
        val origVal = LinkedHashSet<String>()
        addNotEmptyRefs(origVal, originalValue)
        value = origVal
    }

    private fun addNotEmptyRefs(collection: MutableCollection<String>, value: Any?) {
        if (value is Collection<*>) {
            for (element in value) {
                addNotEmptyRefs(collection, element)
            }
        } else if (value is String && value.isNotBlank()) {
            collection.add(value)
        } else if (value is EntityRef && value.isNotEmpty()) {
            collection.add(value.toString())
        }
    }

    fun getAddedTargetsIds(): List<Long> {
        if (added.isEmpty()) {
            return emptyList()
        }
        return if (multiple) {
            ctx.recordRefService.getOrCreateIdByEntityRefs(added.map { it.toEntityRef() })
        } else if (value.isNotEmpty()) {
            emptyList()
        } else {
            ctx.recordRefService.getOrCreateIdByEntityRefs(listOf(added.first().toEntityRef()))
        }
    }

    fun getRemovedTargetIds(): List<Long> {
        return ctx.recordRefService.getOrCreateIdByEntityRefs(removed.map { it.toEntityRef() })
    }

    override fun addAll(values: Collection<String>) {
        added.addAll(values)
        removed.removeAll(values)
    }

    override fun removeAll(values: Collection<String>) {
        removed.addAll(values)
        added.removeAll(values)
    }
}
