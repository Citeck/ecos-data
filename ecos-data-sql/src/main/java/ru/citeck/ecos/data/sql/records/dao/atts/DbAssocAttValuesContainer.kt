package ru.citeck.ecos.data.sql.records.dao.atts

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.data.sql.records.dao.DbRecordsDaoCtx
import ru.citeck.ecos.data.sql.records.dao.mutate.operation.AttValuesContainer
import ru.citeck.ecos.data.sql.repo.entity.DbEntity
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.webapp.api.entity.EntityRef
import ru.citeck.ecos.webapp.api.entity.toEntityRef

class DbAssocAttValuesContainer(
    private val entity: DbEntity,
    private val ctx: DbRecordsDaoCtx,
    originalValue: Any?,
    val child: Boolean,
    val attDef: AttributeDef
) : AttValuesContainer<String> {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    val value: LinkedHashSet<String>
    val added = ArrayList<String>()
    val removed = ArrayList<String>()

    init {
        val origVal = LinkedHashSet<String>()
        forEachNotEmptyRef(originalValue, false) { origVal.add(it) }
        value = origVal
    }

    private fun forEachNotEmptyRef(value: Any?, createContentAssocIfRequired: Boolean, action: (String) -> Unit) {
        value ?: return
        var valueToProc: Any = value
        if (valueToProc is DataValue) {
            valueToProc = valueToProc.asJavaObj() ?: return
        }
        if (valueToProc is Collection<*>) {
            for (element in valueToProc) {
                forEachNotEmptyRef(element, createContentAssocIfRequired, action)
            }
        } else if (valueToProc is String) {
            if (valueToProc.isNotBlank()) {
                action.invoke(valueToProc)
            }
        } else if (valueToProc is EntityRef) {
            if (valueToProc.isNotEmpty()) {
                action.invoke(valueToProc.toString())
            }
        } else if (valueToProc is Map<*, *> && createContentAssocIfRequired) {
            val result = ctx.mutAssocHandler.preProcessContentAssocBeforeMutate(
                entity.extId,
                attDef.id,
                DataValue.of(valueToProc),
                entity.workspace
            )
            forEachNotEmptyRef(result, false, action)
        } else {
            log.warn {
                "Invalid value passed to assoc '${attDef.id}'. " +
                    "Type: ${valueToProc::class.simpleName} " +
                    "Value: ${Json.mapper.toString(valueToProc) ?: valueToProc.toString()}"
            }
        }
    }

    fun getAddedTargetsIds(): List<Long> {
        if (added.isEmpty()) {
            return emptyList()
        }
        if (attDef.multiple) {
            return ctx.recordRefService.getOrCreateIdByEntityRefs(added.map { it.toEntityRef() })
        }
        val currentSingleValue = value.firstOrNull()
        return if (currentSingleValue != null && removed.all { currentSingleValue != it }) {
            emptyList()
        } else {
            ctx.recordRefService.getOrCreateIdByEntityRefs(listOf(added.first().toEntityRef()))
        }
    }

    fun getRemovedTargetIds(): List<Long> {
        return ctx.recordRefService.getOrCreateIdByEntityRefs(removed.map { it.toEntityRef() })
    }

    override fun addAll(values: Collection<*>) {
        forEachNotEmptyRef(values, true) {
            added.add(it)
            removed.remove(it)
        }
    }

    override fun removeAll(values: Collection<*>) {
        forEachNotEmptyRef(values, false) {
            removed.add(it)
            added.remove(it)
        }
    }

    fun isEmpty(): Boolean {
        return removed.isEmpty() && added.isEmpty()
    }
}
