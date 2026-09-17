package ru.citeck.ecos.data.sql.ecostype

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.commons.exception.I18nRuntimeException
import ru.citeck.ecos.data.sql.columnmeta.DbAttTypeColumns
import ru.citeck.ecos.data.sql.columnmeta.DbColumnSemanticType
import ru.citeck.ecos.data.sql.columnmeta.DbExpectedAttTypes
import ru.citeck.ecos.data.sql.dto.DbColumnDef
import ru.citeck.ecos.data.sql.dto.DbColumnIndexDef
import ru.citeck.ecos.data.sql.records.utils.DbReadToleranceLog
import ru.citeck.ecos.model.lib.ModelServiceFactory
import ru.citeck.ecos.model.lib.aspect.dto.AspectInfo
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttStoringType
import ru.citeck.ecos.model.lib.attributes.dto.computed.ComputedAttType
import ru.citeck.ecos.model.lib.type.dto.TypeInfo
import ru.citeck.ecos.model.lib.utils.ModelUtils
import ru.citeck.ecos.webapp.api.entity.EntityRef

class DbEcosModelService(
    modelServices: ModelServiceFactory
) {

    companion object {
        const val TYPE_ID_TEMP_FILE = "temp-file"

        private val log = KotlinLogging.logger {}

        private val VALID_COLUMN_NAME = "[\\w-_:]+".toRegex()

        /**
         * Cap on how many types [getTableAttTypes] will examine while descending from the storage
         * root. A blank resolved `sourceId` (see [getTableAttTypes]'s KDoc) resolves that root to
         * `base` itself, and [getAllChildrenIds] has no cycle guard or depth cap of its own - so an
         * uncapped descend from `base` would walk every type in the installation, with a
         * `getTypeInfo` + `mapAttsToColumns` + `getColumnsForAspects` for each, on every mutation of
         * every DEFAULT-storage type with no explicit `sourceId` - on the mutation path, holding the
         * distributed lock, inside an open transaction. Generous enough that a real installation's
         * type tree under a real storage root never comes close to it.
         */
        private const val MAX_DESCEND_TYPES = 2000
    }

    private val typesRepo = modelServices.typesRepo
    private val aspectsRepo = modelServices.aspectsRepo

    fun isSubType(typeId: String?, ofTypeId: String?): Boolean {
        if (typeId == ofTypeId) {
            return true
        }
        if (typeId.isNullOrBlank() || ofTypeId.isNullOrBlank()) {
            return false
        }
        if (ofTypeId == "base") {
            return true
        }
        var typeInfo = typesRepo.getTypeInfo(ModelUtils.getTypeRef(typeId))
        var iterations = 30
        while (typeInfo != null && typeInfo.id != "base" && --iterations > 0) {
            if (typeInfo.parentRef.getLocalId() == ofTypeId) {
                return true
            }
            typeInfo = typesRepo.getTypeInfo(typeInfo.parentRef)
        }
        return false
    }

    fun getAspectsForAtts(attributes: Set<String>): List<EntityRef> {
        return aspectsRepo.getAspectsForAtts(attributes)
    }

    fun getAllChildrenIds(typeId: String, result: MutableCollection<String>) {
        val children = typesRepo.getChildren(ModelUtils.getTypeRef(typeId))
        for (child in children) {
            result.add(child.getLocalId())
            getAllChildrenIds(child.getLocalId(), result)
        }
    }

    /**
     * Same traversal as [getAllChildrenIds], but stops once [result] reaches [maxSize] instead of
     * walking the whole subtree - see [MAX_DESCEND_TYPES] for why [getTableAttTypes] needs that.
     *
     * Returns true when the cap was hit. That is the same safe direction as the upward walk's own
     * cap in [getStorageBoundaryTypeIds]: fewer children examined means fewer conflicts found,
     * never more, so a capped result can only under-detect, never over-detect - the caller is
     * responsible for turning that into a `warnOnce` rather than treating it as a complete answer.
     */
    private fun getAllChildrenIdsCapped(typeId: String, result: MutableCollection<String>, maxSize: Int): Boolean {
        if (result.size >= maxSize) {
            return true
        }
        val children = typesRepo.getChildren(ModelUtils.getTypeRef(typeId))
        for (child in children) {
            if (result.size >= maxSize) {
                return true
            }
            result.add(child.getLocalId())
            if (getAllChildrenIdsCapped(child.getLocalId(), result, maxSize)) {
                return true
            }
        }
        return false
    }

    fun getAttDefFromChildrenTypes(typeId: String, attId: String): AttributeDef? {
        if (typeId.isBlank()) {
            return null
        }
        val children = typesRepo.getChildren(ModelUtils.getTypeRef(typeId))
        for (childRef in children) {
            val info = typesRepo.getTypeInfo(childRef) ?: continue
            val attDef = info.model.attributes.find { it.id == attId }
            if (attDef != null) {
                return attDef
            }
        }
        for (childRef in children) {
            val attDef = getAttDefFromChildrenTypes(childRef.getLocalId(), attId)
            if (attDef != null) {
                return attDef
            }
        }
        return null
    }

    fun getTypeInfoNotNull(typeId: String): TypeInfo {
        return getTypeInfo(typeId) ?: throw I18nRuntimeException(
            messageKey = "ecos-data.type-not-found-by-id",
            messageArgs = mapOf("typeId" to typeId)
        )
    }

    fun getTypeInfo(typeId: String): TypeInfo? {
        if (typeId.isBlank()) {
            return null
        }
        return typesRepo.getTypeInfo(ModelUtils.getTypeRef(typeId))
    }

    fun getAspectsInfo(aspectRefs: Collection<EntityRef>): List<AspectInfo> {
        return aspectRefs.map { aspectsRepo.getAspectInfo(it) ?: AspectInfo.EMPTY }
    }

    fun getAllAttributesForAspects(aspectRefs: Collection<EntityRef>): List<AttributeDef> {
        return getAttributesForAspects(aspectRefs, true)
    }

    fun getAttributesForAspects(aspectRefs: Collection<EntityRef>, includeSystem: Boolean): List<AttributeDef> {
        val attributes = ArrayList<AttributeDef>(32)
        for (aspectRef in aspectRefs) {
            val aspectInfo = aspectsRepo.getAspectInfo(aspectRef)
            if (aspectInfo != null) {
                attributes.addAll(aspectInfo.attributes)
                if (includeSystem) {
                    attributes.addAll(aspectInfo.systemAttributes)
                }
            }
        }
        return attributes
    }

    fun getColumnsForAspects(aspectRefs: Collection<EntityRef>): List<EcosAttColumnDef> {
        val columns = ArrayList<EcosAttColumnDef>(32)
        for (aspectRef in aspectRefs) {
            val aspectInfo = aspectsRepo.getAspectInfo(aspectRef)
            if (aspectInfo != null) {
                columns.addAll(mapAttsToColumns(aspectInfo.attributes, false))
                columns.addAll(mapAttsToColumns(aspectInfo.systemAttributes, true))
            }
        }
        return columns
    }

    fun getColumnsForTypes(typesInfo: List<TypeInfo>): List<EcosAttColumnDef> {

        val processedTypes = hashSetOf<String>()
        val columnsById = LinkedHashMap<String, EcosAttColumnDef>()

        typesInfo.forEach { typeInfo ->

            if (processedTypes.add(typeInfo.id)) {

                val model = typeInfo.model
                val columns = ArrayList<EcosAttColumnDef>(
                    model.attributes.size + model.systemAttributes.size
                )
                columns.addAll(mapAttsToColumns(model.attributes, false))
                columns.addAll(mapAttsToColumns(model.systemAttributes, true))

                columns.forEach {
                    val currentColumn = columnsById[it.column.name]
                    if (currentColumn != null) {
                        if (it.column.type != currentColumn.column.type) {
                            error(
                                "Columns type doesn't match. " +
                                    "Current column: $currentColumn New column: $it"
                            )
                        }
                        if (it.systemAtt != currentColumn.systemAtt) {
                            error(
                                "System attribute flag doesn't match. " +
                                    "Current column: $currentColumn New column: $it"
                            )
                        }
                    }
                    columnsById[it.column.name] = it
                }
            }
        }

        return columnsById.values.toList()
    }

    /**
     * The chain of type ids from [typeInfo] up to the top of the storage it belongs to: [typeInfo]
     * itself first, then each ancestor that still resolves to the same `sourceId`, the type that
     * owns the table last. An ancestor with a different `sourceId` owns a different table, so it
     * ends the chain instead of joining it.
     *
     * The two callers read the same chain in opposite directions. [getTableAttTypes] wants only its
     * last element - the owner of the table - so that it can ask who else declares that table's
     * columns. A model change wants the whole chain, because the change names a type while what has
     * to be repaired is a table, and a records DAO can be registered by hand on any level of the
     * chain, not only on its top. They share one implementation deliberately: if the trigger's idea
     * of "which table stores this type" ever drifted from the one the frozen-column check uses, a
     * change would be reconciled against a table it does not belong to, or not at all.
     *
     * A blank `sourceId` is compared equal to a blank one here - see [getTableAttTypes]'s KDoc for
     * why that asymmetry is chosen on purpose, and what it costs.
     */
    fun getStorageBoundaryTypeIds(typeInfo: TypeInfo): List<String> {
        val ids = ArrayList<String>(4)
        walkUpToStorageBoundary(typeInfo, ids, StorageBoundaryWalk.FOR_TABLE_LOOKUP)
        return ids
    }

    /**
     * Which of the two callers of the climb is asking - and therefore under which key, and with
     * which consequence, an incomplete climb is reported.
     *
     * The key has to differ per caller because
     * [DbReadToleranceLog.warnOnce] deduplicates for the whole life of the JVM: one shared key and
     * the first path to hit the cap silences the other permanently, for the same type, even though
     * what each of them loses is not the same thing. The price of splitting it is two key slots of
     * the warn budget instead of one per capped type - in a branch a real type tree never reaches.
     */
    private enum class StorageBoundaryWalk(val warnKeyPrefix: String, val consequence: String) {

        /**
         * [getTableAttTypes]: fewer types examined means fewer conflicts found.
         */
        FOR_CONFLICTS(
            "table-att-types-walk-capped",
            "Conflicts for its table may be under-detected as a result."
        ),

        /**
         * [getStorageBoundaryTypeIds]: the table that stores the type may not be found at all.
         */
        FOR_TABLE_LOOKUP(
            "storage-boundary-walk-capped",
            "A model change for this type may not reach the table that stores its records, so " +
                "that table will only be repaired by the next mutation of one of its records."
        )
    }

    /**
     * The single implementation of the climb: returns the type that owns the table and, when [ids]
     * is given, appends every id passed on the way, both ends included.
     *
     * It returns the [TypeInfo] rather than letting the caller re-resolve the last id, so that
     * [getTableAttTypes] keeps exactly the object it used before this walk was extracted - no second
     * `getTypeInfo` on the mutation path, and no new way for the root to come back null when the
     * types repo answers differently the second time.
     *
     * There is no visited-set: a parent chain that loops is stopped by the step cap, the same one
     * that stops a chain which is merely too long.
     */
    private fun walkUpToStorageBoundary(
        typeInfo: TypeInfo,
        ids: MutableList<String>?,
        walk: StorageBoundaryWalk
    ): TypeInfo {
        var rootType = typeInfo
        ids?.add(rootType.id)
        var iterations = 30
        var walkCapped = false
        while (true) {
            if (iterations <= 0) {
                // Stopped only because the cap was hit, not because the real top of the storage
                // boundary was found - unlike the `break`s below, which are genuine stops. Silently
                // treating this as the top would under-populate `conflicts`, and under-detection is
                // the unsafe direction here (see the KDoc), so this is not a case to pass through
                // quietly even though it is practically unreachable.
                walkCapped = true
                break
            }
            iterations--
            val parentId = rootType.parentRef.getLocalId()
            if (parentId.isBlank()) {
                break
            }
            val parent = getTypeInfo(parentId) ?: break
            if (parent.sourceId != typeInfo.sourceId) {
                break
            }
            rootType = parent
            ids?.add(rootType.id)
        }
        if (walkCapped) {
            DbReadToleranceLog.warnOnce(log, "${walk.warnKeyPrefix}:${typeInfo.id}") {
                "Storage-boundary walk for type '${typeInfo.id}' did not reach the top of its " +
                    "parent chain within 30 steps. ${walk.consequence}"
            }
        }
        return rootType
    }

    /**
     * What every type whose records can land in this type's table expects of that table's columns.
     *
     * [typeInfo] may itself be a descendant with no storage of its own, because the mutation that
     * needs this answer is driven by the type of the record being mutated. So the table's boundary
     * is found first, by walking up while the parent's resolved `sourceId` still matches, and only
     * then walked back down: that root plus the descendants inheriting its storage. Aspect
     * attributes count too - they become columns of the same table.
     *
     * A DEFAULT-storage type with no explicit `sourceId` inherits its parent's, which under the bare
     * abstract roots is itself blank - so several unrelated types can have a blank resolved
     * `sourceId`. Blank is still compared equal to blank, deliberately, because the two ways of
     * being wrong are not symmetric: under-detecting lets two types that really share a table
     * corrupt each other's data, while over-detecting only freezes a column that did not need it,
     * which the model can always fix. See [DbExpectedAttTypes.groupingMayBeOverBroad] for how that
     * reaches an operator.
     *
     * Where two of those types declare one attribute id with different types there is no expected
     * type at all, so the column is reported in [DbExpectedAttTypes.conflicts] and the first
     * declaration is kept in [DbExpectedAttTypes.attTypes] only so the registry can describe it.
     */
    /**
     * Every type whose records can land in [typeInfo]'s table: the type that owns the table first,
     * then the descendants that inherit its storage.
     *
     * The same set [getTableAttTypes] folds into conflicts, exposed because the schema
     * reconciliation of a table needs the types themselves and not only their disagreements: a
     * descendant with `DEFAULT` storage declares columns of this table too, and a reconciliation
     * that looked only at the type owning the table would leave exactly those columns to the lazy
     * mutation path - the path this whole feature exists to stop depending on.
     *
     * Kept as one implementation with [getTableAttTypes] for the reason the upward walk is
     * ([getStorageBoundaryTypeIds]): two answers to "which types share this table" that could drift
     * apart would make a column frozen for one caller and migrated by the other.
     *
     * The grouping is over-broad for a blank resolved `sourceId` - see [getTableAttTypes]'s KDoc -
     * so a caller that *writes* to the table because of what it finds here has to narrow the result
     * itself; [ru.citeck.ecos.data.sql.modelchange.DbSchemaReconciler] narrows it against the
     * column registry.
     */
    fun getStorageTypes(typeInfo: TypeInfo): List<TypeInfo> {
        val rootType = walkUpToStorageBoundary(typeInfo, null, StorageBoundaryWalk.FOR_CONFLICTS)
        return collectStorageTypes(rootType, typeInfo.id)
    }

    /**
     * [rootType] plus every descendant of it that resolves to the same `sourceId`, in breadth order.
     * [walkedFromTypeId] names the type the caller started from and is used only in the message of
     * the descend cap.
     */
    private fun collectStorageTypes(rootType: TypeInfo, walkedFromTypeId: String): List<TypeInfo> {
        val types = ArrayList<TypeInfo>()
        types.add(rootType)
        val childrenIds = LinkedHashSet<String>()
        val descendCapped = getAllChildrenIdsCapped(rootType.id, childrenIds, MAX_DESCEND_TYPES)
        if (descendCapped) {
            DbReadToleranceLog.warnOnce(log, "table-att-types-descend-capped:$walkedFromTypeId") {
                "Storage-boundary descend for type '$walkedFromTypeId' (root '${rootType.id}') " +
                    "examined $MAX_DESCEND_TYPES types without reaching the end of the tree. " +
                    "Conflicts for its table may be under-detected as a result."
            }
        }
        for (childId in childrenIds) {
            val child = getTypeInfo(childId) ?: continue
            if (child.sourceId == rootType.sourceId) {
                types.add(child)
            }
        }
        return types
    }

    fun getTableAttTypes(typeInfo: TypeInfo): DbExpectedAttTypes {

        val rootType = walkUpToStorageBoundary(typeInfo, null, StorageBoundaryWalk.FOR_CONFLICTS)
        val types = collectStorageTypes(rootType, typeInfo.id)

        val attTypes = LinkedHashMap<String, DbColumnSemanticType>()
        val declaredShape = HashMap<String, Pair<DbColumnSemanticType, Boolean>>()
        val declaredBy = HashMap<String, String>()
        val conflicts = LinkedHashMap<String, MutableList<String>>()

        for (type in types) {
            val columns = ArrayList<EcosAttColumnDef>()
            columns.addAll(mapAttsToColumns(type.model.attributes, false))
            columns.addAll(mapAttsToColumns(type.model.systemAttributes, true))
            columns.addAll(getColumnsForAspects(type.aspects.map { it.ref }))
            for (column in columns) {
                val name = column.column.name
                val shape = DbColumnSemanticType.Model(column.attribute.type) to column.column.multiple
                val known = declaredShape[name]
                if (known == null) {
                    declaredShape[name] = shape
                    declaredBy[name] = type.id
                    attTypes[name] = shape.first
                } else if (known != shape) {
                    conflicts.computeIfAbsent(name) { mutableListOf(declaredBy.getValue(name)) }
                        .add(type.id)
                }
            }
        }
        // The grouping above is over-broad on purpose for blank source ids (see the KDoc), so a
        // conflict reported for this table may be between two types that do not really share one.
        // Surfaced through DbExpectedAttTypes.groupingMayBeOverBroad, not by writing into
        // `conflicts` itself, which is documented as a list of type ids and is the only
        // machine-readable description of a frozen column plans 4 and 5 will consume.
        val groupingMayBeOverBroad = rootType.sourceId.isBlank()

        // emptySet() and not the table's real child attributes: this object is built for its
        // conflicts, and every caller reads only those - nothing here reaches a migration.
        return DbExpectedAttTypes(attTypes, emptySet()) {
            DbExpectedAttTypes.ConflictsInfo(conflicts, groupingMayBeOverBroad)
        }
    }

    private fun mapAttsToColumns(atts: List<AttributeDef>, system: Boolean): List<EcosAttColumnDef> {
        return atts.mapNotNull {
            mapAttToColumn(it)?.let { columnDef ->
                EcosAttColumnDef(columnDef, it, system)
            }
        }
    }

    private fun mapAttToColumn(attribute: AttributeDef): DbColumnDef? {

        if (!VALID_COLUMN_NAME.matches(attribute.id)) {
            log.debug { "Attribute id '${attribute.id}' is not a valid column name and will be skipped" }
            return null
        }
        if (attribute.id.startsWith("_")) {
            log.debug { "Attribute id '${attribute.id}' starts with '_', but it is reserved system prefix" }
            return null
        }
        if (attribute.computed.type != ComputedAttType.NONE &&
            attribute.computed.storingType == ComputedAttStoringType.NONE
        ) {
            // computed attributes without storingType won't be stored in DB
            return null
        }

        val columnType = DbAttTypeColumns.getColumnType(attribute.type)
        val multiple = DbAttTypeColumns.isMultiple(attribute.type, attribute.multiple)
        val index = DbColumnIndexDef(attribute.index.enabled)

        return DbColumnDef(attribute.id, columnType, multiple, emptyList(), index)
    }
}
