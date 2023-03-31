package ru.citeck.ecos.data.sql.records.dao

interface DbRecordsDaoCtxAware {

    fun setRecordsDaoCtx(recordsDaoCtx: DbRecordsDaoCtx)
}
