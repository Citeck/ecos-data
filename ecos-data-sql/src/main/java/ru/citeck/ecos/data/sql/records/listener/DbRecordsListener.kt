package ru.citeck.ecos.data.sql.records.listener

interface DbRecordsListener {

    fun onMutated(event: MutationEvent)

    fun onDeleted(event: DeletionEvent)
}
