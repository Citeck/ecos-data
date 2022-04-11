package ru.citeck.ecos.data.sql.job

interface DbJob {

    fun getPeriod(): Long

    fun getInitDelay(): Long

    fun execute(): Boolean
}
