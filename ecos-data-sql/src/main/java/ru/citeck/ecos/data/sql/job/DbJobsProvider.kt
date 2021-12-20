package ru.citeck.ecos.data.sql.job

interface DbJobsProvider {

    fun getJobs(): List<DbJob>
}
