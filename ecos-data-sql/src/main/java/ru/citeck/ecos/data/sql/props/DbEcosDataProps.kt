package ru.citeck.ecos.data.sql.props

/**
 * Global ecos-data properties. In webapp these properties
 * are loaded from the 'ecos.webapp.data' configuration section.
 */
class DbEcosDataProps(
    val assocs: AssocsProps = AssocsProps()
) {
    companion object {
        @JvmField
        val DEFAULT = DbEcosDataProps()
    }

    class AssocsProps(
        /**
         * Max count of association values which may be changed by providing
         * full values list in mutation. When record has more values than this
         * limit, then att_add_... and att_rem_... operations should be used.
         */
        val maxCountToEditByFullValuesList: Int = 150
    )
}
