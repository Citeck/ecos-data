package ru.citeck.ecos.data.sql.columnmeta

/**
 * The three outcomes of spec section 5, in increasing order of caution. The order is load-bearing:
 * [DbColumnConversions] combines the type dimension with the multiplicity dimension, and combines
 * the candidates of an unknown source type, by taking the **larger** ordinal - the more cautious
 * answer always wins.
 */
enum class DbConversionClass {

    /**
     * Every value survives. The old column is still kept as a backup.
     */
    SAFE,

    /**
     * The transfer is worth attempting, but some values may not fit. The backup is permanent.
     */
    LOSSY,

    /**
     * There is nothing worth transferring. The new column stays empty and the backup is permanent.
     */
    NONE
}
