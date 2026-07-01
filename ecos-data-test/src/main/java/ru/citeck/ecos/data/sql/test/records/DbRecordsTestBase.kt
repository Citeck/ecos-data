package ru.citeck.ecos.data.sql.test.records

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

/**
 * JUnit-lifecycle test base over [DataMockFactory]: wires the factory's [DataMockFactory.setUp] /
 * [DataMockFactory.close] into `@BeforeEach`/`@AfterEach`. All the records-dao infrastructure (and
 * the `assume*` capability guards) lives in [DataMockFactory], which can be used standalone without
 * extending this.
 */
abstract class DbRecordsTestBase : DataMockFactory() {

    @BeforeEach
    fun beforeEachBase() {
        setUp()
    }

    @AfterEach
    fun afterEachBase() {
        close()
    }
}
