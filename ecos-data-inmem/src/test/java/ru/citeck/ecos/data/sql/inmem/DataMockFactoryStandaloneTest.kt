package ru.citeck.ecos.data.sql.inmem

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.data.sql.test.records.DataMockFactory
import ru.citeck.ecos.data.sql.test.records.DbRecordsTestBackends
import ru.citeck.ecos.model.lib.attributes.dto.AttributeDef
import ru.citeck.ecos.model.lib.attributes.dto.AttributeType

/**
 * Proves [DataMockFactory] is reusable WITHOUT extending it (no DbRecordsTestBase, no JUnit
 * lifecycle inheritance) and that the default backend is the in-memory one.
 */
class DataMockFactoryStandaloneTest {

    @Test
    fun usableWithoutInheritance() {

        // the SPI default backend is the Docker-free in-memory one
        assertThat(DbRecordsTestBackends.DEFAULT_BACKEND).isEqualTo("inmem")

        DataMockFactory().apply { setUp() }.use { factory ->

            factory.registerAtts(
                listOf(
                    AttributeDef.create()
                        .withId("textAtt")
                        .withType(AttributeType.TEXT)
                        .build()
                )
            )

            val rec = factory.createRecord("textAtt" to "hello")
            val read = factory.records.getAtt(rec, "textAtt").asText()

            assertThat(read).isEqualTo("hello")
        }
    }
}
