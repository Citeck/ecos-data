package ru.citeck.ecos.data.sql.pg.spring

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Auto configuration to initialize ecos-data beans.
 */
@Configuration
@ComponentScan(basePackages = ["ru.citeck.ecos.data.sql.pg.spring"])
open class EcosDataAutoConfiguration
