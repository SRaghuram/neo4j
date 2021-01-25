/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.EnumSet;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;

@EnterpriseDbmsExtension
class SetConfigValueProcedureTest
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private Config config;

    @Test
    void configShouldBeAffected()
    {
        db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'off')" );
        assertEquals( LogQueryLevel.OFF, config.get( log_queries ) );

        db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'info')" );
        assertEquals( LogQueryLevel.INFO, config.get( log_queries ) );

        db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'verbose')" );
        assertEquals( LogQueryLevel.VERBOSE, config.get( log_queries ) );
    }

    @Test
    void getDefaultValueOnEmptyArg()
    {
        db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries_threshold.name() + "', '11s')" );
        assertEquals( Duration.ofSeconds( 11 ), config.get( log_queries_threshold ) );

        db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries_threshold.name() + "', '')" );
        assertEquals( log_queries_threshold.defaultValue(), config.get( log_queries_threshold ) );
    }

    @Test
    void failIfUnknownSetting()
    {
        Throwable rootCause = getRootCause(
                assertThrows( RuntimeException.class, () -> db.executeTransactionally( "CALL dbms.setConfigValue('unknown.setting.indeed', 'foo')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "Setting `unknown.setting.indeed` not found", rootCause.getMessage() );
    }

    @Test
    void failIfStaticSetting()
    {
        // Static setting, at least for now
        Throwable rootCause = getRootCause( assertThrows( RuntimeException.class,
                () -> db.executeTransactionally( "CALL dbms.setConfigValue('" + plugin_dir.name() + "', 'path/to/dir')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "Setting 'dbms.directories.plugins' is not dynamic and can not be changed at runtime", rootCause.getMessage() );
    }

    @Test
    void failIfInvalidValue()
    {
        Throwable rootCause = getRootCause( assertThrows( RuntimeException.class,
                () -> db.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'invalid')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "'invalid' not one of " + EnumSet.allOf( GraphDatabaseSettings.LogQueryLevel.class ), rootCause.getMessage() );
    }
}
