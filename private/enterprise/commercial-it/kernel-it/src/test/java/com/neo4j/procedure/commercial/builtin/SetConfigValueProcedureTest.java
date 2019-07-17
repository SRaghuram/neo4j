/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.commercial.builtin;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;

@CommercialDbmsExtension
class SetConfigValueProcedureTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void configShouldBeAffected()
    {
        Config config = db.getDependencyResolver().resolveDependency( Config.class );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'false')" );
        assertFalse( config.get( log_queries ) );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'true')" );
        assertTrue( config.get( log_queries ) );
    }

    @Test
    void failIfUnknownSetting()
    {
        Throwable rootCause = Exceptions.rootCause(
                assertThrows( RuntimeException.class, () -> db.execute( "CALL dbms.setConfigValue('unknown.setting.indeed', 'foo')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "Setting `unknown.setting.indeed` not found", rootCause.getMessage() );
    }

    @Test
    void failIfStaticSetting()
    {
        // Static setting, at least for now
        Throwable rootCause = Exceptions.rootCause(
                assertThrows( RuntimeException.class, () -> db.execute( "CALL dbms.setConfigValue('" + plugin_dir.name() + "', 'path/to/dir')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "Setting 'dbms.directories.plugins' is not dynamic and can not be changed at runtime", rootCause.getMessage() );
    }

    @Test
    void failIfInvalidValue()
    {
        Throwable rootCause = Exceptions.rootCause(
                assertThrows( RuntimeException.class, () -> db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'invalid')" ) ) );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "'invalid' is not a valid boolean value, must be 'true' or 'false'", rootCause.getMessage() );
    }
}
