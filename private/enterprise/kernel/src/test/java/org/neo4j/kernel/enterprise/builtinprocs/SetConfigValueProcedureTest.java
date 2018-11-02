/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.matchers.NestedThrowableMatcher;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentEnterpriseDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.log_queries;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.plugin_dir;

public class SetConfigValueProcedureTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentEnterpriseDatabaseRule();

    @Rule
    public final ExpectedException expect = ExpectedException.none();

    @Test
    public void configShouldBeAffected()
    {
        Config config = db.resolveDependency( Config.class );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'false')" );
        assertFalse( config.get( log_queries ) );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'true')" );
        assertTrue( config.get( log_queries ) );
    }

    @Test
    public void failIfUnknownSetting()
    {
        expect.expect( new NestedThrowableMatcher( IllegalArgumentException.class ) );
        expect.expectMessage( "Unknown setting: unknown.setting.indeed" );

        db.execute( "CALL dbms.setConfigValue('unknown.setting.indeed', 'foo')" );
    }

    @Test
    public void failIfStaticSetting()
    {
        expect.expect( new NestedThrowableMatcher( IllegalArgumentException.class ) );
        expect.expectMessage( "Setting is not dynamic and can not be changed at runtime" );

        // Static setting, at least for now
        db.execute( "CALL dbms.setConfigValue('" + plugin_dir.name() + "', 'path/to/dir')" );
    }

    @Test
    public void failIfInvalidValue()
    {
        expect.expect( new NestedThrowableMatcher( InvalidSettingException.class ) );
        expect.expectMessage( "Bad value 'invalid' for setting 'dbms.logs.query.enabled': must be 'true' or 'false'" );

        db.execute( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'invalid')" );
    }
}
