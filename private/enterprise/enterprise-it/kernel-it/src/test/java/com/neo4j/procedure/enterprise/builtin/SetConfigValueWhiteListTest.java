/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.EnterpriseEditionSettings.dynamic_setting_allowlist;
import static java.util.List.of;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_parameter_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;

@TestDirectoryExtension
class SetConfigValueWhiteListTest
{
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI databaseAPI;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    private void prepareDatabase( List<String> settingWhitelist )
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                                    .setConfig( dynamic_setting_allowlist, settingWhitelist )
                                    .impermanent().build();
        databaseAPI = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    @Test
    void forbidDynamicSettingOfNotMatchedSetting()
    {
        prepareDatabase( of("dbmss*") );

        var rootCause = getRootCause( assertThrows( RuntimeException.class,
                () -> databaseAPI.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', '*')" ) ) );
        assertTrue( rootCause instanceof AuthorizationViolationException );
        assertEquals( "Failed to set value for `" + log_queries.name() + "` using procedure `dbms.setConfigValue`: access denied.",
                rootCause.getMessage() );
    }

    @Test
    void allowDynamicSettingChangeWhenItsMatchedByAnyPattern()
    {
        prepareDatabase( of( "dbmss*", "*" ) );

        assertDoesNotThrow( () -> databaseAPI.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'OFF')" ) );
    }

    @Test
    void allowDynamicSettingChangeWhenItsMatchedByMatchedPattern()
    {
        prepareDatabase( of( log_queries.name(), plugin_dir.name() ) );

        assertDoesNotThrow( () -> databaseAPI.executeTransactionally( "CALL dbms.setConfigValue('" + log_queries.name() + "', 'INFO')" ) );
    }

    @Test
    void forbidDynamicSettingChangeWhenNotMatchedBySettingsListPattern()
    {
        prepareDatabase( of( log_queries.name(), plugin_dir.name() ) );

        var rootCause = getRootCause( assertThrows( RuntimeException.class,
                () -> databaseAPI.executeTransactionally(  "CALL dbms.setConfigValue('" + log_queries_parameter_logging_enabled.name() + "', '*')" )) );
        assertTrue( rootCause instanceof AuthorizationViolationException );
        assertEquals(  "Failed to set value for `" + log_queries_parameter_logging_enabled.name() +
                        "` using procedure `dbms.setConfigValue`: access denied.", rootCause.getMessage() );
    }

    @Test
    void forbidDynamicSettingChangeWhenWhitelistIsEmpty()
    {
        prepareDatabase( of( "" ) );

        var rootCause = getRootCause( assertThrows( RuntimeException.class,
                () -> databaseAPI.executeTransactionally(  "CALL dbms.setConfigValue('" + log_queries_parameter_logging_enabled.name() + "', '*')" )) );
        assertTrue( rootCause instanceof AuthorizationViolationException );
        assertEquals(  "Failed to set value for `" + log_queries_parameter_logging_enabled.name() +
                "` using procedure `dbms.setConfigValue`: access denied.", rootCause.getMessage() );
    }
}
