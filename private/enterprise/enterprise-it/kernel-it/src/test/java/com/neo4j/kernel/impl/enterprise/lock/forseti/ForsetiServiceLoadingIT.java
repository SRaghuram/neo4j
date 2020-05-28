/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager;

@TestDirectoryExtension
class ForsetiServiceLoadingTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldUseForsetiAsDefaultLockManager()
    {
        assertThat( getDBLocksInstance( Map.of() ) ).isInstanceOf( ForsetiLockManager.class );
    }

    @Test
    void shouldAllowUsingCommunityLockManager()
    {
        var cfg = getDBLocksInstance( Map.of( lock_manager, "community" ) );
        assertThat( cfg ).isInstanceOf( CommunityLockManger.class );
    }

    private Locks getDBLocksInstance( Map<Setting<?>,Object> config )
    {
        DatabaseManagementService managementService =
                new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() ).setConfig( config ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        Locks locks = db.getDependencyResolver().resolveDependency( Locks.class );
        managementService.shutdown();
        return locks;
    }
}
