/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.neo4j.configuration.GraphDatabaseSettings.lock_manager;

@ExtendWith( TestDirectoryExtension.class )
class ForsetiServiceLoadingTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldUseForsetiAsDefaultLockManager()
    {
        assertThat( getDBLocksInstance( Map.of() ), instanceOf( ForsetiLockManager.class ) );
    }

    @Test
    @DbmsExtension
    void shouldAllowUsingCommunityLockManager()
    {
        var cfg = getDBLocksInstance( Map.of( lock_manager, "community" ) );
        assertThat( cfg, instanceOf( CommunityLockManger.class ) );
    }

    private Locks getDBLocksInstance( Map<Setting<?>,Object> config )
    {
        DatabaseManagementService managementService = new CommercialDatabaseManagementServiceBuilder( directory.databaseDir() ).setConfig( config ).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        Locks locks = db.getDependencyResolver().resolveDependency( Locks.class );
        managementService.shutdown();
        return locks;
    }
}
