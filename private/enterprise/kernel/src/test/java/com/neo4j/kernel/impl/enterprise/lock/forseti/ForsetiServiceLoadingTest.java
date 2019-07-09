/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@DbmsExtension
class ForsetiServiceLoadingTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldUseForsetiAsDefaultLockManager()
    {
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( ForsetiLockManager.class ) );
    }

    @Test
    @DbmsExtension( configurationCallback = "configureCommunityLockManager" )
    void shouldAllowUsingCommunityLockManager()
    {
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( CommunityLockManger.class ) );
    }

    @ExtensionCallback
    void configureCommunityLockManager( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.lock_manager, "community" );
    }
}
