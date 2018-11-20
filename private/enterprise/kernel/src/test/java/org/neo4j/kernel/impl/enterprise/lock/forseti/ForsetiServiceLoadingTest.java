/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.EmbeddedDbmsRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ForsetiServiceLoadingTest
{
    @Rule
    public EmbeddedDbmsRule dbRule = new EmbeddedDbmsRule().startLazily();

    @Test
    public void shouldUseForsetiAsDefaultLockManager()
    {
        // When
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( ForsetiLockManager.class ) );
    }

    @Test
    public void shouldAllowUsingCommunityLockManager()
    {
        // When
        dbRule.withSetting( GraphDatabaseSettings.lock_manager, "community" );
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // Then
        assertThat( db.getDependencyResolver().resolveDependency( Locks.class ), instanceOf( CommunityLockManger.class ) );
    }
}
