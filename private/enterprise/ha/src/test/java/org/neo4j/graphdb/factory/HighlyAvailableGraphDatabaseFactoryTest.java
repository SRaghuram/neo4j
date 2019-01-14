/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.graphdb.factory;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.ha.HaSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HighlyAvailableGraphDatabaseFactoryTest
{
    @Test
    public void shouldIncludeCorrectSettingsClasses()
    {
        // When
        GraphDatabaseFactoryState state = new HighlyAvailableGraphDatabaseFactory().getCurrentState();

        // Then
        assertThat( state.databaseDependencies().settingsClasses(),
            containsInAnyOrder( GraphDatabaseSettings.class, HaSettings.class, ClusterSettings.class ) );
    }
}
