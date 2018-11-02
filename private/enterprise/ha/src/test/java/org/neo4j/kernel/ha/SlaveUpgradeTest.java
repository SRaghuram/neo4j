/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SlaveUpgradeTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void haShouldFailToStartWithOldStore()
    {
        try
        {
            File storeDirectory = testDirectory.directory( "haShouldFailToStartWithOldStore" );
            File databaseDirectory = testDirectory.databaseDir( storeDirectory );
            MigrationTestUtils.find23FormatStoreDirectory( databaseDirectory );

            new TestHighlyAvailableGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( databaseDirectory )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:9999" )
                    .newGraphDatabase();

            fail( "Should exit abnormally" );
        }
        catch ( Exception e )
        {
            Throwable rootCause = Exceptions.rootCause( e );
            assertThat( rootCause, instanceOf( UpgradeNotAllowedByConfigurationException.class ) );
        }
    }
}
