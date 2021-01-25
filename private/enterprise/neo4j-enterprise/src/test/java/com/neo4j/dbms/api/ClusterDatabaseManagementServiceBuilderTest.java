/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.api;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;

@TestDirectoryExtension
class ClusterDatabaseManagementServiceBuilderTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    void augmentsConfigWithMaxTxSize()
    {
        // given
        var config = Config.defaults();
        var builder = new ClusterDatabaseManagementServiceBuilder( temporaryFolder.directory( "home" ) );

        // when config only has default values
        var newConfig = builder.augmentConfig( config );

        // then setting should be within limits
        assertThat( newConfig.get( GraphDatabaseSettings.memory_transaction_max_size ) ).isLessThanOrEqualTo( ByteUnit.gibiBytes( 2 ) );
        assertThat( newConfig.get( GraphDatabaseSettings.memory_transaction_max_size ) ).isPositive();

        // when setting is dynamically set to 0
        newConfig.setDynamic( GraphDatabaseSettings.memory_transaction_max_size, 0L, getClass().getSimpleName() );

        // then setting should still be within limits
        assertThat( newConfig.get( GraphDatabaseSettings.memory_transaction_max_size ) ).isLessThanOrEqualTo( ByteUnit.gibiBytes( 2 ) );
        assertThat( newConfig.get( GraphDatabaseSettings.memory_transaction_max_size ) ).isPositive();
    }

}
