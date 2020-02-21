/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.collection.Iterables.single;

@TestDirectoryExtension
public class FrekiRestartIT
{
    private static final String DB_NAME = "freki.db";
    private static final Label LABEL = Label.label( "dude" );
    private static final RelationshipType TYPE = RelationshipType.withName( "TYPE" );

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI frekiDb;

    @BeforeEach
    void setUp()
    {
        restartDbms();
    }

    @AfterEach
    void tearDown()
    {
        shutdownDbms();
    }

    private void restartDbms()
    {
        shutdownDbms();
        Config config = Config.newBuilder()
                .set( neo4j_home, testDirectory.homeDir().toPath() )
                .set( default_database, DB_NAME )
                .build();

        dbms = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( config ).build();
        frekiDb = (GraphDatabaseAPI) dbms.database( DB_NAME );
        StorageEngineFactory storageEngineFactory = frekiDb.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        assertThat( storageEngineFactory instanceof FrekiStorageEngineFactory ).isTrue();
    }

    private void shutdownDbms()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
            dbms = null;
            frekiDb = null;
        }
    }

    @Test
    void shouldBeAbleToRestartEmptyDbms()
    {
        assertDoesNotThrow( this::restartDbms );
    }

    @Test
    void shouldBeAbleToReadSmallGraphAfterRestart()
    {
        long nodeId;
        long otherNodeId;
        try ( Transaction tx = frekiDb.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( "name", "Exists!" );
            Node otherNode = tx.createNode();
            node.createRelationshipTo( otherNode, TYPE );
            nodeId = node.getId();
            otherNodeId = otherNode.getId();
            tx.commit();
        }
        assertDoesNotThrow( this::restartDbms );
        try ( Transaction tx = frekiDb.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            Node otherNode = tx.getNodeById( otherNodeId );
            assertThat( node.hasLabel( LABEL ) ).isTrue();
            assertThat( otherNode.hasLabel( LABEL ) ).isFalse();
            assertThat( node.getProperty( "name" ) ).isEqualTo( "Exists!" );
            Relationship relationship = single( node.getRelationships() );
            assertThat( relationship.getEndNode() ).isEqualTo( otherNode );
            tx.commit();
        }
    }
}
