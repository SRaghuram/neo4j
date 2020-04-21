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
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@TestDirectoryExtension
public class FrekiWritesIT
{
    private static final String DB_NAME = "freki.db";
    private static final RelationshipType TYPE1 = RelationshipType.withName( "TYPE1" );
    private static final RelationshipType TYPE2 = RelationshipType.withName( "TYPE2" );

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI frekiDb;

    @BeforeEach
    void setUp()
    {
        Config config = Config.newBuilder()
                .set( neo4j_home, testDirectory.homeDir().toPath() )
                .set( default_database, DB_NAME )
                .build();

        dbms = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() ).setConfig( config ).build();
        frekiDb = (GraphDatabaseAPI) dbms.database( DB_NAME );
        StorageEngineFactory storageEngineFactory = frekiDb.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        assertThat( storageEngineFactory instanceof FrekiStorageEngineFactory ).isTrue();
    }

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldReadAndWriteDegreesCorrectly()
    {
        long nodeId;
        try ( var tx = frekiDb.beginTx() )
        {
            nodeId = tx.createNode().getId();
            tx.commit();
        }

        for ( int i = 1; i < 500; i++ ) // 2k relationships should be enough to eventually go dense
        {
            try ( var tx = frekiDb.beginTx() )
            {
                Node node = tx.getNodeById( nodeId );

                Node otherNode1 = tx.createNode();
                Node otherNode2 = tx.createNode();

                node.createRelationshipTo( otherNode1, TYPE1 ); //two outgoing
                node.createRelationshipTo( otherNode2, TYPE2 );
                otherNode1.createRelationshipTo( node, TYPE1 ); //one incoming
                node.createRelationshipTo( node, TYPE1 ); //one loop

                String msg = "Iteration " + i;
                assertEquals( i * 4, node.getDegree(), msg );
                assertEquals( i * 3, node.getDegree( TYPE1 ), msg );
                assertEquals( i, node.getDegree( TYPE2 ), msg );
                assertEquals( i * 3, node.getDegree( Direction.OUTGOING ), msg );
                assertEquals( i * 2, node.getDegree( Direction.INCOMING ), msg );
                assertEquals( 0, node.getDegree( TYPE2, Direction.INCOMING ), msg );
                assertEquals( i, node.getDegree( TYPE2, Direction.OUTGOING ), msg );
                assertEquals( i * 2, node.getDegree( TYPE1, Direction.INCOMING ), msg );
                assertEquals( i * 2, node.getDegree( TYPE1, Direction.OUTGOING ), msg );
                tx.commit();
            }
        }
    }
}
