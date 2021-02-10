/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

@DbmsExtension( configurationCallback = "configure" )
@ExtendWith( RandomExtension.class )
class KernelAPIParallelTypeScanStressIT
{
    private static final int N_THREADS = 10;
    private static final int N_RELS = 10_000;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private RandomRule random;
    @Inject
    private Kernel kernel;

    @Test
    void shouldDoParallelTypeScans() throws Throwable
    {
        int[] types = new int[3];

        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            types[0] = createRelationships( tx, N_RELS, "TYPE1" );
            types[1] = createRelationships( tx, N_RELS, "TYPE2" );
            types[2] = createRelationships( tx, N_RELS, "TYPE3" );
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx( kernel,
                                                    N_THREADS,
                                                    tx -> tx.cursors().allocateRelationshipTypeIndexCursor( PageCursorTracer.NULL ),
                                                    ( read, cursor ) -> typeScan( read,
                                                                                  cursor,
                                                                                  types[random.nextInt( types.length )] ) );
    }

    private static int createRelationships( KernelTransaction tx, int count, String typeName ) throws KernelException
    {
        int type = tx.tokenWrite().relationshipTypeCreateForName( typeName, false );
        for ( int i = 0; i < count; i++ )
        {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( n1, type, n2 );
        }
        return type;
    }

    private Runnable typeScan( Read read, RelationshipTypeIndexCursor cursor, int label )
    {
        return () ->
        {
            read.relationshipTypeScan( label, cursor, IndexOrder.NONE );

            int n = 0;
            while ( cursor.next() )
            {
                n++;
            }
            assertThat( n ).as( "correct number of relationships" ).isEqualTo( N_RELS );
        };
    }

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
    }
}
