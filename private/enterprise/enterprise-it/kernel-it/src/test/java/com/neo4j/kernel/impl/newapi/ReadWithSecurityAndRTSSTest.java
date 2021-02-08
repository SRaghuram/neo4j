/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */

package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

/**
 * {@link Read#relationshipTypeScan(int, RelationshipTypeIndexCursor, IndexOrder)} requires enabled RTSS. Enabled RTSS affects relationship counting functions,
 * therefore running full set of tests with RTSS enabled.
 */
public class ReadWithSecurityAndRTSSTest extends ReadWithSecurityTestBase<EnterpriseReadTestSupport>
{
    @Override
    public EnterpriseReadTestSupport newTestSupport()
    {
        return new EnterpriseReadTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder( Path storeDir )
            {
                TestDatabaseManagementServiceBuilder builder = super.newManagementServiceBuilder( storeDir );
                builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
                return builder;
            }
        };
    }

    @Test
    void relationshipTypeIndexScan() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        List<Long> ids = new ArrayList<>();
        try ( RelationshipTypeIndexCursor rels = cursors.allocateRelationshipTypeIndexCursor( NULL ) )
        {
            // when
            read.relationshipTypeScan( aType, rels, IndexOrder.NONE );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids ).containsExactlyInAnyOrder( fooAbar, barAfoo );
    }

    @Test
    void relationshipTypeIndexScanDeniedType() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        List<Long> ids = new ArrayList<>();
        try ( RelationshipTypeIndexCursor rels = cursors.allocateRelationshipTypeIndexCursor( NULL ) )
        {
            // when
            read.relationshipTypeScan( cType, rels, IndexOrder.NONE );
            while ( rels.next() )
            {
                ids.add( rels.relationshipReference() );
            }
        }

        // then
        assertThat( ids ).isEmpty();
    }
}
