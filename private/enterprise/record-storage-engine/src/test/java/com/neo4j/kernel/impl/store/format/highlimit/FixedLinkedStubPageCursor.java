/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

class FixedLinkedStubPageCursor extends StubPageCursor
{
    FixedLinkedStubPageCursor( int initialPageId, int size )
    {
        super( initialPageId, size );
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        // Since we always assume here that test data will be small enough for one page it's safe
        // to assume that all cursors will be be positioned into that one page.
        // And since stub cursors use byte buffers to store data we want to prevent data loss and keep already
        // created linked cursors
        if ( linkedCursor == null )
        {
            return super.openLinkedCursor( pageId );
        }
        return linkedCursor;
    }
}
