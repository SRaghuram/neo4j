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

package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexReader;

import static org.neo4j.kernel.impl.index.schema.TokenIndexUpdater.rangeOf;

public class DefaultTokenIndexReader implements TokenIndexReader
{

    private final GBPTree<TokenScanKey,TokenScanValue> index;

    public DefaultTokenIndexReader( GBPTree<TokenScanKey,TokenScanValue> index )
    {
        this.index = index;
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityTokenClient client, IndexQueryConstraints constraints, TokenPredicate query )
    {
        try
        {
            final int tokenId = query.tokenId();
            final IndexOrder order = constraints.order();
            Seeker<TokenScanKey,TokenScanValue> seeker = seekerForToken( tokenId, order, context.cursorTracer() );
            IndexProgressor progressor = new TokenScanValueIndexProgressor( seeker, client, order );
            client.initialize( progressor, tokenId, order );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private Seeker<TokenScanKey,TokenScanValue> seekerForToken( int tokenId, IndexOrder indexOrder, PageCursorTracer cursorTracer )
            throws IOException
    {
        long rangeFrom = Long.MIN_VALUE;
        long rangeTo = Long.MAX_VALUE;

        if ( indexOrder == IndexOrder.DESCENDING )
        {
            long tmp = rangeFrom;
            rangeFrom = rangeTo;
            rangeTo = tmp;
        }

        TokenScanKey fromKey = new TokenScanKey( tokenId, rangeOf( rangeFrom ) );
        TokenScanKey toKey = new TokenScanKey( tokenId, rangeOf( rangeTo ) );
        return index.seek( fromKey, toKey, cursorTracer );
    }

    @Override
    public void close()
    {
        // nothing
    }
}
