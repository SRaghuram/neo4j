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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.api.NamedToken;

/**
 * This is a utility to use if using the {@link FrekiStorageEngine} directly w/o the kernel on top. In this scenario the token management is
 * quite bananas and needs this indirection and lazy lookup to even make sense.
 */
class ProxyImmediateTokenCreator implements TokenCreator
{
    private final Supplier<GBPTreeTokenStore> tokenStoreSupplier;

    ProxyImmediateTokenCreator( Supplier<GBPTreeTokenStore> tokenStoreSupplier )
    {
        this.tokenStoreSupplier = tokenStoreSupplier;
    }

    @Override
    public synchronized int createToken( String name, boolean internal )
    {
        GBPTreeTokenStore tokenStore = tokenStoreSupplier.get();
        int id = tokenStore.nextTokenId( PageCursorTracer.NULL );
        try
        {
            tokenStore.writeToken( new NamedToken( name, id ), PageCursorTracer.NULL );
            return id;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
