/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.helper;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyHelpersTest
{
    private final UnpooledByteBufAllocator unpooledByteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
    private final PooledByteBufAllocator pooledByteBufAllocator = PooledByteBufAllocator.DEFAULT;

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAllowZeroSaftyMargin()
    {
        NettyHelpers.calculateChunkSize( unpooledByteBufAllocator, 0, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAllowAbove1SaftyMargin()
    {
        NettyHelpers.calculateChunkSize( unpooledByteBufAllocator, 1.1f, 1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAllowNegativeDefaultValue()
    {
        NettyHelpers.calculateChunkSize( unpooledByteBufAllocator, 0.9f, -1 );
    }

    @Test( expected = NullPointerException.class )
    public void shouldNotAllowNullBuffer()
    {
        NettyHelpers.calculateChunkSize( null, 0.9f, 1 );
    }

    @Test
    public void shouldUseDefaultValueIfNotPooled()
    {
        assertEquals( 1, NettyHelpers.calculateChunkSize( unpooledByteBufAllocator, 0.9f, 1 ) );
    }

    @Test
    public void shouldUsePooledBuffersValue()
    {
        int chunkSize = pooledByteBufAllocator.metric().chunkSize();
        assertEquals( chunkSize, NettyHelpers.calculateChunkSize( pooledByteBufAllocator, 1, 1 ) );
    }
}
