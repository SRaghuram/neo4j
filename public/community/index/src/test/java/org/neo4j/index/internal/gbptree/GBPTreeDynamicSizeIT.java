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
package org.neo4j.index.internal.gbptree;

import org.neo4j.test.rule.RandomRule;

import static org.neo4j.index.internal.gbptree.TreeNodeDynamicSize.keyValueSizeCapFromPageSize;

public class GBPTreeDynamicSizeIT extends GBPTreeITBase<RawBytes,RawBytes>
{
    @Override
    TestLayout<RawBytes,RawBytes> getLayout( RandomRule random, int pageSize )
    {
        return new SimpleByteArrayLayout( keyValueSizeCapFromPageSize( pageSize ) / 2, random.intBetween( 0, 10 ) );
    }

    @Override
    Class<RawBytes> getKeyClass()
    {
        return RawBytes.class;
    }
}
