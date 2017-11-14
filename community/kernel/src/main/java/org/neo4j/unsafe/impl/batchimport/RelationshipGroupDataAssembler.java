/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.LongArrayDataAssember;

public class RelationshipGroupDataAssembler extends LongArrayDataAssember<RelationshipGroupRecord>
{
    public RelationshipGroupDataAssembler()
    {
        super( 5 );
    }

    @Override
    protected void append( long[] array, RelationshipGroupRecord item, int index )
    {
        array[index * 5 + 0] = item.getOwningNode();
        array[index * 5 + 1] = item.getType();
        array[index * 5 + 2] = item.getFirstOut();
        array[index * 5 + 3] = item.getFirstIn();
        array[index * 5 + 4] = item.getFirstLoop();
    }
}
