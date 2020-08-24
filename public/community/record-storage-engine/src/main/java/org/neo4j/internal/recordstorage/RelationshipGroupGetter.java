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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

public class RelationshipGroupGetter
{
    private final IdSequence idGenerator;
    private final PageCursorTracer cursorTracer;

    public RelationshipGroupGetter( IdSequence idGenerator, PageCursorTracer cursorTracer )
    {
        this.idGenerator = idGenerator;
        this.cursorTracer = cursorTracer;
    }

    public RelationshipGroupPosition getRelationshipGroup( NodeRecord node, int type,
            RecordAccess<RelationshipGroupRecord> relGroupRecords )
    {
        long groupId = node.getNextRel();
        long previousGroupId = Record.NO_NEXT_RELATIONSHIP.intValue();
        RecordProxy<RelationshipGroupRecord> previous = null;
        RecordProxy<RelationshipGroupRecord> current;
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            current = relGroupRecords.getOrLoad( groupId, cursorTracer );
            RelationshipGroupRecord record = current.forReadingData();
            record.setPrev( previousGroupId ); // not persistent so not a "change"
            if ( record.getType() == type )
            {
                return new RelationshipGroupPosition( previous, current );
            }
            else if ( record.getType() > type )
            {   // The groups are sorted in the chain, so if we come too far we can return
                // empty handed right away
                return new RelationshipGroupPosition( previous, null );
            }
            previousGroupId = groupId;
            groupId = record.getNext();
            previous = current;
        }
        return new RelationshipGroupPosition( previous, null );
    }

    public RecordProxy<RelationshipGroupRecord> getOrCreateRelationshipGroup(
            RecordProxy<NodeRecord> nodeChange, int type, RecordAccess<RelationshipGroupRecord> relGroupRecords )
    {
        RelationshipGroupPosition existingGroup = getRelationshipGroup( nodeChange.forReadingLinkage(), type, relGroupRecords );
        RecordProxy<RelationshipGroupRecord> change = existingGroup.group();
        if ( change == null )
        {
            NodeRecord node = nodeChange.forChangingLinkage();
            assert node.isDense() : "Node " + node + " should have been dense at this point";
            long id = idGenerator.nextId( cursorTracer );
            change = relGroupRecords.create( id, cursorTracer );
            RelationshipGroupRecord record = change.forChangingData();
            record.setInUse( true );
            record.setCreated();
            record.setOwningNode( node.getId() );

            // Attach it...
            RecordProxy<RelationshipGroupRecord> closestPreviousChange = existingGroup.closestPrevious();
            if ( closestPreviousChange != null )
            {   // ...after the closest previous one
                RelationshipGroupRecord closestPrevious = closestPreviousChange.forChangingLinkage();
                record.setNext( closestPrevious.getNext() );
                record.setPrev( closestPrevious.getId() );
                closestPrevious.setNext( id );
            }
            else
            {   // ...first in the chain
                long firstGroupId = node.getNextRel();
                if ( firstGroupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
                {   // There are others, make way for this new group
                    RelationshipGroupRecord previousFirstRecord = relGroupRecords.getOrLoad( firstGroupId, cursorTracer ).forReadingData();
                    record.setNext( previousFirstRecord.getId() );
                    previousFirstRecord.setPrev( id );
                }
                node.setNextRel( id );
            }
        }
        return change;
    }

    public static class RelationshipGroupPosition
    {
        private final RecordProxy<RelationshipGroupRecord> closestPrevious;
        private final RecordProxy<RelationshipGroupRecord> group;

        public RelationshipGroupPosition( RecordProxy<RelationshipGroupRecord> closestPrevious,
                RecordProxy<RelationshipGroupRecord> group )
        {
            this.closestPrevious = closestPrevious;
            this.group = group;
        }

        public RecordProxy<RelationshipGroupRecord> group()
        {
            return group;
        }

        public RecordProxy<RelationshipGroupRecord> closestPrevious()
        {
            return closestPrevious;
        }
    }
}
