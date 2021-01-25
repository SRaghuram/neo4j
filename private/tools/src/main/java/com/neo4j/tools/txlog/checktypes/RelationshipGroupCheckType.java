/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

public class RelationshipGroupCheckType extends CheckType<Command.RelationshipGroupCommand,RelationshipGroupRecord>
{
    RelationshipGroupCheckType()
    {
        super( Command.RelationshipGroupCommand.class );
    }

    @Override
    public RelationshipGroupRecord before( Command.RelationshipGroupCommand command )
    {
        return command.getBefore();
    }

    @Override
    public RelationshipGroupRecord after( Command.RelationshipGroupCommand command )
    {
        return command.getAfter();
    }

    @Override
    protected boolean inUseRecordsEqual( RelationshipGroupRecord record1, RelationshipGroupRecord record2 )
    {
        return record1.getFirstIn() == record2.getFirstIn() &&
               record1.getFirstLoop() == record2.getFirstLoop() &&
               record1.getFirstOut() == record2.getFirstOut() &&
               record1.getNext() == record2.getNext() &&
               record1.getOwningNode() == record2.getOwningNode() &&
               record1.getPrev() == record2.getPrev() &&
               record1.getType() == record2.getType();
    }

    @Override
    public String name()
    {
        return "relationship_group";
    }
}
