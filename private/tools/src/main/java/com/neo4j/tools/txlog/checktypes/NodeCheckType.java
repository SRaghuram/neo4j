/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class NodeCheckType extends CheckType<Command.NodeCommand,NodeRecord>
{
    NodeCheckType()
    {
        super( Command.NodeCommand.class );
    }

    @Override
    public NodeRecord before( Command.NodeCommand command )
    {
        return command.getBefore();
    }

    @Override
    public NodeRecord after( Command.NodeCommand command )
    {
        return command.getAfter();
    }

    @Override
    protected boolean inUseRecordsEqual( NodeRecord record1, NodeRecord record2 )
    {
        return record1.getNextProp() == record2.getNextProp() &&
               record1.getNextRel() == record2.getNextRel() &&
               record1.isDense() == record2.isDense() &&
               record1.getLabelField() == record2.getLabelField();
    }

    @Override
    public String name()
    {
        return "node";
    }
}
