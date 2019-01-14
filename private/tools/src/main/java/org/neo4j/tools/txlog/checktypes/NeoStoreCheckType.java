/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog.checktypes;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;

public class NeoStoreCheckType extends CheckType<Command.NeoStoreCommand,NeoStoreRecord>
{
    NeoStoreCheckType()
    {
        super( Command.NeoStoreCommand.class );
    }

    @Override
    public NeoStoreRecord before( Command.NeoStoreCommand command )
    {
        return command.getBefore();
    }

    @Override
    public NeoStoreRecord after( Command.NeoStoreCommand command )
    {
        return command.getAfter();
    }

    @Override
    protected boolean inUseRecordsEqual( NeoStoreRecord record1, NeoStoreRecord record2 )
    {
        return record1.getNextProp() == record2.getNextProp();
    }

    @Override
    public String name()
    {
        return "neo_store";
    }
}
