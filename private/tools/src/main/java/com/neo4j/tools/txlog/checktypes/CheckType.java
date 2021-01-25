/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import java.util.Objects;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Type of command ({@link NodeCommand}, {@link PropertyCommand}, ...) to check during transaction log verification.
 * This class exists to mitigate the absence of interfaces for commands with before and after state.
 * It also provides an alternative equality check instead of {@link AbstractBaseRecord#equals(Object)} that only
 * checks {@linkplain AbstractBaseRecord#getId() entity id}.
 *
 * @param <C> the type of command to check
 * @param <R> the type of records that this command contains
 */
public abstract class CheckType<C extends Command, R extends AbstractBaseRecord>
{
    private final Class<C> recordClass;

    CheckType( Class<C> recordClass )
    {
        this.recordClass = recordClass;
    }

    public Class<C> commandClass()
    {
        return recordClass;
    }

    public abstract R before( C command );

    public abstract R after( C command );

    public final boolean equal( R record1, R record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        if ( record1.getId() != record2.getId() )
        {
            return false;
        }
        else if ( record1.inUse() != record2.inUse() )
        {
            return false;
        }
        else if ( !record1.inUse() )
        {
            return true;
        }
        else
        {
            return inUseRecordsEqual( record1, record2 );
        }
    }

    protected abstract boolean inUseRecordsEqual( R record1, R record2 );

    public abstract String name();
}
