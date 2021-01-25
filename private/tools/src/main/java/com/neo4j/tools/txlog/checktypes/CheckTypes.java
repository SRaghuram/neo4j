/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import java.util.List;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public final class CheckTypes
{
    public static final NodeCheckType NODE = new NodeCheckType();
    public static final PropertyCheckType PROPERTY = new PropertyCheckType();
    public static final RelationshipCheckType RELATIONSHIP = new RelationshipCheckType();
    public static final RelationshipGroupCheckType RELATIONSHIP_GROUP = new RelationshipGroupCheckType();

    public static final List<CheckType<?,?>> CHECK_TYPES =
            List.of( NODE, PROPERTY, RELATIONSHIP, RELATIONSHIP_GROUP );

    private CheckTypes()
    {
    }

    public static <C extends Command,R extends AbstractBaseRecord> CheckType<C,R> fromName( String name )
    {
        for ( CheckType<?,?> checkType : CHECK_TYPES )
        {
            if ( checkType.name().equals( name ) )
            {
                //noinspection unchecked
                return (CheckType<C,R>) checkType;
            }
        }
        throw new IllegalArgumentException( "Unknown check named " + name );
    }
}
