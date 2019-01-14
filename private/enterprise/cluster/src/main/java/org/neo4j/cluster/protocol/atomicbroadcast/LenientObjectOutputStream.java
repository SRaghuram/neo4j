/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;

public class LenientObjectOutputStream extends ObjectOutputStream
{
    private VersionMapper versionMapper;

    public LenientObjectOutputStream( ByteArrayOutputStream bout, VersionMapper versionMapper ) throws IOException
    {
        super( bout );
        this.versionMapper = versionMapper;
    }

    @Override
    protected void writeClassDescriptor( ObjectStreamClass desc ) throws IOException
    {
        if ( versionMapper.hasMappingFor( desc.getName() ) )
        {
            updateWirePayloadSuid( desc );
        }

        super.writeClassDescriptor( desc );
    }

    private void updateWirePayloadSuid( ObjectStreamClass wirePayload )
    {
        try
        {
            Field field = getAccessibleSuidField( wirePayload );
            field.set( wirePayload, versionMapper.mappingFor( wirePayload.getName() ) );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Field getAccessibleSuidField( ObjectStreamClass localClassDescriptor ) throws NoSuchFieldException
    {
        Field suidField = localClassDescriptor.getClass().getDeclaredField( "suid" );
        suidField.setAccessible( true );
        return suidField;
    }
}
