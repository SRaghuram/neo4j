/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class LenientObjectInputStream extends ObjectInputStream
{
    private VersionMapper versionMapper;

    public LenientObjectInputStream( ByteArrayInputStream fis, VersionMapper versionMapper ) throws IOException
    {
        super( fis );
        this.versionMapper = versionMapper;
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException
    {
        ObjectStreamClass wireClassDescriptor = super.readClassDescriptor();
        if ( !versionMapper.hasMappingFor( wireClassDescriptor.getName() ) )
        {
            versionMapper.addMappingFor( wireClassDescriptor.getName(), wireClassDescriptor.getSerialVersionUID() );
        }

        Class localClass; // the class in the local JVM that this descriptor represents.
        try
        {
            localClass = Class.forName( wireClassDescriptor.getName() );
        }
        catch ( ClassNotFoundException e )
        {
            return wireClassDescriptor;
        }
        ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup( localClass );
        if ( localClassDescriptor != null )
        {
            final long localSUID = localClassDescriptor.getSerialVersionUID();
            final long wireSUID = wireClassDescriptor.getSerialVersionUID();
            if ( wireSUID != localSUID )
            {
                wireClassDescriptor = localClassDescriptor;
            }
        }
        return wireClassDescriptor;
    }
}
