/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Stream factory for serializing/deserializing messages.
 */
public class ObjectStreamFactory implements ObjectInputStreamFactory, ObjectOutputStreamFactory
{
    private final VersionMapper versionMapper;

    public ObjectStreamFactory()
    {
        versionMapper = new VersionMapper();
    }

    @Override
    public ObjectOutputStream create( ByteArrayOutputStream bout ) throws IOException
    {
        return new LenientObjectOutputStream( bout, versionMapper );
    }

    @Override
    public ObjectInputStream create( ByteArrayInputStream in ) throws IOException
    {
        return new LenientObjectInputStream( in, versionMapper );
    }
}
