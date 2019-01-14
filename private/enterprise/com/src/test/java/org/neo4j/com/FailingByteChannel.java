/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FailingByteChannel extends KnownDataByteChannel
{
    private final String failWithMessage;
    private final int sizeToFailAt;

    public FailingByteChannel( int sizeToFailAt, String failWithMessage )
    {
        super( sizeToFailAt * 2 );
        this.sizeToFailAt = sizeToFailAt;
        this.failWithMessage = failWithMessage;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( position > sizeToFailAt )
        {
            throw new MadeUpException( failWithMessage );
        }
        return super.read( dst );
    }
}
