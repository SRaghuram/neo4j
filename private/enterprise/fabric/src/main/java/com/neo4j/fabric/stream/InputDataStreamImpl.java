/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.neo4j.cypher.internal.runtime.InputCursor;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.values.AnyValue;

public class InputDataStreamImpl implements InputDataStream
{
    private final Rx2SyncStream wrappedStream;
    private InputCursor inputCursor;

    public InputDataStreamImpl( Rx2SyncStream wrappedStream )
    {
        this.wrappedStream = wrappedStream;
        inputCursor = new Cursor();
    }

    @Override
    public InputCursor nextInputBatch()
    {
        return inputCursor;
    }

    private class Cursor implements InputCursor
    {

        private Record currentRecord;

        @Override
        public boolean next()
        {
            currentRecord = wrappedStream.readRecord();

            if ( currentRecord != null )
            {
                return true;
            }

            inputCursor = null;
            return false;
        }

        @Override
        public AnyValue value( int offset )
        {
            return currentRecord.getValue( offset );
        }

        @Override
        public void close()
        {
            // TODO what exactly is close on the cursor supposed to do?
        }
    }
}
