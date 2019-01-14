/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.nio.channels.ReadableByteChannel;

public interface MadeUpCommunicationInterface
{
    Response<Integer> multiply( int value1, int value2 );

    Response<Void> fetchDataStream( MadeUpWriter writer, int dataSize );

    Response<Void> sendDataStream( ReadableByteChannel data );

    Response<Integer> throwException( String messageInException );

    Response<Integer> streamBackTransactions( int responseToSendBack, int txCount );

    Response<Integer> informAboutTransactionObligations( int responseToSendBack, long desiredObligation );
}
