/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v3.messaging;

import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.messaging.MessageProcessingHandler;
import org.neo4j.bolt.v1.messaging.ResultHandler;
import org.neo4j.bolt.v1.messaging.decoder.AckFailureMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.DiscardAllMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.PullAllMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.ResetMessageDecoder;
import org.neo4j.bolt.v1.messaging.decoder.RunMessageDecoder;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

public class BoltRequestMessageReaderV3 extends BoltRequestMessageReader
{
    public BoltRequestMessageReaderV3( BoltConnection connection, BoltResponseMessageWriter responseMessageWriter,
            BoltMessageLogger messageLogger, LogService logService )
    {
        super( connection,
                newSimpleResponseHandler( connection, responseMessageWriter, logService ),
                buildDecoders( connection, responseMessageWriter, messageLogger, logService ),
                messageLogger );
    }

    private static List<RequestMessageDecoder> buildDecoders( BoltConnection connection, BoltResponseMessageWriter responseMessageWriter,
            BoltMessageLogger messageLogger, LogService logService )
    {
        BoltResponseHandler helloHandler = newSimpleResponseHandler( connection, responseMessageWriter, logService );
        BoltResponseHandler runHandler = newSimpleResponseHandler( connection, responseMessageWriter, logService );
        BoltResponseHandler resultHandler = new ResultHandler( responseMessageWriter, connection, internalLog( logService ) );
        BoltResponseHandler defaultHandler = newSimpleResponseHandler( connection, responseMessageWriter, logService );

        return Arrays.asList(
                new HelloMessageDecoder( helloHandler, messageLogger ),
                new AckFailureMessageDecoder( defaultHandler, messageLogger ),
                new ResetMessageDecoder( connection, defaultHandler, messageLogger ),
                new RunMessageDecoder( runHandler, messageLogger ),
                new DiscardAllMessageDecoder( resultHandler, messageLogger ),
                new PullAllMessageDecoder( resultHandler, messageLogger )
        );
    }

    private static BoltResponseHandler newSimpleResponseHandler( BoltConnection connection,
            BoltResponseMessageWriter responseMessageWriter, LogService logService )
    {
        return new MessageProcessingHandler( responseMessageWriter, connection, internalLog( logService ) );
    }

    private static Log internalLog( LogService logService )
    {
        return logService.getInternalLog( BoltRequestMessageReaderV3.class );
    }
}
