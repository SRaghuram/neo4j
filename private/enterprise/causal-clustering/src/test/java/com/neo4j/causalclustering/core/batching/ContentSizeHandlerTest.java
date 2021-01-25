/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.neo4j.causalclustering.core.batching.Helpers.contentWithOneByte;
import static com.neo4j.causalclustering.core.batching.Helpers.emptyContent;
import static com.neo4j.causalclustering.core.batching.Helpers.message;
import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ContentSizeHandlerTest
{

    @ParameterizedTest
    @MethodSource( "contents" )
    void shouldProvideCorrectSize( ReplicatedContent content )
    {
        var message = Helpers.message( content );
        var size = ContentSizeHandler.of( message );

        assertEquals( content.size().orElse( 0L ), size );
    }

    @ParameterizedTest
    @MethodSource( "entries" )
    void shouldProvideCorrectSize( RaftLogEntry[] entries )
    {
        RaftMessages.InboundRaftMessageContainer<RaftMessages.AppendEntries.Request> message = message( 1, entries );
        var size = ContentSizeHandler.of( message );

        var expectedSize = stream( message.message().entries() ).flatMapToLong( e -> e.content().size().stream() ).sum();
        assertEquals( expectedSize, size );
    }

    static Stream<Arguments> entries()
    {
        return Stream.of( arguments( (Object) new RaftLogEntry[]{new RaftLogEntry( 1, contentWithOneByte() )} ),
                          arguments( (Object) new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() )} ),
                          arguments( (Object) new RaftLogEntry[]{new RaftLogEntry( 1, emptyContent() ), new RaftLogEntry( 1, contentWithOneByte() ),
                                                                 new RaftLogEntry( 1, contentWithOneByte() )} ),
                          arguments( (Object) RaftLogEntry.empty )
        );
    }

    static ReplicatedContent[] contents()
    {
        return new ReplicatedContent[]{contentWithOneByte(), emptyContent()};
    }
}
