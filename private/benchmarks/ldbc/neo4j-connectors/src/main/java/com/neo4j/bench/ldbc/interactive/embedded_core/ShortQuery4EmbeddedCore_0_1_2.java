/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery4;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;

public class ShortQuery4EmbeddedCore_0_1_2 extends Neo4jShortQuery4<Neo4jConnectionState>
{
    @Override
    public LdbcShortQuery4MessageContentResult execute( Neo4jConnectionState connection,
            LdbcShortQuery4MessageContent operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node message = Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.messageId() );
        long messageCreationDate = dateUtil.formatToUtc( (long) message.getProperty( Message.CREATION_DATE ) );
        String messageContent = (message.hasProperty( Message.CONTENT ))
                                ? (String) message.getProperty( Message.CONTENT )
                                : (String) message.getProperty( Post.IMAGE_FILE );
        return new LdbcShortQuery4MessageContentResult( messageContent, messageCreationDate );
    }
}
