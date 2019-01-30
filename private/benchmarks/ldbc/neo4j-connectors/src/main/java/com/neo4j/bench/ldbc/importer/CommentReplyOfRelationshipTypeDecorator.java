/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.Domain.Rels;

import java.util.Arrays;

import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class CommentReplyOfRelationshipTypeDecorator
        implements Decorator<InputRelationship>
{
    private static final String[] EMPTY_STRING_ARRAY = new String[]{};

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputRelationship apply( InputRelationship inputRelationship ) throws RuntimeException
    {
        // comment reply of comment/post
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment|
        // NOTE only replyOfPost and replyOfComment are passed through as properties,
        // and only the non-empty one will appear here
        long endNodeId;
        String newType;
        String replyOfType = (String) inputRelationship.properties()[0];
        if ( "replyOfPost".equals( replyOfType ) )
        {
            endNodeId = Long.parseLong( (String) inputRelationship.properties()[1] );
            newType = Rels.REPLY_OF_POST.name();
        }
        else if ( "replyOfComment".equals( replyOfType ) )
        {
            endNodeId = Long.parseLong( (String) inputRelationship.properties()[1] );
            newType = Rels.REPLY_OF_COMMENT.name();
        }
        else
        {
            throw new RuntimeException( String.format( "Both replyOfPost and replyOfComment columns had values\n%s",
                    Arrays.toString( inputRelationship.properties() ) ) );
        }

        return new InputRelationship(
                inputRelationship.sourceDescription(),
                inputRelationship.lineNumber(),
                inputRelationship.position(),
                EMPTY_STRING_ARRAY,
                (inputRelationship.hasFirstPropertyId()) ? inputRelationship.firstPropertyId() : null,
                inputRelationship.startNodeGroup(),
                inputRelationship.startNode(),
                inputRelationship.startNodeGroup(),
                endNodeId,
                newType,
                (inputRelationship.hasTypeId()) ? inputRelationship.typeId() : null
        );
    }
}
