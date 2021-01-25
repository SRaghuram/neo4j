/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.Domain.Rels;

import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class CommentReplyOfRelationshipTypeDecorator implements Decorator
{
    private final Group group;

    public CommentReplyOfRelationshipTypeDecorator( Group group )
    {
        this.group = group;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        // comment reply of comment/post
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment|
        // NOTE only replyOfPost and replyOfComment are passed through as properties,
        // and only the non-empty one will appear here
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            @Override
            public boolean property( String key, Object value )
            {
                if ( "replyOfPost".equals( key ) )
                {
                    return super.type( Rels.REPLY_OF_POST.name() ) &&
                           super.endId( value, group );
                }
                else if ( "replyOfComment".equals( key ) )
                {
                    return super.type( Rels.REPLY_OF_COMMENT.name() ) &&
                           super.endId( value, group );
                }
                else
                {
                    return true;
                }
            }
        };
    }
}
