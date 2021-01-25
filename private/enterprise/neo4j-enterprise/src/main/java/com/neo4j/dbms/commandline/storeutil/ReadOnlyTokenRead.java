/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;

public class ReadOnlyTokenRead implements TokenRead
{
    private final TokenHolders tokenHolders;

    ReadOnlyTokenRead( TokenHolders tokenHolders )
    {
        this.tokenHolders = tokenHolders;
    }

    @Override
    public int nodeLabel( String name )
    {
        return tokenHolders.labelTokens().getIdByName( name );
    }

    @Override
    public String nodeLabelName( int labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            return tokenHolders.labelTokens().getTokenById( labelId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new LabelNotFoundKernelException( labelId, e );
        }
    }

    @Override
    public int relationshipType( String name )
    {
        return tokenHolders.relationshipTypeTokens().getIdByName( name );
    }

    @Override
    public String relationshipTypeName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        try
        {
            return tokenHolders.relationshipTypeTokens().getTokenById( relationshipTypeId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new RelationshipTypeIdNotFoundKernelException( relationshipTypeId, e );
        }
    }

    @Override
    public int propertyKey( String name )
    {
        return tokenHolders.propertyKeyTokens().getIdByName( name );
    }

    @Override
    public String propertyKeyName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        try
        {
            return tokenHolders.propertyKeyTokens().getTokenById( propertyKeyId ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new PropertyKeyIdNotFoundKernelException( propertyKeyId, e );
        }
    }

    @Override
    public Iterator<NamedToken> labelsGetAllTokens()
    {
        return tokenHolders.labelTokens().getAllTokens().iterator();
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens()
    {
        return tokenHolders.propertyKeyTokens().getAllTokens().iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens()
    {
        return tokenHolders.relationshipTypeTokens().getAllTokens().iterator();
    }

    @Override
    public int labelCount()
    {
        return tokenHolders.labelTokens().size();
    }

    @Override
    public int propertyKeyCount()
    {
        return tokenHolders.propertyKeyTokens().size();
    }

    @Override
    public int relationshipTypeCount()
    {
        return tokenHolders.relationshipTypeTokens().size();
    }

    @Override
    public String labelGetName( int labelId )
    {
        return tokenHolders.labelGetName( labelId );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId )
    {
        return tokenHolders.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId )
    {
        return tokenHolders.propertyKeyGetName( propertyKeyId );
    }
}
