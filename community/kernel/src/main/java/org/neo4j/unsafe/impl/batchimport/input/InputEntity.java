/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple utility for gathering all information about an {@link InputEntityVisitor} and exposing getters
 * for that data. Easier to work with than purely visitor-based implementation in tests.
 */
public class InputEntity implements InputEntityVisitor, Cloneable
{
    public static final Object[] NO_PROPERTIES = new Object[0];
    public static final String[] NO_LABELS = new String[0];

    private final InputEntityVisitor delegate;

    public InputEntity( InputEntityVisitor delegate )
    {
        this.delegate = delegate;
        clear();
    }

    public InputEntity()
    {
        this( new InputEntityVisitor.Adapter() );
    }

    public boolean hasPropertyId;
    public long propertyId;
    public boolean hasIntPropertyKeyIds;
    public final List<Object> properties = new ArrayList<>();

    public boolean hasLongId;
    public long longId;
    public Object objectId;
    public Group idGroup;

    public final List<String> labels = new ArrayList<>();
    public boolean hasLabelField;
    public long labelField;

    public boolean hasLongStartId;
    public long longStartId;
    public Object objectStartId;
    public Group startIdGroup;

    public boolean hasLongEndId;
    public long longEndId;
    public Object objectEndId;
    public Group endIdGroup;

    public boolean hasIntType;
    public int intType;
    public String stringType;

    private boolean end;

    @Override
    public boolean propertyId( long nextProp )
    {
        checkClear();
        hasPropertyId = true;
        propertyId = nextProp;
        return delegate.propertyId( nextProp );
    }

    @Override
    public boolean property( String key, Object value )
    {
        checkClear();
        properties.add( key );
        properties.add( value );
        return delegate.property( key, value );
    }

    @Override
    public boolean property( int propertyKeyId, Object value )
    {
        checkClear();
        hasIntPropertyKeyIds = true;
        properties.add( propertyKeyId );
        properties.add( value );
        return delegate.property( propertyKeyId, value );
    }

    @Override
    public boolean id( long id )
    {
        checkClear();
        hasLongId = true;
        longId = id;
        return delegate.id( id );
    }

    @Override
    public boolean id( Object id, Group group )
    {
        checkClear();
        objectId = id;
        idGroup = group;
        return delegate.id( id, group );
    }

    @Override
    public boolean labels( String[] labels )
    {
        checkClear();
        for ( String label : labels )
        {
            this.labels.add( label );
        }
        return delegate.labels( labels );
    }

    @Override
    public boolean labelField( long labelField )
    {
        checkClear();
        hasLabelField = true;
        this.labelField = labelField;
        return delegate.labelField( labelField );
    }

    @Override
    public boolean startId( long id )
    {
        checkClear();
        hasLongStartId = true;
        longStartId = id;
        return delegate.startId( id );
    }

    @Override
    public boolean startId( Object id, Group group )
    {
        checkClear();
        objectStartId = value( id );
        startIdGroup = group;
        return delegate.startId( id, group );
    }

    @Override
    public boolean endId( long id )
    {
        checkClear();
        hasLongEndId = true;
        longEndId = id;
        return delegate.endId( id );
    }

    @Override
    public boolean endId( Object id, Group group )
    {
        checkClear();
        objectEndId = value( id );
        endIdGroup = group;
        return delegate.endId( id, group );
    }

    @Override
    public boolean type( int type )
    {
        checkClear();
        hasIntType = true;
        intType = type;
        return delegate.type( type );
    }

    @Override
    public boolean type( String type )
    {
        checkClear();
        stringType = type;
        return delegate.type( type );
    }

    @Override
    public void endOfEntity() throws IOException
    {
        // Mark that the next call to any data method should clear the state
        end = true;
        delegate.endOfEntity();
    }

    public String[] labels()
    {
        return labels.toArray( new String[labels.size()] );
    }

    public Object[] properties()
    {
        return properties.toArray();
    }

    public Object id()
    {
        return hasLongId ? longId : objectId;
    }

    public Object endId()
    {
        return hasLongEndId ? longEndId : objectEndId;
    }

    public Object startId()
    {
        return hasLongStartId ? longStartId : objectStartId;
    }

    private void checkClear()
    {
        if ( end )
        {
            clear();
        }
    }

    private void clear()
    {
        end = false;
        hasPropertyId = false;
        propertyId = -1;
        hasIntPropertyKeyIds = false;
        properties.clear();
        hasLongId = false;
        longId = -1;
        objectId = null;
        idGroup = Group.GLOBAL;
        labels.clear();
        hasLabelField = false;
        labelField = -1;
        hasLongStartId = false;
        longStartId = -1;
        objectStartId = null;
        startIdGroup = Group.GLOBAL;
        hasLongEndId = false;
        longEndId = -1;
        objectEndId = null;
        endIdGroup = Group.GLOBAL;
        hasIntType = false;
        intType = -1;
        stringType = null;
    }

    @Override
    public InputEntity clone() throws CloneNotSupportedException
    {
        InputEntity clone = new InputEntity();
        clone.hasPropertyId = hasPropertyId;
        clone.propertyId = propertyId;
        clone.hasIntPropertyKeyIds = hasIntPropertyKeyIds;
        clone.properties.addAll( properties );
        clone.hasLongId = hasLongId;
        clone.longId = longId;
        clone.objectId = objectId;
        clone.idGroup = idGroup;
        clone.labels.addAll( labels );
        clone.hasLabelField = hasLabelField;
        clone.labelField = labelField;
        clone.hasLongStartId = hasLongStartId;
        clone.longStartId = longStartId;
        clone.objectStartId = objectStartId;
        clone.startIdGroup = startIdGroup;
        clone.hasLongEndId = hasLongEndId;
        clone.longEndId = longEndId;
        clone.objectEndId = objectEndId;
        clone.endIdGroup = endIdGroup;
        clone.hasIntType = hasIntType;
        clone.intType = intType;
        clone.stringType = stringType;
        return clone;
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

    public int propertyCount()
    {
        return properties.size() / 2;
    }

    public Object propertyValue( int i )
    {
        return properties.get( i * 2 + 1 );
    }
}
