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
package org.neo4j.kernel.api.properties;
import org.neo4j.kernel.impl.api.state.PagedCache;

final public class  StringProperty extends DefinedProperty implements DefinedProperty.WithStringValue
{
	/*
		private final String value;

    StringProperty( int propertyKeyId, String value )
    {
        super( propertyKeyId );
        assert value != null;
        this.value = value;
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return valueEquals( value, other );
    }

    static boolean valueEquals( String value, Object other )
    {
        if ( other instanceof String )
        {
            return value.equals( other );
        }
        if ( other instanceof Character )
        {
            Character that = (Character) other;
            return value.length() == 1 && value.charAt( 0 ) == that;
        }
        return false;
    }

    @Override
    public String value()
    {
        return value;
    }

    @Override
    int valueHash()
    {
        return value.hashCode();
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringProperty )
        {
            StringProperty that = (StringProperty) other;
            return value.equals( that.value );
        }
        if ( other instanceof CharProperty )
        {
            CharProperty that = (CharProperty) other;
            return value.length() == 1 && that.value == value.charAt( 0 );
        }
        return false;
    }

    @Override
    public String stringValue()
    {
        return value;
}
	 */
	
	private boolean asIs = true;
	private String value = null;
	private long valueCacheId = -1;

    StringProperty( int propertyKeyId, String value )
    {
        super( propertyKeyId );
        assert value != null;
        
        try
        {
        		int id = PagedCache.PagedCacheClientID.get();
        		asIs = false;
        		valueCacheId = PagedCache.transStateCache.putString(value);
        } catch (Exception e)
		{
        	
			// not assigned yet
        		asIs = true;
        		this.value = value;
		}
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return asIs ? valueEquals( value, other ):
        				  valueEquals( PagedCache.transStateCache.getString(valueCacheId), other ) ;
    }

    static boolean valueEquals( String value, Object other )
    {
        if ( other instanceof String )
        {
            return value.equals( other );
        }
        if ( other instanceof Character )
        {
            Character that = (Character) other;
            return value.length() == 1 && value.charAt( 0 ) == that;
        }
        return false;
    }

    @Override
    public String value()
    {
        return asIs ? value : PagedCache.transStateCache.getString(valueCacheId);
    }

    @Override
    int valueHash()
    {
        return asIs ? value.hashCode() : PagedCache.transStateCache.getString(valueCacheId).hashCode();
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringProperty )
        {
            StringProperty that = (StringProperty) other;
            return asIs ? value.equals( that.value ) : PagedCache.transStateCache.getString(valueCacheId).equals(that.value);
        }
        if ( other instanceof CharProperty )
        {
            CharProperty that = (CharProperty) other;
            String value1 = asIs ? null : PagedCache.transStateCache.getString(valueCacheId);
            return asIs ? value.length() == 1 && that.value == value.charAt( 0 ) :
            			value1.length() == 1 && that.value == value1.charAt( 0 );
        }
        return false;
    }

    @Override
    public String stringValue()
    {
        return asIs ? value : PagedCache.transStateCache.getString(valueCacheId);
}
	/*
    private long valueCacheId = -1;//private final String value;

    public StringProperty( int propertyKeyId, String value )
    {
        super( propertyKeyId );
        assert value != null;
        //this.value = value;
        try
        {
        		valueCacheId = DiskBasedCache.transStateCache.putString(value);
        } catch (Exception e)
		{
			System.out.println("Error in putString:"+e.getMessage());
		}
    }

    @Override
    public boolean valueEquals( Object other )
    {
        return valueEquals( DiskBasedCache.transStateCache.getString(valueCacheId), other );
    }

    static boolean valueEquals( String value, Object other )
    {
        if ( other instanceof String )
        {
            return value.equals( other );
        }
        if ( other instanceof Character )
        {
            Character that = (Character) other;
            return value.length() == 1 && value.charAt( 0 ) == that;
        }
        return false;
    }

    @Override
    public String value()
    {
        return DiskBasedCache.transStateCache.getString(valueCacheId);
    }

    @Override
    int valueHash()
    {
        return DiskBasedCache.transStateCache.getString(valueCacheId).hashCode();
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringProperty )
        {
            StringProperty that = (StringProperty) other;
            return DiskBasedCache.transStateCache.getString(valueCacheId).equals( that.value() );
        }
        if ( other instanceof CharProperty )
        {
            CharProperty that = (CharProperty) other;
            return DiskBasedCache.transStateCache.getString(valueCacheId).length() == 1 && that.value == DiskBasedCache.transStateCache.getString(valueCacheId).charAt( 0 );
        }
        return false;
    }

    @Override
    public String stringValue()
    {
        return DiskBasedCache.transStateCache.getString(valueCacheId);
    }
  */
/*
{
    private final long valueId;//private final String value;
    public static long spaceSaving = 0;
    public static int inK = 100000;

    public StringProperty( int propertyKeyId, String value )
    {
        super( propertyKeyId );
        assert value != null;
        //this.value = value;
        valueId = putDefinedProperty(value);
        spaceSaving += (value.length() - 8);
        if (spaceSaving - inK > 0)
        {
        		inK += 100000;
        		System.out.println("SpaceSavings:[" +spaceSaving + "] DeepSize ["+ VersionedHashMap.dpSize+"]");
        }
    }

    @Override
    public boolean valueEquals( Object other )
    {
    		String value = getDefinedProperty(valueId);
        return valueEquals( value, other );
    }

    static boolean valueEquals( String value, Object other )
    {
        if ( other instanceof String )
        {
            return value.equals( other );
        }
        if ( other instanceof Character )
        {
            Character that = (Character) other;
            return value.length() == 1 && value.charAt( 0 ) == that;
        }
        return false;
    }

    @Override
    public String value()
    {
    		String value = getDefinedProperty(valueId);
        return value;
    }

    @Override
    int valueHash()
    {
    		String value = getDefinedProperty(valueId);
        return value.hashCode();
    }

    @Override
    boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof StringProperty )
        {
            StringProperty that = (StringProperty) other;
            String value = getDefinedProperty(valueId);
            return value.equals( that.value() );
        }
        if ( other instanceof CharProperty )
        {
            CharProperty that = (CharProperty) other;
            String value = getDefinedProperty(valueId);
            return value.length() == 1 && that.value == value.charAt( 0 );
        }
        return false;
    }

    @Override
    public String stringValue()
    {
    		String value = getDefinedProperty(valueId);
        return value;
    }
    
    public long putDefinedProperty( String value )
    {
    		long id = 0;
    		//if (record instanceof StringProperty)
    		//{
    			//StringProperty prop = (StringProperty)record;
    			byte[] valueBytes = PropertyStore.encodeString(value);
    			int valueLength = valueBytes.length;
    			id = TransactionCacheState.transStateCache.idGen.nextId(valueLength);
    			long pageId = TransactionCacheState.pageIdForRecord( id >> 32 );
    	        int offset = TransactionCacheState.offsetForId( id >> 32 );
    	        try ( PageCursor cursor = TransactionCacheState.transStateCache.file.io( pageId, PF_SHARED_WRITE_LOCK ) )
    	        {
    	        	if ( cursor.next( pageId ) )
    	            {
    	                // There is a page in the store that covers this record, go write it
    	                do
    	                {
    	                    cursor.setOffset( offset );
    	                    //cursor.putInt(prop.propertyKeyId());
    	                    cursor.putBytes(valueBytes);
    	                }
    	                while ( cursor.shouldRetry() );
    	            }
    	        }
    	        catch ( IOException e )
    	        {
    	            throw new UnderlyingStorageException( e );
    	        }
    		//}
    		return id;
    }
    
    public void freeId(long id)
    {
    	TransactionCacheState.transStateCache.idGen.freeId(id);
    }
    public String getDefinedProperty(long id)
    {
    		StringProperty prop = null;
    		byte[] value = null;
    		int id1 = (int)(id >> 32); 
    		int length = (int)id;
    		long pageId = TransactionCacheState.pageIdForRecord( id1 );
        int offset = TransactionCacheState.offsetForId( id1 );
        try ( PageCursor cursor = TransactionCacheState.transStateCache.file.io( pageId, PF_SHARED_READ_LOCK ) )
        {
        	if ( cursor.next( pageId ) )
            {
                // There is a page in the store that covers this record, go write it
                do
                {
                    cursor.setOffset( offset );
                    //int propKeyId = cursor.getInt(offset);
                    value = new byte[length];
                    cursor.getBytes(value);
                    //prop = new StringProperty(propKeyId, value.toString());
                }
                while ( cursor.shouldRetry() );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        return PropertyStore.decodeString(value);
    }
*/
}
