package com.neo4j.bench.client.util;

import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.neo4j.bench.client.ClientUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Resources implements AutoCloseable
{
    public static String fileToString( String filename )
    {
        try ( InputStream inputStream = ClientUtil.class.getResource( filename ).openStream() )
        {
            return CharStreams.toString( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error reading stream to string", e );
        }
    }

    public static Map<String,String> fileToMap( String filename ) throws IOException
    {
        try ( InputStream inputStream = ClientUtil.class.getResource( filename ).openStream() )
        {
            Properties properties = new Properties();
            properties.load( inputStream );
            return new HashMap<>( Maps.fromProperties( properties ) );
        }
    }

    public static Path toPath( String name )
    {
        try
        {
            return Paths.get( Resources.class.getResource( name ).toURI() );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    private FileSystem jarFileSystem;

    public Path resourceFile( String filename )
    {
        URI resourceUri = resourceUriFor( filename );
        if ( resourceUri.toString().contains( ".jar!" ) )
        {
            initJarFileSystem( resourceUri );
        }
        return Paths.get( resourceUri );
    }

    private void initJarFileSystem( URI resourceUri )
    {
        if ( null == jarFileSystem )
        {
            jarFileSystem = initFileSystem( resourceUri );
        }
    }

    private static URI resourceUriFor( String filename )
    {
        try
        {
            return Resources.class.getResource( filename ).toURI();
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( "Error retrieve URI for resource with name: " + filename );
        }
    }

    private static FileSystem initFileSystem( URI uri )
    {
        try
        {
            return FileSystems.newFileSystem( uri, Collections.emptyMap() );
        }
        catch ( IllegalArgumentException e )
        {
            return FileSystems.getDefault();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to initialize file system for URI: " + uri );
        }
    }

    @Override
    public void close()
    {
        try
        {
            if ( null != jarFileSystem )
            {
                jarFileSystem.close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error closing JAR file system", e );
        }
    }
}
