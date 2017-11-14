package org.janelia.saalfeldlab.regionmerging;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.janelia.saalfeldlab.graph.edge.Edge;
import org.janelia.saalfeldlab.util.unionfind.HashMapStoreUnionFind;

import com.google.common.io.Files;

import gnu.trove.list.array.TLongArrayList;

public class UnionFindFromArray
{

	private final long[] merges;

	public UnionFindFromArray( final long[] merges )
	{
		super();
		this.merges = merges;
	}

	public HashMapStoreUnionFind getAtLevel( final double threshold )
	{
		final HashMapStoreUnionFind uf = new HashMapStoreUnionFind();
		for ( int i = 0; i < merges.length; i += 3 )
		{
			final double weight = Edge.ltd( merges[ i ] );
			if ( weight < threshold )
			{
//				System.out.println( "THRESHOLDING FOR " + weight + " " + merges[ i + 1 ] + " " + merges[ i + 2 ] );
				final long from = uf.findRoot( merges[ i + 1 ] );
				final long to = uf.findRoot( merges[ i + 2 ] );
				if ( from != to )
					uf.join( from, to );
			}
		}
		return uf;
	}

	public static UnionFindFromArray fromFiles( final String... fileNames ) throws IOException {
		return fromFiles( Arrays.asList( fileNames ) );
	}

	public static UnionFindFromArray fromFiles( final Collection< String > fileNames ) throws IOException
	{
		final TLongArrayList merges = new TLongArrayList();
		for ( final String fileName : fileNames ) {
			final List< String > lines = Files.readLines( new File( fileName ), Charset.defaultCharset() );
			for ( int i = 1; i < lines.size(); ++i )
			{
				final String line = lines.get( i );
				final String[] split = line.split( "," );
				merges.add( Edge.dtl( Double.parseDouble( split[ 0 ] ) ) );
				merges.add( Long.parseLong( split[ 1 ] ) );
				merges.add( Long.parseLong( split[ 2 ] ) );
			}
		};
		return new UnionFindFromArray( merges.toArray() );
	}

}
