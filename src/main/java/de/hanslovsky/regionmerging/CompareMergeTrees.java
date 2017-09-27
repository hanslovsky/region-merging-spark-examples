package de.hanslovsky.regionmerging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import de.hanslovsky.util.unionfind.HashMapStoreUnionFind;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class CompareMergeTrees
{

	public static void main( final String[] args ) throws IOException
	{
		final String fn1 = "log-small-3";
		final String fn2 = "log-small-6";

		final List< Tuple2< Long, Long > > merges1 = merges( fn1 );
		final List< Tuple2< Long, Long > > merges2 = merges( fn2 );

		final TLongLongHashMap roots1 = new TLongLongHashMap();
		final TLongLongHashMap roots2 = new TLongLongHashMap();

		final HashMapStoreUnionFind uf1 = new HashMapStoreUnionFind( roots1, new TLongLongHashMap(), 0 );
		final HashMapStoreUnionFind uf2 = new HashMapStoreUnionFind( roots2, new TLongLongHashMap(), 0 );

		merges1.forEach( m -> uf1.join( uf1.findRoot( m._1() ), uf1.findRoot( m._2() ) ) );
		merges2.forEach( m -> uf2.join( uf2.findRoot( m._1() ), uf2.findRoot( m._2() ) ) );

		final TLongLongHashMap min1 = relabelSetsByMinimumMember( roots1, uf1 );
		final TLongLongHashMap min2 = relabelSetsByMinimumMember( roots2, uf2 );

		System.out.println( "min1.size(): " + min1.size() );
		System.out.println( "min2.size(): " + min2.size() );

		min1.forEachEntry( ( key, value ) -> {
			if ( !min2.contains( key ) )
				System.out.println( "KEY NOT CONTAINED IN MIN2! " + key + " " + value );
			else if ( value != min2.get( key ) )
				System.out.println( "VALUES DISAGREE IN MIN2!     " + key + " " + value + " " + min2.get( key ) );
			return true;
		} );

		min2.forEachEntry( ( key, value ) -> {
			if ( !min1.contains( key ) )
				System.out.println( "KEY NOT CONTAINED IN MIN1! " + key + " " + value );
			else if ( value != min1.get( key ) )
				System.out.println( "VALUES DISAGREE IN MIN1!     " + key + " " + value + " " + min1.get( key ) );
			return true;
		} );

		System.out.println( min1.get( 290 ) );

	}

	public static List< Tuple2< Long, Long > > merges( final String path ) throws IOException
	{
		return Files.lines( Paths.get( path ) ).map( str -> str.split( "," ) ).map( arr -> new Tuple2<>( Long.parseLong( arr[ 1 ] ), Long.parseLong( arr[ 2 ] ) ) ).collect( Collectors.toList() );
	}

	public static TLongLongHashMap relabelSetsByMinimumMember( final TLongLongHashMap input, final HashMapStoreUnionFind uf )
	{
		final TLongLongHashMap output = new TLongLongHashMap();

		final TLongLongHashMap minimumMembers = new TLongLongHashMap();

		input.forEachEntry( ( key, value ) -> {
			final long r = uf.findRoot( key );
			assert r == value;
			if ( !minimumMembers.contains( r ) )
				minimumMembers.put( r, key );
			else
				minimumMembers.put( r, Math.min( key, minimumMembers.get( r ) ) );
			return true;
		} );

		input.forEachEntry( ( key, value ) -> {
			output.put( key, minimumMembers.get( value ) );
			return true;
		} );

		return output;
	}

}
