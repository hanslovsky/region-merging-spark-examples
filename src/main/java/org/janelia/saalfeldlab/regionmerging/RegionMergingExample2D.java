package org.janelia.saalfeldlab.regionmerging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator.AffinityHistogram;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger.MEDIAN_AFFINITY_MERGER;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight.MedianAffinityWeight;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Data;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Options;
import org.janelia.saalfeldlab.util.unionfind.HashMapStoreUnionFind;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.javafx.application.PlatformImpl;

import bdv.bigcat.viewer.atlas.Atlas;
import bdv.bigcat.viewer.atlas.data.RandomAccessibleIntervalSpec;
import bdv.img.cache.VolatileGlobalCellCache;
import bdv.img.h5.H5Utils;
import bdv.util.Prefs;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TIntHashSet;
import javafx.application.Platform;
import javafx.stage.Stage;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.TypeIdentity;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.type.volatiles.VolatileARGBType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class RegionMergingExample2D
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args )
	{
		final Optional< String > rawFile = Optional.empty();
		final Optional< String > rawPath = Optional.empty();
		final String affinitiesFile = HOME_DIR + "/Downloads/excerpt.h5";
		final String affinitiesPath = "main";
		final String superVoxelFile = affinitiesFile;
		final String superVoxelPath = "zws";
		final int[] affinitiesChunkSize = { 300, 300, 100, 3 };
		final int[] superVoxelChunkSize = { 300, 300, 100 };

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/raw-and-affinities.h5";
//		final String affinitiesPath = "volumes/predicted_affs";
//		final String superVoxelFile = HOME_DIR + "/local/tmp/data-jan/labels.h5";
//		final String superVoxelPath = "volumes/labels/neuron_ids";
//		final int[] affinitiesChunkSize = { 1461, 1578, 63, 3 };
//		final int[] superVoxelChunkSize = { 84, 90, 2 };

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/cutout.h5";
//		final String affinitiesPath = "aff";
//		final String superVoxelFile = affinitiesFile;
//		final String superVoxelPath = "seg";
//		final int[] affinitiesChunkSize = { 500, 500, 63, 3 };
//		final int[] superVoxelChunkSize = { 500, 500, 63 };

		final long slice = 9;

		final CellImg< FloatType, ? > input = H5Utils.loadFloat( affinitiesFile, affinitiesPath, affinitiesChunkSize );
		System.out.println( "Loaded affinities from " + affinitiesFile + "/" + affinitiesFile );
		final long[] affsDim = Intervals.dimensionsAsLongArray( input );
		affsDim[ 2 ] = 1;
		final RandomAccessibleInterval< FloatType > affs = ArrayImgs.floats( affsDim );

		final int[] perm = getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		final long offset[] = new long[ 4 ];
		offset[ 2 ] = slice;
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.offsetInterval( inputPerm, offset, affsDim ), affs ), affs ) )
			p.getB().set( p.getA() );

		for ( int d = 0; d < 3; ++d )
			for ( final RealComposite< FloatType > aff : Views.hyperSlice( Views.collapseReal( affs ), d, 0l ) )
				aff.get( d ).set( Float.NaN );

//		for ( final RealComposite< FloatType > col : Views.flatIterable( Views.collapseReal( affs ) ) )
//			col.get( 2 ).setReal( Float.NaN );

		final RandomAccessibleInterval< LongType > labels = Views.offsetInterval( H5Utils.loadUnsignedLong( superVoxelFile, superVoxelPath, superVoxelChunkSize ), new long[] { 0, 0, slice }, new long[] { affsDim[ 0 ], affsDim[ 1 ], 1 } );
		System.out.println( "Loaded labels from " + superVoxelFile + "/" + superVoxelPath );


		final Random rng = new Random( 100 );
		final TLongIntHashMap colors = new TLongIntHashMap();
		for ( final LongType l : Views.flatIterable( labels ) )
			if ( !colors.contains( l.get() ) )
			{
				final int r = rng.nextInt( 255 );// + 255 - 100;
				final int g = rng.nextInt( 255 );// + 255 - 100;
				final int b = rng.nextInt( 255 );// + 255 - 100;
				colors.put( l.get(), 0xff << 24 | r << 16 | g << 8 | b << 0 );
			}
		colors.put( 0, 0 );

		final RandomAccessibleInterval< ARGBType > rgbAffs = Converters.convert( Views.collapseReal( affs ), ( s, t ) -> {
			t.set( ARGBType.rgba( 255 * ( 1 - s.get( 0 ).get() ), 255 * ( 1 - s.get( 1 ).get() ), 255, 255.0 ) );
		}, new ARGBType() );
		final double factor = 1.0;// 0.52;

		final double[][] resolution = { { 1.0, 1.0, 10.0 } };

		Prefs.showMultibox( false );
		Prefs.showTextOverlay( false );

		final Converter< LongType, ARGBType > colorConv = ( s, t ) -> {
			t.set( colors.get( s.get() ) );
		};

		final Converter< ARGBType, VolatileARGBType > convertVolatile = ( s, t ) -> t.get().set( s );
		final Function< RandomAccessibleInterval< ARGBType >, RandomAccessibleInterval< VolatileARGBType > > makeVolatile = s -> Converters.convert( s, convertVolatile, new VolatileARGBType() );

		PlatformImpl.startup( () -> {} );
		final Atlas atlas = new Atlas( labels, new VolatileGlobalCellCache( 1, 12 ) );
		Platform.runLater( () -> {
			final Stage stage = new Stage();
			try
			{
				atlas.start( stage );
			}
			catch ( final InterruptedException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			stage.show();
			final RandomAccessibleIntervalSpec< ARGBType, VolatileARGBType > spec = new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { rgbAffs }, new RandomAccessibleInterval[] { makeVolatile.apply( rgbAffs ) }, resolution, null, "affs" );
			atlas.addARGBSource( spec );

			final RandomAccessibleInterval< ARGBType > colored = Converters.convert( labels, colorConv, new ARGBType() );
			final RandomAccessibleIntervalSpec< ?, VolatileARGBType > voxelSpec = new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { labels }, new RandomAccessibleInterval[] { makeVolatile.apply( colored ) }, resolution, null, "super voxels " );
			atlas.addARGBSource( voxelSpec );
		} );

		final int nBins = 256;
		final AffinityHistogram creator = new EdgeCreator.AffinityHistogram( nBins, 0.0, 1.0 );
		final MEDIAN_AFFINITY_MERGER merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
		final MedianAffinityWeight edgeWeight = new EdgeWeight.MedianAffinityWeight( nBins, 0.0, 1.0 );

//		final NoDataSerializableCreator creator = new EdgeCreator.NoDataSerializableCreator();
//		final MIN_AFFINITY_MERGER merger = new EdgeMerger.MIN_AFFINITY_MERGER();
//		final EdgeWeight edgeWeight = new EdgeWeight.OneMinusAffinity();

		final long[] dimensions = Intervals.dimensionsAsLongArray( labels );
		System.out.println( "DIMENSIONS " + Arrays.toString( dimensions ) );
		final int[] blockSize = { ( int ) dimensions[ 0 ], ( int ) dimensions[ 1 ], 1 };

		final CellLoader< LongType > ll = cell -> {
			burnIn( labels, cell );
		};

		final CellLoader< FloatType > al = cell -> {
			burnIn( affs, cell );
		};

		final SparkConf conf = new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( DataPreparation.class.toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() );

		final JavaSparkContext sc = new JavaSparkContext( conf );

		final JavaPairRDD< HashWrapper< long[] >, Data > graph = DataPreparation.createGraphPointingBackwards( sc, new RegionMergingExample.FloatAndLongLoader( dimensions, blockSize, ll, al ), creator, merger, blockSize );
		graph.cache();
		final long nBlocks = graph.count();
		System.out.println( "Starting with " + nBlocks + " blocks." );

		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, 2 );

		final Options options = new BlockedRegionMergingSpark.Options( 0.5, StorageLevel.MEMORY_ONLY() );

		final TIntObjectHashMap< TLongArrayList > mergesLog = new TIntObjectHashMap<>();

		final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger = ( i, rdd ) -> {
			final TLongArrayList merges = new TLongArrayList();
			rdd.values().collect().stream().map( Tuple2::_1 ).forEach( merges::addAll );
			final int newIndex = mergesLog.size();
			mergesLog.put( newIndex, merges );
		};

		System.out.println( "Start agglomerating!" );
		rm.agglomerate( sc, graph, mergesLogger, options );
		System.out.println( "Done agglomerating!" );

		final TIntObjectHashMap< TLongArrayList > merges = mergesLog;

		final HashMapStoreUnionFind[] ufs = Stream.generate( HashMapStoreUnionFind::new ).limit( merges.size() ).toArray( HashMapStoreUnionFind[]::new );

		for ( int iteration = 0; iteration < ufs.length; ++iteration )
		{

			final TLongArrayList list = merges.get( iteration );

			System.out.println( "Got " + list.size() / 4 + " merges!" );

			for ( int ufIndex = iteration; ufIndex < ufs.length; ++ufIndex )
			{

				final HashMapStoreUnionFind uf = ufs[ ufIndex ];
				for ( int i = 0; i < list.size(); i += 4 )
				{
					final long r1 = uf.findRoot( list.get( i + 0 ) );
					final long r2 = uf.findRoot( list.get( i + 1 ) );
//				System.out.println( list.get( i ) + " " + list.get( i + 1 ) + " " + r1 + " " + r2 );
					if ( r1 != r2 )
						uf.join( r1, r2 );
				}
			}
		}

//		final TLongArrayList list = mergesLog.get( 0 );
//		System.out.println( "Got " + lst.size() / 2 + " merges!" );
//		for ( int i = 0; i < list.size(); i += 2 )
//		{
//			final long r1 = uf.findRoot( list.get( i + 0 ) );
//			final long r2 = uf.findRoot( list.get( i + 1 ) );
////			System.out.println( list.get( i ) + " " + list.get( i + 1 ) + " " + r1 + " " + r2 );
//			if ( r1 != r2 )
//				uf.join( r1, r2 );
//		}

		final ArrayList< RandomAccessibleIntervalSpec< LongType, VolatileARGBType > > raiSpecs = new ArrayList<>();

		for ( int i = 0; i < ufs.length; ++i )
		{
			final HashMapStoreUnionFind uf = ufs[ i ];
			final RandomAccessibleInterval< LongType > firstJoined = Converters.convert( labels, ( s, t ) -> {
				t.set( uf.findRoot( s.get() ) );
			}, new LongType() );
			final RandomAccessibleInterval< ARGBType > colored = Converters.convert( firstJoined, colorConv, new ARGBType() );
			raiSpecs.add( new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { firstJoined }, new RandomAccessibleInterval[] { makeVolatile.apply( colored ) }, resolution, null, "iteration " + i ) );
		}

		Platform.runLater( () -> {
			raiSpecs.forEach( atlas::addARGBSource );
		} );



	}

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}

	public static < T extends Type< T > > void burnIn( final RandomAccessible< T > source, final RandomAccessibleInterval< T > target )
	{
		for ( final Pair< T, T > p : Views.flatIterable( Views.interval( Views.pair( source, target ), target ) ) )
			p.getB().set( p.getA() );
	}

	public static class Registrator implements KryoRegistrator
	{

		@Override
		public void registerClasses( final Kryo kryo )
		{
//			kryo.register( HashMap.class );
//			kryo.register( TIntHashSet.class, new TIntHashSetSerializer() );
			kryo.register( Data.class, new DataSerializer() );
//			kryo.register( TDoubleArrayList.class, new TDoubleArrayListSerializer() );
//			kryo.register( TLongLongHashMap.class, new TLongLongHashMapListSerializer() );
//			kryo.register( HashWrapper.class, new HashWrapperSerializer<>() );
		}

	}

	public static class DataSerializer extends Serializer< Data >
	{

		@Override
		public void write( final Kryo kryo, final Output output, final Data object )
		{
			// edges
			output.writeInt( object.edges().size() );
			output.writeDoubles( object.edges().toArray() );

			// non-contracting edges
			output.writeInt( object.nonContractingEdges().size() );
			for ( final Entry< HashWrapper< long[] >, TIntHashSet > entry : object.nonContractingEdges().entrySet() )
			{
//				System.out.println( "Writing entry: " + entry );
				output.writeInt( entry.getKey().getData().length );
				output.writeLongs( entry.getKey().getData() );
				output.writeInt( entry.getValue().size() );
				output.writeInts( entry.getValue().toArray() );
			}

		}

		@Override
		public Data read( final Kryo kryo, final Input input, final Class< Data > type )
		{
			// edges
			final int numEdges = input.readInt();
			final TDoubleArrayList edgeStore = new TDoubleArrayList( input.readDoubles( numEdges ) );

			// non-contracting edges
			final int size = input.readInt();
			final HashMap< HashWrapper< long[] >, TIntHashSet > nonContractingEdges = new HashMap<>();
			for ( int i = 0; i < size; ++i )
			{
//				System.out.println( "reading key" );
				final int nDim = input.readInt();
				final HashWrapper< long[] > key = HashWrapper.longArray( input.readLongs( nDim ) );
//				System.out.println( "reading value" );
				final int setSize = input.readInt();
				final TIntHashSet value = new TIntHashSet( input.readInts( setSize ) );
//				System.out.println( "ok" );
				nonContractingEdges.put( key, value );
			}

			return new Data(
					edgeStore,
					nonContractingEdges );
		}
	}



}
