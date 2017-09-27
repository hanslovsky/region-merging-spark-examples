package de.hanslovsky.regionmerging;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.hanslovsky.graph.edge.Edge;
import de.hanslovsky.graph.edge.EdgeCreator;
import de.hanslovsky.graph.edge.EdgeCreator.AffinityHistogram;
import de.hanslovsky.graph.edge.EdgeMerger;
import de.hanslovsky.graph.edge.EdgeMerger.MEDIAN_AFFINITY_MERGER;
import de.hanslovsky.graph.edge.EdgeWeight;
import de.hanslovsky.graph.edge.EdgeWeight.MedianAffinityWeight;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Data;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Options;
import de.hanslovsky.regionmerging.loader.hdf5.AffinitiesAndLabelsFromH5;
import de.hanslovsky.regionmerging.loader.hdf5.LoaderFromLoaders;
import de.hanslovsky.util.unionfind.HashMapStoreUnionFind;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TIntHashSet;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class RegionMergingExampleLogMerges
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args ) throws IOException
	{
		final String affinitiesFile = HOME_DIR + "/Downloads/excerpt.h5";
		final String affinitiesPath = "main";// "affs-0-6-90x60+150+0";
		final String superVoxelFile = affinitiesFile;
		final String superVoxelPath = "zws";// "zws-0-6-90x60+150+0";

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/raw-and-affinities.h5";
//		final String affinitiesPath = "volumes/predicted_affs";
//		final String superVoxelFile = HOME_DIR + "/local/tmp/data-jan/labels.h5";
//		final String superVoxelPath = "volumes/labels/neuron_ids";

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/cutout.h5";
//		final String affinitiesPath = "aff";
//		final String superVoxelFile = affinitiesFile;
//		final String superVoxelPath = "seg";

		final long startX = 0;
		final long startY = 150;
		final long startZ = 0;
		final long stopX = startX + 60;
		final long stopY = startY + 60;
		final long stopZ = startZ + 6;

		final long[] affinitiesMin = new long[] { startX, startY, startZ, 0 };
		final long[] affinitiesMax = new long[] { stopX - 1, stopY - 1, stopZ - 1, 2 };
		final long[] superVoxelMin = Arrays.stream( affinitiesMin ).limit( 3 ).toArray();
		final long[] superVoxelMax = Arrays.stream( affinitiesMax ).limit( 3 ).toArray();

		final int stepSizeZ = 6;

		final LoaderFromLoaders< LongType, FloatType, LongArray, FloatArray > loader = AffinitiesAndLabelsFromH5.get( affinitiesFile, affinitiesPath, superVoxelFile, superVoxelPath, true );

		final LoadedCellCacheLoader< FloatType, FloatArray > al = LoadedCellCacheLoader.get( loader.affinitiesGrid(), loader.affinitiesLoader(), new FloatType() );
		final LoadedCellCacheLoader< LongType, LongArray > ll = LoadedCellCacheLoader.get( loader.labelGrid(), loader.labelLoader(), new LongType() );
		final LazyCellImg< FloatType, ? > affinities = new LazyCellImg<>( loader.affinitiesGrid(), new FloatType(), index -> {
			try
			{
				return al.get( index );
			}
			catch ( final Exception e )
			{
				throw new RuntimeException( e );
			}
		} );
		final LazyCellImg< LongType, ? > superVoxel = new LazyCellImg<>( loader.labelGrid(), new LongType(), index -> {
			try
			{
				return ll.get( index );
			}
			catch ( final Exception e )
			{
				throw new RuntimeException( e );
			}
		} );

		final IntervalView< FloatType > affinitiesCrop = Views.offsetInterval( affinities, new FinalInterval( affinitiesMin, affinitiesMax ) );
		final IntervalView< LongType > superVoxelCrop = Views.offsetInterval( superVoxel, new FinalInterval( superVoxelMin, superVoxelMax ) );
		final CellGrid sg = new CellGrid( Intervals.dimensionsAsLongArray( superVoxelCrop ), Intervals.dimensionsAsIntArray( superVoxelCrop ) );
		final CellGrid ag = new CellGrid( Intervals.dimensionsAsLongArray( affinitiesCrop ), Intervals.dimensionsAsIntArray( affinitiesCrop ) );


		final LoaderFromLoaders< LongType, FloatType, LongArray, FloatArray > actualLoader = new LoaderFromLoaders<>(
				sg, ag, cell -> burnIn( superVoxelCrop, cell ), cell -> burnIn( affinitiesCrop, cell ), new LongType(), new FloatType(), new LongArray( 1 ), new FloatArray( 1 ) );

		final int[] blockSize = Arrays.stream( actualLoader.labelGrid().getImgDimensions() ).mapToInt( l -> ( int ) l ).toArray();
		blockSize[ 2 ] = stepSizeZ;

		final int nBins = 256;
		final AffinityHistogram creator = new EdgeCreator.AffinityHistogram( nBins, 0.0, 1.0 );
		final MEDIAN_AFFINITY_MERGER merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
		final MedianAffinityWeight edgeWeight = new EdgeWeight.MedianAffinityWeight( nBins, 0.0, 1.0 );

//		final NoDataSerializableCreator creator = new EdgeCreator.NoDataSerializableCreator();
//		final MIN_AFFINITY_MERGER merger = new EdgeMerger.MIN_AFFINITY_MERGER();
//		final EdgeWeight edgeWeight = new EdgeWeight.OneMinusAffinity();

		final SparkConf conf = new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( DataPreparation.class.toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() );

		final JavaSparkContext sc = new JavaSparkContext( conf );

		final JavaPairRDD< HashWrapper< long[] >, Data > graph = DataPreparation.createGraphPointingBackwards( sc, actualLoader, creator, merger, blockSize );
		graph.cache();
		final HashMap< Tuple2< Long, Long >, Double > edges = new HashMap< Tuple2< Long, Long >, Double >();
		graph.values().collect().forEach( data -> {
			final Edge e = new Edge( data.edges(), merger.dataSize() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long from = e.from();
				final long to = e.to();
				edges.put( new Tuple2<>( Math.min( from, to ), Math.max( from, to ) ), e.affinity() );
			}
		} );
		final StringBuilder sb = new StringBuilder( "from,to,weight" );
//		System.out.println( edges );
		for ( final Entry< Tuple2< Long, Long >, Double > edge : edges.entrySet() )
			sb.append( "\n" ).append( edge.getKey()._1() ).append( "," ).append( edge.getKey()._2() ).append( "," ).append( edge.getValue() );

		final String edgesLogFileName = "log-edges-small-" + stepSizeZ;
		Files.deleteIfExists( new File( edgesLogFileName ).toPath() );
		Files.createFile( Paths.get( edgesLogFileName ) );
		Files.write( Paths.get( edgesLogFileName ), sb.toString().getBytes(), StandardOpenOption.APPEND );

		final IntFunction< MergeNotifyWithFinishNotification > mergeNotifyGenerator = new LazyMergeNotify.Generator();

		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, mergeNotifyGenerator, 2 );

		final Options options = new BlockedRegionMergingSpark.Options( 0.5, StorageLevel.MEMORY_ONLY() );

		final String outputFileName = "log-small-" + stepSizeZ;
		Files.deleteIfExists( new File( outputFileName ).toPath() );
		Files.createFile( Paths.get( outputFileName ) );

		final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger = ( i, rdd ) -> {
			final List< String > strings = rdd.mapValues( new GetFirst<>() ).mapValues( new MergesToCSVString() ).values().collect();

			final Path path = Paths.get( outputFileName );
			strings.forEach( str -> {
				try
				{
					Files.write( path, str.getBytes(), StandardOpenOption.APPEND );
				}
				catch ( final IOException e1 )
				{
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} );

		};

		System.out.println( "Start agglomerating!" );
		rm.agglomerate( sc, graph, mergesLogger, options );
		System.out.println( "Done agglomerating!" );



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
			kryo.register( LazyMergeNotify.class );
			kryo.register( LazyMergeNotify.Generator.class );
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

			// counts
			output.writeInt( object.counts().size() );
			output.writeLongs( object.counts().keys() );
			output.writeLongs( object.counts().values() );
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

			// counts
			final int numNodes = input.readInt();
			final long[] keys = input.readLongs( numNodes );
			final long[] values = input.readLongs( numNodes );
			final TLongLongHashMap counts = new TLongLongHashMap( keys, values );
			return new Data(
					edgeStore,
					nonContractingEdges,
					counts );
		}
	}

	public static class MergeNotifyGenerator implements IntFunction< MergeNotifyWithFinishNotification >, Serializable
	{

		private final MergesAccumulator merges;

		public MergeNotifyGenerator( final MergesAccumulator merges )
		{
			super();
			this.merges = merges;
		}

		@Override
		public MergeNotifyWithFinishNotification apply( final int value )
		{
			final TLongArrayList mergesInBlock = new TLongArrayList();
			return new MergeNotifyWithFinishNotification()
			{

				@Override
				public void addMerge( final long node1, final long node2, final long newNode, final double weight )
				{
					mergesInBlock.add( node1 );
					mergesInBlock.add( node2 );
					mergesInBlock.add( newNode );
					mergesInBlock.add( Double.doubleToRawLongBits( weight ) );
//					System.out.println( "Added merge " + node1 + " " + node2 + " " + newNode + " " + weight );
				}

				@Override
				public void notifyDone()
				{
					synchronized ( merges )
					{
						final TIntObjectHashMap< TLongArrayList > m = new TIntObjectHashMap<>();
						m.put( value, mergesInBlock );
						merges.add( m );
					}
					System.out.println( "Added " + mergesInBlock.size() / 4 + " merges at iteration " + value );
				}
			};
		}

	}

	public static class MergesAccumulator extends AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > >
	{

		private final TIntObjectHashMap< TLongArrayList > data;

		public MergesAccumulator()
		{
			this( new TIntObjectHashMap<>() );
		}

		public MergesAccumulator( final TIntObjectHashMap< TLongArrayList > data )
		{
			super();
			this.data = data;
		}

		@Override
		public void add( final TIntObjectHashMap< TLongArrayList > data )
		{
			synchronized ( this.data )
			{
				for ( final TIntObjectIterator< TLongArrayList > it = data.iterator(); it.hasNext(); )
				{
					it.advance();
					if ( !this.data.contains( it.key() ) )
						this.data.put( it.key(), new TLongArrayList() );
					this.data.get( it.key() ).addAll( it.value() );
				}
			}
		}

		@Override
		public AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > > copy()
		{
			synchronized ( data )
			{
				final TIntObjectHashMap< TLongArrayList > copy = new TIntObjectHashMap<>( data );
				return new MergesAccumulator( copy );
			}
		}

		@Override
		public boolean isZero()
		{
			synchronized ( data )
			{
				return data.size() == 0;
			}
		}

		@Override
		public void merge( final AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > > other )
		{
			synchronized ( data )
			{
				add( other.value() );
			}
		}

		@Override
		public void reset()
		{
			synchronized ( data )
			{
				this.data.clear();
			}
		}

		@Override
		public TIntObjectHashMap< TLongArrayList > value()
		{
			synchronized ( data )
			{
				return data;
			}
		}

	}

	public static class GetFirst< T, U > implements Function< Tuple2< T, U >, T >
	{

		@Override
		public T call( final Tuple2< T, U > v1 ) throws Exception
		{
			return v1._1();
		}

	}

	public static class MergesToCSVString implements Function< TLongArrayList, String >
	{

		@Override
		public String call( final TLongArrayList merges ) throws Exception
		{
			final int step = 4;
			final StringBuilder sb = new StringBuilder();
			for ( int index = 0; index < merges.size(); index += step )
				sb
				.append( Double.longBitsToDouble( merges.get( index + 1 ) ) )
				.append( "," )
				.append( merges.get( index + 2 ) )
				.append( "," )
				.append( merges.get( index + 3 ) )
				.append( System.lineSeparator() );
			return sb.toString();
		}

	}

	public static class LazyMergeNotify implements MergeNotifyWithFinishNotification
	{

		public static class Generator implements IntFunction< MergeNotifyWithFinishNotification >, Serializable
		{

			@Override
			public MergeNotifyWithFinishNotification apply( final int value )
			{
				return new LazyMergeNotify();
			}

		}

		@Override
		public void addMerge( final long node1, final long node2, final long newNode, final double weight )
		{

		}

		@Override
		public void notifyDone()
		{

		}
	}

}
