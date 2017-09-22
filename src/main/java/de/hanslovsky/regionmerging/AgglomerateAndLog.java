package de.hanslovsky.regionmerging;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.util.AccumulatorV2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.hanslovsky.graph.edge.EdgeCreator;
import de.hanslovsky.graph.edge.EdgeMerger;
import de.hanslovsky.graph.edge.EdgeWeight;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Data;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Options;
import de.hanslovsky.regionmerging.DataPreparation.Loader;
import de.hanslovsky.regionmerging.loader.hdf5.AffinitiesAndLabelsFromH5;
import de.hanslovsky.regionmerging.loader.hdf5.LoaderFromLoaders;
import de.hanslovsky.util.unionfind.HashMapStoreUnionFind;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TIntHashSet;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import scala.Tuple2;

public class AgglomerateAndLog
{

	public static String HOME_DIR = System.getProperty( "user.home" );

//	public static void main( final String[] args )
//	{
//
//		final String affinitiesFile = args[ 0 ];
//		final String affinitiesPath = args[ 1 ];
//		final String superVoxelFile = args[ 2 ];
//		final String superVoxelPath = args[ 3 ];
//	}

	public static void runFromH5(
			final JavaSparkContext sc,
			final String affinitiesFile,
			final String affinitiesPath,
			final String superVoxelFile,
			final String superVoxelPath,
			final EdgeCreator creator,
			final EdgeMerger merger,
			final EdgeWeight edgeWeight,
			final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger,
			final Function< CellGrid, int[] > blockSize,
			final BlockedRegionMergingSpark.Options options )
	{
		final LoaderFromLoaders< LongType, FloatType, LongArray, FloatArray > loader = AffinitiesAndLabelsFromH5.get( affinitiesFile, affinitiesPath, superVoxelFile, superVoxelPath, true );
		final JavaPairRDD< HashWrapper< long[] >, Data > blockedGraph = DataPreparation.createGraphPointingBackwards( sc, loader, creator, merger, blockSize.apply( loader.labelGrid() ) ); // er
		blockedGraph.persist( options.persistenceLevel() );
		run( sc, blockedGraph, loader, creator, merger, edgeWeight, options, mergesLogger );
		blockedGraph.unpersist();
	}

	public static void runFromH5(
			final JavaSparkContext sc,
			final String affinitiesFile,
			final String affinitiesPath,
			final String superVoxelFile,
			final String superVoxelPath,
			final EdgeCreator creator,
			final EdgeMerger merger,
			final EdgeWeight edgeWeight,
			final Function< CellGrid, int[] > blockSize,
			final BlockedRegionMergingSpark.Options options,
			final double[] thresholds,
			final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > >[] mergesLogger )
	{
		final LoaderFromLoaders< LongType, FloatType, LongArray, FloatArray > loader = AffinitiesAndLabelsFromH5.get( affinitiesFile, affinitiesPath, superVoxelFile, superVoxelPath, true );
		final JavaPairRDD< HashWrapper< long[] >, Data > blockedGraph = DataPreparation.createGraphPointingBackwards( sc, loader, creator, merger, blockSize.apply( loader.labelGrid() ) ); // er
		blockedGraph.persist( options.persistenceLevel() );
		run( sc, blockedGraph, loader, creator, merger, edgeWeight, options, thresholds, mergesLogger );
		blockedGraph.unpersist();
	}

	public static < LABEL extends IntegerType< LABEL > & NativeType< LABEL >, AFF extends RealType< AFF > & NativeType< AFF >, LABEL_A extends ArrayDataAccess< LABEL_A >, AFF_A extends ArrayDataAccess< AFF_A > > void run(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, Data > blockedGraph,
			final Loader< LABEL, AFF, LABEL_A, AFF_A > loader,
			final EdgeCreator creator,
			final EdgeMerger merger,
			final EdgeWeight edgeWeight,
			final BlockedRegionMergingSpark.Options options,
			final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger )
	{
		final double[] thresholds = new double[] { options.threshold() };
		final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > >[] loggers = new BiConsumer[] { mergesLogger };
		run( sc, blockedGraph, loader, creator, merger, edgeWeight, options, thresholds, loggers );
	}

	public static < LABEL extends IntegerType< LABEL > & NativeType< LABEL >, AFF extends RealType< AFF > & NativeType< AFF >, LABEL_A extends ArrayDataAccess< LABEL_A >, AFF_A extends ArrayDataAccess< AFF_A > > void run(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, Data > blockedGraph,
			final Loader< LABEL, AFF, LABEL_A, AFF_A > loader,
			final EdgeCreator creator,
			final EdgeMerger merger,
			final EdgeWeight edgeWeight,
			final BlockedRegionMergingSpark.Options options,
			final double[] thresholds,
			final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > >[] mergesLogger )
	{
		final MergesAccumulator accu = new MergesAccumulator();
		sc.sc().register( accu, "mergesAccumulator" );

		final IntFunction< MergeNotifyWithFinishNotification > mergeNotifyGenerator = new MergeNotifyGenerator( accu );

		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, mergeNotifyGenerator, 2 );

		System.out.println( "Start agglomerating!" );
		for ( int i = 0; i < thresholds.length; ++i )
		{
			final Options opts = options.copy().threshold( thresholds[ i ] );
			rm.agglomerate( sc, blockedGraph, mergesLogger[ i ], opts );
		}
		System.out.println( "Done agglomerating!" );
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

	public static class ApplyMergesAndWriteImages< I extends IntegerType< I > & NativeType< I > > implements BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > >
	{

		private JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< I > > currentLabeling;

		private final BiFunction< long[], Integer, long[][] > mapToOriginalIndices;

		private final BiFunction< long[], Integer, Consumer< RandomAccessibleInterval< I > > > saver;

		public ApplyMergesAndWriteImages(
				final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< I > > currentLabeling,
				final BiFunction< long[], Integer, long[][] > mapToOriginalIndices,
				final BiFunction< long[], Integer, Consumer< RandomAccessibleInterval< I > > > saver )
		{
			super();
			this.currentLabeling = currentLabeling;
			this.mapToOriginalIndices = mapToOriginalIndices;
			this.saver = saver;
		}

		@Override
		public void accept( final Integer iteration, final JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > merges )
		{
			final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< I > > labelingTmp = currentLabeling
					.join( merges.flatMapToPair( new FlatMapIndicesToOriginal( iteration, mapToOriginalIndices ) ) )
					.mapValues( imageAndMerges -> {
						final RandomAccessibleInterval< I > image = imageAndMerges._1();
						final ArrayImgFactory< I > af = new ArrayImgFactory<>();
						final I type = net.imglib2.util.Util.getTypeFromInterval( image ).createVariable();
						final ArrayImg< I, ? > target = af.create( Intervals.dimensionsAsLongArray( image ), type );
						target.setLinkedType( type );

						final HashMapStoreUnionFind unionFind = imageAndMerges._2()._2();
						for ( Cursor< I > s = Views.flatIterable( image ).cursor(), t = target.cursor(); t.hasNext(); )
							t.next().setInteger(unionFind.findRoot( s.next().getIntegerLong() ) );
						return target;
					} );
			labelingTmp.cache();

			labelingTmp.map( t -> {
				final Consumer< RandomAccessibleInterval< I > > localSaver = saver.apply( t._1().getData(), iteration );
				localSaver.accept( t._2() );
				return true;
			} ).collect();

			currentLabeling.unpersist();

			currentLabeling = labelingTmp;


		}

		public static class FlatMapIndicesToOriginal implements PairFlatMapFunction< Tuple2< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > >, HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > >
		{

			private final int iteration;

			private final BiFunction< long[], Integer, long[][] > mapToOriginalIndices;

			public FlatMapIndicesToOriginal( final int iteration, final BiFunction< long[], Integer, long[][] > mapToOriginalIndices )
			{
				super();
				this.iteration = iteration;
				this.mapToOriginalIndices = mapToOriginalIndices;
			}

			@Override
			public Iterator< Tuple2< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > call( final Tuple2< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > mergeList ) throws Exception
			{
				return Arrays.stream( mapToOriginalIndices.apply( mergeList._1().getData(), iteration ) ).map( HashWrapper::longArray ).map( hash -> new Tuple2<>( hash, mergeList._2() ) ).iterator();
			}

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

}
