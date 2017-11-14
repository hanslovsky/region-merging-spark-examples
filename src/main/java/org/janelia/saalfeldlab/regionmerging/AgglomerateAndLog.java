package org.janelia.saalfeldlab.regionmerging;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Data;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Options;
import org.janelia.saalfeldlab.regionmerging.DataPreparation.Loader;
import org.janelia.saalfeldlab.regionmerging.loader.hdf5.AffinitiesAndLabelsFromH5;
import org.janelia.saalfeldlab.regionmerging.loader.hdf5.LoaderFromLoaders;
import org.janelia.saalfeldlab.util.unionfind.HashMapStoreUnionFind;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
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
		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, 2 );

		System.out.println( "Start agglomerating!" );
		final JavaPairRDD< HashWrapper< long[] >, Data >[] intermediate = new JavaPairRDD[ 1 ];
		for ( int i = 0; i < thresholds.length; ++i )
		{
			final Options opts = options.copy().threshold( thresholds[ i ] );
			rm.agglomerate( sc, blockedGraph, mergesLogger[ i ], opts, ( iteration, rdd ) -> intermediate[ 0 ] = rdd, ( iteration, keys ) -> intermediate[ 0 ] );
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



}
