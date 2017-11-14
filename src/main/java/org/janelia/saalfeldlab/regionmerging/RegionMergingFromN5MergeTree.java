package org.janelia.saalfeldlab.regionmerging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import org.janelia.saalfeldlab.graph.edge.Edge;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator.AffinityHistogram;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger.MEDIAN_AFFINITY_MERGER;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight.MedianAffinityWeight;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Data;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Options;
import org.janelia.saalfeldlab.regionmerging.DataPreparation.Loader;
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
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import scala.Tuple2;

public class RegionMergingFromN5MergeTree
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args ) throws IOException
	{

		final String n5AffinitiesRoot = args[ 0 ];
		final String n5SuperVoxelRoot = args[ 2 ];
		final String n5AffinitiesDataset = args[ 1 ];
		final String n5SuperVoxelDataset = args[ 3 ];
		final String mergeTreeBaseDir = args[ 4 ];
		final String intermediateBase = args[ 5 ];
		final Optional< int[] > cellDimensions = args.length > 6 ? Optional.of( Arrays.stream( args[ 6 ].split( "," ) ).mapToInt( Integer::parseInt ).toArray() ) : Optional.empty();

		final N5FSReader affinitiesReader = new N5FSReader( n5AffinitiesRoot );
		final N5FSReader superVoxelReader = new N5FSReader( n5SuperVoxelRoot );
		final DatasetAttributes affinitiesAttributes = affinitiesReader.getDatasetAttributes( n5AffinitiesDataset );
		final DatasetAttributes superVoxelAttributes = superVoxelReader.getDatasetAttributes( n5SuperVoxelDataset );

		final long[] affinitiesDimensions = affinitiesAttributes.getDimensions();
		final long[] superVoxelDimensions = superVoxelAttributes.getDimensions();

		final int[] affinitiesChunkSize = affinitiesAttributes.getBlockSize();
		final int[] superVoxelChunkSize = superVoxelAttributes.getBlockSize();

		final int nBins = 256;
		final AffinityHistogram creator = new EdgeCreator.AffinityHistogram( nBins, 0.0, 1.0 );
		final MEDIAN_AFFINITY_MERGER merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
		final MedianAffinityWeight edgeWeight = new EdgeWeight.MedianAffinityWeight( nBins, 0.0, 1.0 );

		final int[] blockSize = cellDimensions.orElse( superVoxelChunkSize );
		final long[] dimensions = superVoxelDimensions;

//		final N5CellLoader< FloatType > al = new N5CellLoader<>( affinitiesReader, n5AffinitiesDataset, affinitiesChunkSize );
		final FloatTypeN5Loader al = new FloatTypeN5Loader( n5AffinitiesRoot, n5AffinitiesDataset );
//		final N5CellLoader< UnsignedLongType > ll = new N5CellLoader<>( superVoxelReader, n5SuperVoxelDataset, superVoxelChunkSize );
		final UnsignedLongTypeN5Loader ll = new UnsignedLongTypeN5Loader( n5SuperVoxelRoot, n5SuperVoxelDataset );

		final SparkConf conf = new SparkConf()
//				.setMaster( "local[*]" )
				.setAppName( MethodHandles.lookup().lookupClass().toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() )
				;

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "OFF" );

		sc.setLogLevel( "ERROR" );

		final JavaPairRDD< HashWrapper< long[] >, Data > graph = DataPreparation.createGraphPointingBackwards( sc, new FloatAndUnsignedLongLoader( dimensions, blockSize, ll, al ), creator, merger, blockSize );
		graph.cache();
		final long nBlocks = graph.count();
		System.out.println( "Starting with " + nBlocks + " blocks." );

		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, 2 );

		final Options options = new BlockedRegionMergingSpark.Options( 1.0, StorageLevel.MEMORY_AND_DISK_SER() );

		final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger = ( i, rdd ) -> {
			final String baseDirPath = mergeTreeBaseDir + File.separator + i;
			final File baseDir = new File( baseDirPath );
			baseDir.mkdirs();
			rdd.map( tuple -> {
				final long[] position = tuple._1().getData();
				final String subDir = baseDirPath + File.separator + String.join( File.separator, Arrays.stream( position ).mapToObj( pos -> String.format( "%d", pos ) ).limit( position.length - 1 ).collect( Collectors.toList() ) );
				final String file = subDir + File.separator + position[ position.length - 1 ];
				new File( subDir ).mkdirs();

				final StringBuilder sb = new StringBuilder();
				sb.append( "weight,from,to" );

				final TLongArrayList merges = tuple._2()._1();
				for ( int merge = 0; merge < merges.size(); merge += RegionMerging.MERGES_LOG_STEP_SIZE )
					sb
					.append( "\n" ).append( Edge.ltd( merges.get( merge + RegionMerging.MERGES_LOG_WEIGHT_OFFSET ) ) )
					.append( "," ).append( merges.get( merge + RegionMerging.MERGES_LOG_FROM_OFFSET ) )
					.append( "," ).append( merges.get( merge + RegionMerging.MERGES_LOG_TO_OFFSET ))
					;

				Files.write( Paths.get( file ), sb.toString().getBytes() );

				return true;
			} ).count();
		};

		System.out.println( "Start agglomerating!" );
		final IntFunction< String > makeFormat = iteration -> {
			return intermediateBase + File.separator + String.format( "%d", iteration ) + File.separator + "%d,%d,%d";
		};
		// TODO fix intermediate saving!
		rm.agglomerate(
				sc,
				graph,
				mergesLogger,
				options,
				( iteration, rdd ) -> rdd.map( new Persist( makeFormat.apply( iteration ) ) ).count(),
				( iteration, keys ) -> keys.repartition( keys.context().defaultParallelism() ).mapToPair( new Load( makeFormat.apply( iteration ) ) ) );
//		rm.agglomerate( sc, graph, mergesLogger, options );
		System.out.println( "Done agglomerating!" );


	}

	public static class Persist implements Function< Tuple2< HashWrapper< long[] >, Data >, Boolean >
	{

		private final String format;

		public Persist( final String format )
		{
			super();
			this.format = format;
		}

		@Override
		public Boolean call( final Tuple2< HashWrapper< long[] >, Data > tuple ) throws Exception
		{
			final String directory = String.format( format, Arrays.stream( tuple._1().getData() ).mapToObj( l -> l ).toArray() );
			final Data data = tuple._2();
			new File( directory ).mkdirs();

			final String edges = String.format( "%s%sedges", directory, File.separator );
			final String nonContracting = String.format( "%s%snon-contracting", directory, File.separator );

			final ObjectOutputStream edgesOut = new ObjectOutputStream( new FileOutputStream( edges ) );
			edgesOut.writeObject( data.edges().toArray() );
			edgesOut.close();

			final ObjectOutputStream nonContractingOut = new ObjectOutputStream( new FileOutputStream( nonContracting ) );
			nonContractingOut.writeObject( data.nonContractingEdges() );
			nonContractingOut.close();
			return true;
		}

	}

	public static class Load implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, Data >
	{

		private final String format;

		public Load( final String format )
		{
			super();
			this.format = format;
		}

		@Override
		public Tuple2< HashWrapper< long[] >, Data > call( final HashWrapper< long[] > pos ) throws Exception
		{
			final String directory = String.format( format, Arrays.stream( pos.getData() ).mapToObj( l -> l ).toArray() );

			final String edges = String.format( "%s%sedges", directory, File.separator );
			final String nonContracting = String.format( "%s%snon-contracting", directory, File.separator );

			final ObjectInputStream edgesIn = new ObjectInputStream( new FileInputStream( edges ) );
			final double[] e = ( double[] ) edgesIn.readObject();
			edgesIn.close();

			final ObjectInputStream nonContractingIn = new ObjectInputStream( new FileInputStream( nonContracting ) );
			final HashMap< HashWrapper< long[] >, TIntHashSet > nonContractingEdges = ( HashMap< HashWrapper< long[] >, TIntHashSet > ) nonContractingIn.readObject();
			nonContractingIn.close();

			return new Tuple2<>( pos, new Data( new TDoubleArrayList( e ), nonContractingEdges ) );

		}

	}

	public static class FloatAndUnsignedLongLoader implements Loader< UnsignedLongType, FloatType, LongArray, FloatArray >
	{

		private final long[] dimensions;

		private final int[] blockSize;

		private final CellLoader< UnsignedLongType > labelLoader;

		private final CellLoader< FloatType > affinitiesLoader;

		public FloatAndUnsignedLongLoader( final long[] dimensions, final int[] blockSize, final CellLoader< UnsignedLongType > labelLoader, final CellLoader< FloatType > affinitiesLoader )
		{
			super();
			this.dimensions = dimensions;
			this.blockSize = blockSize;
			this.labelLoader = labelLoader;
			this.affinitiesLoader = affinitiesLoader;
		}

		@Override
		public CellGrid labelGrid()
		{
			return new CellGrid( dimensions, blockSize );
		}

		@Override
		public CellGrid affinitiesGrid()
		{
			final long[] d = new long[ dimensions.length + 1 ];
			final int[] b = new int[ dimensions.length + 1 ];
			System.arraycopy( dimensions, 0, d, 0, dimensions.length );
			System.arraycopy( blockSize, 0, b, 0, dimensions.length );
			d[ dimensions.length ] = dimensions.length;
			b[ dimensions.length ] = dimensions.length;
			return new CellGrid( d, b );
		}

		@Override
		public CellLoader< UnsignedLongType > labelLoader()
		{
			return labelLoader;
		}

		@Override
		public CellLoader< FloatType > affinitiesLoader()
		{
			return affinitiesLoader;
		}

		@Override
		public UnsignedLongType labelType()
		{
			return new UnsignedLongType();
		}

		@Override
		public FloatType affinityType()
		{
			return new FloatType();
		}

		@Override
		public LongArray labelAccess()
		{
			return new LongArray( 0 );
		}

		@Override
		public FloatArray affinityAccess()
		{
			return new FloatArray( 0 );
		}

	}

	public static class FloatAndLongLoader implements Loader< LongType, FloatType, LongArray, FloatArray >
	{

		private final long[] dimensions;

		private final int[] blockSize;

		private final CellLoader< LongType > labelLoader;

		private final CellLoader< FloatType > affinitiesLoader;

		public FloatAndLongLoader( final long[] dimensions, final int[] blockSize, final CellLoader< LongType > labelLoader, final CellLoader< FloatType > affinitiesLoader )
		{
			super();
			this.dimensions = dimensions;
			this.blockSize = blockSize;
			this.labelLoader = labelLoader;
			this.affinitiesLoader = affinitiesLoader;
		}

		@Override
		public CellGrid labelGrid()
		{
			return new CellGrid( dimensions, blockSize );
		}

		@Override
		public CellGrid affinitiesGrid()
		{
			final long[] d = new long[ dimensions.length + 1 ];
			final int[] b = new int[ dimensions.length + 1 ];
			System.arraycopy( dimensions, 0, d, 0, dimensions.length );
			System.arraycopy( blockSize, 0, b, 0, dimensions.length );
			d[ dimensions.length ] = dimensions.length;
			b[ dimensions.length ] = dimensions.length;
			return new CellGrid( d, b );
		}

		@Override
		public CellLoader< LongType > labelLoader()
		{
			return labelLoader;
		}

		@Override
		public CellLoader< FloatType > affinitiesLoader()
		{
			return affinitiesLoader;
		}

		@Override
		public LongType labelType()
		{
			return new LongType();
		}

		@Override
		public FloatType affinityType()
		{
			return new FloatType();
		}

		@Override
		public LongArray labelAccess()
		{
			return new LongArray( 0 );
		}

		@Override
		public FloatArray affinityAccess()
		{
			return new FloatArray( 0 );
		}

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
			kryo.register( UnsignedLongTypeN5Loader.class, new UnsignedLongTypeN5Loader.LoaderSerializer() );
			kryo.register( FloatTypeN5Loader.class, new FloatTypeN5Loader.LoaderSerializer() );
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

	public static class UnsignedLongTypeN5Loader implements CellLoader< UnsignedLongType >
	{

		private final N5FSReader reader;

		private final String root;

		private final String dataset;

		private final int[] blockSize;

		private final DatasetAttributes attrs;

		public UnsignedLongTypeN5Loader( final String root, final String dataset ) throws IOException
		{
			super();
			this.reader = new N5FSReader( root );
			this.root = root;
			this.dataset = dataset;
			this.attrs = reader.getDatasetAttributes( dataset );
			this.blockSize = this.attrs.getBlockSize();
		}

		@Override
		public void load( final SingleCellArrayImg< UnsignedLongType, ? > cell ) throws Exception
		{
			final long[] gridPosition = Intervals.minAsLongArray( cell );
//			System.out.println( "Loading uint64 at " + Arrays.toString( gridPosition ) );
			for ( int i = 0; i < gridPosition.length; ++i )
				gridPosition[ i ] /= blockSize[ i ];
			final DataBlock< ? > block = reader.readBlock( dataset, attrs, gridPosition );
			final long[] data = ( long[] ) block.getData();
			final Cursor< UnsignedLongType > cursor = Views.flatIterable( cell ).cursor();
			for ( int i = 0; cursor.hasNext(); ++i )
				cursor.next().set( data[i] );
		}

		public static class LoaderSerializer extends Serializer< UnsignedLongTypeN5Loader >
		{
			@Override
			public void write( final Kryo kryo, final Output output, final UnsignedLongTypeN5Loader object )
			{
				// edges
				output.writeString( object.root );
				output.writeString( object.dataset );
			}

			@Override
			public UnsignedLongTypeN5Loader read( final Kryo kryo, final Input input, final Class< UnsignedLongTypeN5Loader > type )
			{
				// edges
				final String root = input.readString();
				final String dataset = input.readString();
				try
				{
					return new UnsignedLongTypeN5Loader( root, dataset );
				}
				catch ( final IOException e )
				{
					throw new RuntimeException( e );
				}
			}
		}
	}

	public static class FloatTypeN5Loader implements CellLoader< FloatType >
	{

		private final N5Reader reader;

		private final String root;

		private final String dataset;

		private final int[] blockSize;

		private final DatasetAttributes attrs;

		public FloatTypeN5Loader( final String root, final String dataset ) throws IOException
		{
			super();
			this.reader = new N5FSReader( root );
			this.root = root;
			this.dataset = dataset;
			this.attrs = reader.getDatasetAttributes( dataset );
			this.blockSize = this.attrs.getBlockSize();
		}

		@Override
		public void load( final SingleCellArrayImg< FloatType, ? > cell ) throws Exception
		{
			final long[] gridPosition = Intervals.minAsLongArray( cell );
			for ( int i = 0; i < gridPosition.length; ++i )
				gridPosition[ i ] /= blockSize[ i ];
			final DataBlock< ? > block = reader.readBlock( dataset, attrs, gridPosition );
			final float[] data = ( float[] ) block.getData();
			final Cursor< FloatType > cursor = Views.flatIterable( cell ).cursor();
			for ( int i = 0; cursor.hasNext(); ++i )
				cursor.next().set( data[ i ] );
		}

		public static class LoaderSerializer extends Serializer< FloatTypeN5Loader >
		{
			@Override
			public void write( final Kryo kryo, final Output output, final FloatTypeN5Loader object )
			{
				// edges
				output.writeString( object.root );
				output.writeString( object.dataset );
			}

			@Override
			public FloatTypeN5Loader read( final Kryo kryo, final Input input, final Class< FloatTypeN5Loader > type )
			{
				// edges
				final String root = input.readString();
				final String dataset = input.readString();
				try
				{
					return new FloatTypeN5Loader( root, dataset );
				}
				catch ( final IOException e )
				{
					throw new RuntimeException( e );
				}
			}
		}

	}

}
