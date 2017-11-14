package org.janelia.saalfeldlab.regionmerging;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator;
import org.janelia.saalfeldlab.graph.edge.EdgeCreator.AffinityHistogram;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger;
import org.janelia.saalfeldlab.graph.edge.EdgeMerger.MEDIAN_AFFINITY_MERGER;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight;
import org.janelia.saalfeldlab.graph.edge.EdgeWeight.MedianAffinityWeight;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;
import org.janelia.saalfeldlab.regionmerging.BlockedRegionMergingSpark.Data;
import org.janelia.saalfeldlab.regionmerging.DataPreparation.Loader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.set.hash.TIntHashSet;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class RegionMergingFromN5Render
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args ) throws IOException
	{

		final String n5AffinitiesRoot = args[ 0 ];
		final String n5SuperVoxelRoot = args[ 2 ];
		final String n5AffinitiesDataset = args[ 1 ];
		final String n5SuperVoxelDataset = args[ 3 ];
		final String renderRoot = args[ 4 ];
		final double[] thresholds = Arrays.stream( args[ 5 ].split( "," ) ).mapToDouble( Double::parseDouble ).toArray();

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

		final int[] blockSize = superVoxelChunkSize;
		final long[] dimensions = superVoxelDimensions;

		final N5CellLoader< FloatType > al = new N5CellLoader<>( affinitiesReader, n5AffinitiesDataset, affinitiesChunkSize );
		final N5CellLoader< UnsignedLongType > ll = new N5CellLoader<>( superVoxelReader, n5SuperVoxelDataset, superVoxelChunkSize );

		final SparkConf conf = new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( MethodHandles.lookup().lookupClass().toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() );

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "OFF" );

		sc.setLogLevel( "ERROR" );

		final JavaPairRDD< HashWrapper< long[] >, Data > graph = DataPreparation.createGraphPointingBackwards( sc, new FloatAndUnsignedLongLoader( dimensions, blockSize, ll, al ), creator, merger, blockSize );
		graph.cache();
		final long nBlocks = graph.count();
		System.out.println( "Starting with " + nBlocks + " blocks." );

//		for ( int thresholdIndex = 0; thresholdIndex < thresholds.length; ++thresholdIndex )
//		{
//			final double threshold = thresholds[ thresholdIndex ];
//			final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, mergeNotifyGenerator, 2 );
//
//			final Options options = new BlockedRegionMergingSpark.Options( threshold, StorageLevel.MEMORY_ONLY() );
//
//			final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger = ( iteration, rdd ) -> {
//				if ( rdd.count() == 1 )
//				{
//					final N5FSWriter n5Writer = new N5FSWriter( renderRoot );
//					n5Writer.createDataset( "threshold=" + threshold, superVoxelDimensions, superVoxelChunkSize, DataType.UINT64, CompressionType.GZIP );
//					rdd.map( tuple -> {
//						final long[] position = tuple._1().getData();
//						final String subDir = baseDirPath + File.separator + String.join( File.separator, Arrays.stream( position ).mapToObj( pos -> String.format( "%d", pos ) ).limit( position.length - 1 ).collect( Collectors.toList() ) );
//						final String file = subDir + File.separator + position[ position.length - 1 ];
//						new File( subDir ).mkdirs();
//
//						final StringBuilder sb = new StringBuilder();
//						sb.append( "weight,from,to" );
//
//						final TLongArrayList merges = tuple._2()._1();
//						for ( int merge = 0; merge < merges.size(); merge += RegionMerging.MERGES_LOG_STEP_SIZE )
//							sb
//							.append( "\n" ).append( Edge.ltd( merges.get( merge + RegionMerging.MERGES_LOG_WEIGHT_OFFSET ) ) )
//							.append( "," ).append( merges.get( merge + RegionMerging.MERGES_LOG_FROM_OFFSET ) )
//							.append( "," ).append( merges.get( merge + RegionMerging.MERGES_LOG_TO_OFFSET ))
//							;
//
//						Files.write( Paths.get( file ), sb.toString().getBytes() );
//
//						return true;
//					} ).count();
//				}
//			};
//
//			System.out.println( "Start agglomerating!" );
//			rm.agglomerate( sc, graph, mergesLogger, options );
//			System.out.println( "Done agglomerating!" );
//
//		}


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
