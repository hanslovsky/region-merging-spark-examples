package org.janelia.saalfeldlab.regionmerging;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;
import org.janelia.saalfeldlab.regionmerging.AgglomerateAndLog.Registrator;
import org.janelia.saalfeldlab.util.unionfind.HashMapStoreUnionFind;

import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class CreateSuperVoxels
{

	public static void main( final String[] args ) throws IOException
	{
		final String n5AffinitiesRoot = "/groups/saalfeld/home/hanslovskyp/from_funkej/fib25/fib25-e402c09-2017.04.06-23.06.29_z6000-7000_y2200-3200_x2800-3800/n5";// args[
		// 0
		// ];
		final String n5AffinitiesDs = "main";// args[ 1 ];
		final String n5SuperVoxelRoot = "/groups/saalfeld/home/hanslovskyp/from_funkej/fib25/fib25-e402c09-2017.04.06-23.06.29_z6000-7000_y2200-3200_x2800-3800/n5"; // args[
		// 2
		// ];
		final String n5SuperVoxelDs = "supervoxels";// args[ 3 ];

		final N5FSReader sourceReader = new N5FSReader( n5AffinitiesRoot );
		final DatasetAttributes attrs = sourceReader.getDatasetAttributes( n5AffinitiesDs );
		final int[] blockSize = attrs.getBlockSize();
		final long[] dimensions = attrs.getDimensions();

		final long[] labelsDimensions = Arrays.stream( dimensions ).limit( dimensions.length - 1 ).toArray();
		final int[] labelsBlockSize = Arrays.stream( blockSize ).limit( blockSize.length-1 ).toArray();
		final N5FSWriter superVoxelWriter = new N5FSWriter( n5SuperVoxelRoot );
		superVoxelWriter.createDataset( n5SuperVoxelDs, labelsDimensions, labelsBlockSize, DataType.UINT64, CompressionType.GZIP );


		final List< HashWrapper< long[] > > blocks = DataPreparation.collectAllOffsets( labelsDimensions, labelsBlockSize, HashWrapper::longArray );

		final SparkConf conf = new SparkConf()
				.setAppName( CreateSuperVoxels.class.getSimpleName() )
				.setMaster( "local[*]" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() );
		final JavaSparkContext sc = new JavaSparkContext( conf );

		final JavaPairRDD< HashWrapper< long[] >, ArrayImg< UnsignedLongType, LongArray > > labelBlocks = sc.parallelize( blocks ).mapToPair( block -> {
			final long[] blockPosition = block.getData();
			final N5FSReader reader = new N5FSReader( n5AffinitiesRoot );
			final N5CellLoader< FloatType > loader = new N5CellLoader<>( reader, n5AffinitiesDs, blockSize );
			final CellGrid grid = new CellGrid( dimensions, blockSize );
			final Cache< Long, Cell< FloatArray > > affinitiesCache = new SoftRefLoaderCache< Long, Cell< FloatArray > >().withLoader( LoadedCellCacheLoader.get( grid, loader, new FloatType() ) );
			final CachedCellImg< FloatType, FloatArray > affinities = new CachedCellImg<>( grid, new FloatType(), affinitiesCache, new FloatArray( 1 ) );
			final long[] dim = Arrays.stream( labelsBlockSize ).mapToLong( i -> i ).toArray();

			for ( int d = 0; d < dim.length; ++d )
				dim[ d ] = blockPosition[ d ] + dim[ d ] > labelsDimensions[ d ] ? labelsDimensions[ d ] - blockPosition[ d ] : dim[ d ];

				final ArrayImg< UnsignedLongType, LongArray > store = ArrayImgs.unsignedLongs( new LongArray( ( int ) Intervals.numElements( dim ) ), dim );
				final RandomAccessibleInterval< UnsignedLongType > labels = Views.translate( store, block.getData() );

				createSuperVoxels( affinities, labels, 0.1 );

				return new Tuple2<>( block, store );
		} );

		labelBlocks.map( tuple -> {
			final long[] blockPosition = tuple._1().getData();
			final long[] blockIndex = IntStream.range( 0, blockPosition.length ).mapToLong( i -> blockPosition[ i ] / labelsBlockSize[ i ] ).toArray();
			final N5FSWriter writer = new N5FSWriter( n5SuperVoxelRoot);
			final long[] store = tuple._2().update( null ).getCurrentStorageArray();
			final LongArrayDataBlock block = new LongArrayDataBlock( Intervals.dimensionsAsIntArray( tuple._2() ), blockIndex, store );
			final DatasetAttributes superVoxelAttributes = writer.getDatasetAttributes( n5SuperVoxelDs );
			writer.writeBlock( n5SuperVoxelDs, superVoxelAttributes, block );
			return true;
		} ).count();

		sc.close();

	}

	public static < R extends RealType< R >, I extends IntegerType< I > > void createSuperVoxels( final RandomAccessibleInterval< R > affinities, final RandomAccessibleInterval< I > labels, final double threshold )
	{

		final TLongLongHashMap parents = new TLongLongHashMap();
		final HashMapStoreUnionFind uf = new HashMapStoreUnionFind( parents, new TLongLongHashMap(), 0 );

		for ( final Cursor< I > l = Views.flatIterable( labels ).cursor(); l.hasNext(); )
		{
			l.fwd();
			final long id = IntervalIndexer.positionToIndex( l, labels );
			l.get().setInteger( id );
			uf.findRoot( id );
		}

		final int ndim = labels.numDimensions();
		final long[] lowerMin = Intervals.minAsLongArray( labels );
		final long[] upperMin = Intervals.minAsLongArray( labels );
		final long[] lowerMax = Intervals.maxAsLongArray( labels );
		final long[] upperMax = Intervals.maxAsLongArray( labels );

		for ( int d = 0; d < ndim; ++d )
		{

			if ( labels.dimension( d ) < 2 )
				continue;

			upperMin[ d ] += 1;
			lowerMax[ d ] -= 1;

			final Cursor< I > upperCursor = Views.flatIterable( Views.offsetInterval( labels, new FinalInterval( upperMin, upperMax ) ) ).cursor();
			final Cursor< I > lowerCursor = Views.flatIterable( Views.offsetInterval( labels, new FinalInterval( lowerMin, lowerMax ) ) ).cursor();
			final Cursor< R > affinitiesCursor = Views.flatIterable( Views.offsetInterval( Views.hyperSlice( affinities, ndim, ( long ) d ), new FinalInterval( upperMin, upperMax ) ) ).cursor();

			while ( upperCursor.hasNext() )
			{
				final double aff = affinitiesCursor.next().getRealDouble();
				upperCursor.fwd();
				lowerCursor.fwd();
				if ( !Double.isNaN( aff ) && aff > threshold )
				{
					final long l1 = upperCursor.get().getIntegerLong();
					final long l2 = lowerCursor.get().getIntegerLong();
					if ( l1 != l2 )
					{
						final long r1 = uf.findRoot( l1 );
						final long r2 = uf.findRoot( l2 );
						if ( r1 != r2 )
							uf.join( r1, r2 );
					}
				}
			}

			upperMin[ d ] -= 1;
			lowerMax[ d ] += 1;
		}

		for ( final I label : Views.flatIterable( labels ) )
			label.setInteger( uf.findRoot( label.getIntegerLong() ) );

	}

}
