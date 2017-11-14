package org.janelia.saalfeldlab.regionmerging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;
import org.janelia.saalfeldlab.util.unionfind.HashMapStoreUnionFind;

import com.sun.javafx.application.PlatformImpl;

import bdv.bigcat.viewer.atlas.Atlas;
import bdv.bigcat.viewer.atlas.data.LabelDataSource;
import bdv.bigcat.viewer.atlas.data.LabelDataSourceFromDelegates;
import bdv.bigcat.viewer.atlas.data.RandomAccessibleIntervalDataSource;
import bdv.bigcat.viewer.state.FragmentSegmentAssignmentWithHistory;
import bdv.bigcat.viewer.state.SelectedIds;
import bdv.bigcat.viewer.stream.GoldenAngleSaturatedHighlightingARGBStream;
import bdv.util.volatiles.SharedQueue;
import gnu.trove.map.hash.TLongLongHashMap;
import javafx.stage.Stage;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealARGBConverter;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.volatiles.VolatileARGBType;
import net.imglib2.type.volatiles.VolatileUnsignedByteType;
import net.imglib2.type.volatiles.VolatileUnsignedLongType;

public class RegionMergingExampleFromFile
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	@SuppressWarnings( { "restriction", "unchecked" } )
	public static void main( final String[] args ) throws IOException
	{
		final String n5AffinitiesRoot = args[ 0 ];
		final String n5SuperVoxelRoot = args[ 2 ];
		final String n5AffinitiesDataset = args[ 1 ];
		final String n5SuperVoxelDataset = args[ 3 ];
		final String mergeTreeBaseDir = args[ 4 ];

		final N5FSReader affinitiesReader = new N5FSReader( n5AffinitiesRoot );
		final N5FSReader superVoxelReader = new N5FSReader( n5SuperVoxelRoot );
		final DatasetAttributes affinitiesAttributes = affinitiesReader.getDatasetAttributes( n5AffinitiesDataset );
		final DatasetAttributes superVoxelAttributes = superVoxelReader.getDatasetAttributes( n5SuperVoxelDataset );

		final long[] affinitiesDimensions = affinitiesAttributes.getDimensions();
		final long[] superVoxelDimensions = superVoxelAttributes.getDimensions();

		final int[] affinitiesChunkSize = affinitiesAttributes.getBlockSize();
		final int[] superVoxelChunkSize = superVoxelAttributes.getBlockSize();

		final N5CellLoader< UnsignedByteType > al = new N5CellLoader<>( affinitiesReader, n5AffinitiesDataset, affinitiesChunkSize );
		final N5CellLoader< UnsignedLongType > ll = new N5CellLoader<>( superVoxelReader, n5SuperVoxelDataset, superVoxelChunkSize );

		final CellGrid affinitiesGrid = new CellGrid( affinitiesDimensions, affinitiesChunkSize );
		final CellGrid superVoxelGrid = new CellGrid( superVoxelDimensions, superVoxelChunkSize );

		final Cache< Long, Cell< ByteArray > > affinitiesCache = new SoftRefLoaderCache< Long, Cell< ByteArray > >().withLoader( LoadedCellCacheLoader.get( affinitiesGrid, al, new UnsignedByteType() ) );
		final Cache< Long, Cell< LongArray > > superVoxelCache = new SoftRefLoaderCache< Long, Cell< LongArray > >().withLoader( LoadedCellCacheLoader.get( superVoxelGrid, ll, new UnsignedLongType() ) );

		final CachedCellImg< UnsignedByteType, ByteArray > affinities = new CachedCellImg<>( affinitiesGrid, new UnsignedByteType(), affinitiesCache, new ByteArray( 1 ) );
		final RandomAccessibleInterval< UnsignedLongType > superVoxelBase = new CachedCellImg<>( superVoxelGrid, new UnsignedLongType(), superVoxelCache, new LongArray( 1 ) );
//		double min = Double.POSITIVE_INFINITY;
//		double max = Double.NEGATIVE_INFINITY;
//		for ( final FloatType a : affinities )
//		{
//			final double v = a.getRealDouble();
//			min = Math.min( v, min );
//			max = Math.max( v, max );
//		}
//		System.out.println( " GOT MIN MAX " + min + " " + max );

		final Collection< File > files = FileUtils.listFiles( new File( mergeTreeBaseDir ), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE );
		final List< String > fileNames = files.stream().map( File::getAbsolutePath ).collect( Collectors.toList() );
		final UnionFindFromArray unionFindGenerator = UnionFindFromArray.fromFiles( fileNames );
		final double thresh = 0.1;
		final HashMapStoreUnionFind uf = unionFindGenerator.getAtLevel( thresh );
		System.out.println( "THRESHOLDED AT LEVEL " + thresh );
		final RandomAccessibleInterval< UnsignedLongType > superVoxel = Converters.convert( superVoxelBase, ( s, t ) -> t.set( uf.findRoot( s.get() ) ), new UnsignedLongType() );

		final GoldenAngleSaturatedHighlightingARGBStream gs = new GoldenAngleSaturatedHighlightingARGBStream( new SelectedIds(), new FragmentSegmentAssignmentWithHistory( new TLongLongHashMap(), action -> {}, () -> {
			try
			{
				Thread.sleep( Long.MAX_VALUE );
			}
			catch ( final InterruptedException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return new TLongLongHashMap();
		} ) );
		final Converter< UnsignedLongType, ARGBType > cmapConverter = ( s, t ) -> t.set( gs.argb( s.get() ) );
//		final Random rng = new Random( 100 );
//		final TLongIntHashMap colorMap = new TLongIntHashMap();
//		for ( final UnsignedLongType sv : superVoxel )
//		{
//			final long val = sv.get();
//			if ( !colorMap.contains( val ) )
//				colorMap.put( val, rng.nextInt() | 0xff000000 );
//		}
//
//		final Converter< UnsignedLongType, ARGBType > cmapConverter = ( s, t ) -> {
//			final long val = uf.findRoot( s.get() );
//			t.set( colorMap.get( val ) );
//		};
		final RandomAccessibleInterval< ARGBType > ARGBSuperVoxels = Converters.convert( superVoxel, cmapConverter, new ARGBType() );

		final Converter< ARGBType, VolatileARGBType > convertVolatile = ( s, t ) -> t.get().set( s );
		final Function< RandomAccessibleInterval< ARGBType >, RandomAccessibleInterval< VolatileARGBType > > makeVolatile = s -> Converters.convert( s, convertVolatile, new VolatileARGBType() );

		final SharedQueue queue = new SharedQueue( 12, 1 );

		final AffineTransform3D[] mipmapTransforms = {
				new AffineTransform3D()
		};
		mipmapTransforms[ 0 ].set( 4, 0, 0 );
		mipmapTransforms[ 0 ].set( 4, 1, 1 );
		mipmapTransforms[ 0 ].set( 40, 2, 2 );

		PlatformImpl.startup( () -> {} );
		final Atlas atlas = new Atlas( superVoxel, queue );
		PlatformImpl.runLater( () -> {
			final Stage stage = new Stage();
			try
			{
				atlas.start( stage );
			}
			catch ( final InterruptedException e )
			{
				throw new RuntimeException( e );
			}

			final RealARGBConverter< VolatileUnsignedByteType > conv = new RealARGBConverter<>( 0, 255 );
			final RandomAccessibleIntervalDataSource< UnsignedByteType, VolatileUnsignedByteType > rawSource = new RandomAccessibleIntervalDataSource<>(
					new RandomAccessibleInterval[] { affinities },
					new RandomAccessibleInterval[] { makeVolatile( affinities, VolatileUnsignedByteType::new ) },
					mipmapTransforms,
					RegionMergingExample.makeFactory( new UnsignedByteType() ),
					RegionMergingExample.makeFactory( new VolatileUnsignedByteType() ),
					UnsignedByteType::new,
					VolatileUnsignedByteType::new,
					"raw" );
			atlas.addRawSource( rawSource, 0, 255 );

			final RandomAccessibleIntervalDataSource< UnsignedLongType, VolatileUnsignedLongType > svSource = new RandomAccessibleIntervalDataSource<>(
					new RandomAccessibleInterval[] { superVoxel },
					new RandomAccessibleInterval[] { makeVolatile( superVoxel, VolatileUnsignedLongType::new ) },
					mipmapTransforms,
					RegionMergingExample.makeFactory( new UnsignedLongType() ),
					RegionMergingExample.makeFactory( new VolatileUnsignedLongType() ),
					UnsignedLongType::new,
					VolatileUnsignedLongType::new,
					"super voxels" );
			final FragmentSegmentAssignmentWithHistory assignment = new FragmentSegmentAssignmentWithHistory( action -> {}, () -> {
				try
				{
					Thread.sleep( Integer.MAX_VALUE );
				}
				catch ( final InterruptedException e )
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return new TLongLongHashMap();
			} );
			final LabelDataSource< UnsignedLongType, VolatileUnsignedLongType > delegateSource = new LabelDataSourceFromDelegates<>( svSource, assignment );
			atlas.addLabelSource( delegateSource, t -> t.get().getIntegerLong() );
		} );

	}

	public static < T extends Type< T >, V extends Volatile< T > & Type< V > > RandomAccessibleInterval< V > makeVolatile( final RandomAccessibleInterval< T > rai, final Supplier< V > generator )
	{
		final V v = generator.get();
		v.setValid( true );
		final Converter< T, V > conv = ( s, t ) -> t.get().set( s );
		return Converters.convert( rai, conv, v );
	}

}
