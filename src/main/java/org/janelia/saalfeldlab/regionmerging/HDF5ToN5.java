package org.janelia.saalfeldlab.regionmerging;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.regionmerging.loader.hdf5.AffinitiesAndLabelsFromH5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.converter.Converters;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;

public class HDF5ToN5
{

	public static Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args ) throws IOException
	{

		final String h5Path = "/groups/saalfeld/home/hanslovskyp/from_papec/for_philipp/sampleA+/sampleA+_wsdt_unetLRGPV1_automatically_realigned.h5";
		final String n5Path = "/groups/saalfeld/home/hanslovskyp/from_papec/for_philipp/sampleA+/n5";
//		final String dsPath = "main"; // float32
		final String dsPath = "data"; // uint8
		final String n5PathDS = "supervoxel";
//		final String dsPath = "gt"; // uint64
		final int[] n5BlockSize = { 64, 64, 64, 3 };

		final Optional< Pair< CellLoader< UnsignedIntType >, CellGrid > > reader = AffinitiesAndLabelsFromH5.get( h5Path, dsPath, true, new UnsignedIntType() );
		if ( !reader.isPresent() )
		{
			System.out.println( "NOT THERE!" );
			System.exit( 123 );
		}

		final CellGrid grid = new CellGrid( reader.get().getB().getImgDimensions(), Arrays.stream( reader.get().getB().getImgDimensions() ).mapToInt( l -> ( int ) l ).toArray() );
//		final CellGrid grid = reader.get().getB();


		final Cache< Long, Cell< IntArray > > cache = new SoftRefLoaderCache< Long, Cell< IntArray > >().withLoader( LoadedCellCacheLoader.get( grid, reader.get().getA(), new UnsignedIntType() ) );

		final RandomAccessibleInterval< UnsignedIntType > img = new CachedCellImg<>( grid, new UnsignedIntType(), cache, new IntArray( 1 ) );

		System.out.println( reader.get().getB() + " " + Arrays.toString( Intervals.dimensionsAsLongArray( img ) ) );

//		final CellRandomAccess< FloatType, ? > ra = img.randomAccess();
//		ra.setPosition( new long[] { 0, 1, 4, 0 } );
//		ra.get().get();
//		System.exit( 123 );

		final N5FSWriter n5 = new N5FSWriter( n5Path );
		N5Utils.save( Converters.convert( img, ( s, t ) -> t.set( s.get() ), new UnsignedLongType() ), n5, n5PathDS, n5BlockSize, CompressionType.GZIP );

	}



}
