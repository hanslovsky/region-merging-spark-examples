package org.janelia.saalfeldlab.regionmerging.loader.hdf5;

import java.util.Arrays;
import java.util.Optional;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class AffinitiesAndLabelsFromH5
{

	public static LoaderFromLoaders< LongType, FloatType, LongArray, FloatArray > get(
			final String affinitiesFile,
			final String affinitiesPath,
			final String superVoxelFile,
			final String superVoxelPath,
			final boolean reverseLastDimension )
	{
		final IHDF5Reader affinitiesLoader = HDF5Factory.openForReading( affinitiesFile );
		final IHDF5Reader superVoxelLoader = HDF5Factory.openForReading( superVoxelFile );

		final HDF5DataSetInformation affinitiesDataset = affinitiesLoader.getDataSetInformation( affinitiesPath );
		final HDF5DataSetInformation superVoxelDataset = superVoxelLoader.getDataSetInformation( superVoxelPath );

		final long[] affinitiesDimensions = Util.reorder( affinitiesDataset.getDimensions() );
		final long[] superVoxelDimensions = Util.reorder( superVoxelDataset.getDimensions() );

		final int[] affinitiesChunkSize = affinitiesDataset.getStorageLayout().equals( HDF5StorageLayout.CHUNKED ) ? Util.reorder( affinitiesDataset.tryGetChunkSizes() ) : Arrays.stream( affinitiesDimensions ).mapToInt( l -> ( int ) l ).toArray();
		final int[] superVoxelChunkSize = superVoxelDataset.getStorageLayout().equals( HDF5StorageLayout.CHUNKED ) ? Util.reorder( superVoxelDataset.tryGetChunkSizes() ) : Arrays.stream( superVoxelDimensions ).mapToInt( l -> ( int ) l ).toArray();

		final CellGrid affinitiesGrid = new CellGrid( affinitiesDimensions, affinitiesChunkSize );
		final CellGrid superVoxelGrid = new CellGrid( superVoxelDimensions, superVoxelChunkSize );

		final CellLoader< FloatType > affinitiesCellLoader = reverseLastDimension ? new HDF5FloatLoaderFlipLastDimension( affinitiesLoader, affinitiesPath ) : new HDF5FloatLoader( affinitiesLoader, affinitiesPath );
		final CellLoader< LongType > superVoxelCellLoader = new HDF5LongLoader( superVoxelLoader, superVoxelPath );

		return new LoaderFromLoaders<>( superVoxelGrid, affinitiesGrid, superVoxelCellLoader, affinitiesCellLoader, new LongType(), new FloatType(), new LongArray( 1 ), new FloatArray( 1 ) );

	}

	public static < T extends NativeType< T > > Optional< Pair< CellLoader< T >, CellGrid > > get(
			final String file,
			final String path,
			final boolean reverseLastDimension,
			final T t )
	{
		final IHDF5Reader reader = HDF5Factory.openForReading( file );
		final HDF5DataSetInformation ds = reader.getDataSetInformation( path );
		final long[] dims = Util.reorder( ds.getDimensions() );
		final int[] chunkSize = ds.getStorageLayout().equals( HDF5StorageLayout.CHUNKED ) ? Util.reorder( ds.tryGetChunkSizes() ) : Arrays.stream( dims ).mapToInt( l -> ( int ) l ).toArray();
		System.out.println( "OPENING DS AT " + path + " " + Arrays.toString( dims ) + " " + Arrays.toString( chunkSize ) );
		final CellGrid grid = new CellGrid( dims, chunkSize );
		if ( t instanceof FloatType )
			return Optional.of( new ValuePair<>( ( CellLoader< T > ) ( reverseLastDimension ? new HDF5FloatLoaderFlipLastDimension( reader, path ) : new HDF5FloatLoader( reader, path ) ), grid ) );
		else if ( t instanceof UnsignedIntType )
			return Optional.of( new ValuePair<>( ( CellLoader< T > ) new HDF5UnsignedIntLoader( reader, path), grid ) );
		else if ( t instanceof LongType )
			return Optional.of( new ValuePair<>( ( CellLoader< T > ) new HDF5LongLoader( reader, path ), grid ) );
		else if ( t instanceof UnsignedByteType )
			return Optional.of( new ValuePair<>( ( CellLoader< T > ) new HDF5UnsignedByteLoader( reader, path), grid ) );
		return Optional.empty();
	}

}
