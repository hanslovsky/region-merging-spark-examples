package org.janelia.saalfeldlab.regionmerging;

import java.io.IOException;

public class ShowSuperVoxels
{

	public static void main( final String[] args ) throws IOException
	{
//		final String n5AffinitiesRoot = "/groups/saalfeld/home/hanslovskyp/from_funkej/fib25/fib25-e402c09-2017.04.06-23.06.29_z6000-7000_y2200-3200_x2800-3800/n5";// args[
//		// 0
//		// ];
//		final String n5AffinitiesDs = "main";// args[ 1 ];
//		final String n5SuperVoxelRoot = "/groups/saalfeld/home/hanslovskyp/from_funkej/fib25/fib25-e402c09-2017.04.06-23.06.29_z6000-7000_y2200-3200_x2800-3800/n5"; // args[
//		// 2
//		// ];
//		final String n5SuperVoxelDs = "supervoxels";// args[ 3 ];
//
//		final N5FSReader sourceReader = new N5FSReader( n5AffinitiesRoot );
//		final DatasetAttributes attrs = sourceReader.getDatasetAttributes( n5AffinitiesDs );
//		final int[] blockSize = attrs.getBlockSize();
//		final long[] dimensions = attrs.getDimensions();
//
//		final N5CellLoader< FloatType > affinitiesLoader = new N5CellLoader<>( sourceReader, n5AffinitiesDs, blockSize );
//		final CellGrid affsGrid = new CellGrid( dimensions, blockSize );
//		final Cache< Long, Cell< VolatileFloatArray > > affsCache = new SoftRefLoaderCache< Long, Cell< VolatileFloatArray > >().withLoader( LoadedCellCacheLoader.get( affsGrid, affinitiesLoader, new FloatType(), AccessFlags.VOLATILE ) );
//		final RandomAccessibleInterval< FloatType > affs = new CachedCellImg< FloatType, VolatileFloatArray >( affsGrid, new FloatType(), affsCache, new VolatileFloatArray( 1, false ) );
//		final Converter< RealComposite< VolatileFloatType >, VolatileARGBType > affsConv = ( s, t ) -> {
//			final boolean isValid = s.get( 0 ).isValid() && s.get( 1 ).isValid() && s.get( 2 ).isValid();
//			t.setValid( isValid );
//			if ( isValid )
//			{
//				final double max = Math.max( Math.max( s.get( 0 ).getRealDouble(), s.get( 1 ).getRealDouble() ), s.get( 2 ).getRealDouble() );
//				t.get().set( ( int ) ( 255 * ( 1 - max ) ) << 16 | ( int ) ( 255 * ( 1 - max ) ) << 8 | ( int ) ( 255 * ( 1 - max ) ) << 0 );
//			}
//		};
//		final BdvStackSource< VolatileARGBType > bdv = BdvFunctions.show( Converters.convert( Views.collapseReal( VolatileViews.wrapAsVolatile( affs ) ), affsConv, new VolatileARGBType() ), "affs" );
//
//		final long[] labelsDimensions = Arrays.stream( dimensions ).limit( dimensions.length - 1 ).toArray();
//		final int[] labelsBlockSize = Arrays.stream( blockSize ).limit( blockSize.length - 1 ).toArray();
//
//		final N5CellLoader< UnsignedLongType > labelsLoader = new N5CellLoader<>( new N5FSReader( n5SuperVoxelRoot ), n5SuperVoxelDs, labelsBlockSize );
//		final TLongIntHashMap colors = new TLongIntHashMap();
//		final Random rng = new Random( 100 );
//		final CellGrid labelsGrid = new CellGrid( labelsDimensions, labelsBlockSize );
//		final Cache< Long, Cell< VolatileLongArray > > labelsCache = new SoftRefLoaderCache< Long, Cell< VolatileLongArray > >().withLoader( LoadedCellCacheLoader.get( labelsGrid, labelsLoader, new UnsignedLongType(), AccessFlags.VOLATILE ) );
//		final RandomAccessibleInterval< UnsignedLongType > labels = new CachedCellImg< UnsignedLongType, VolatileLongArray >( labelsGrid, new UnsignedLongType(), labelsCache, new VolatileLongArray( 1, false ) );
//		final Converter< VolatileUnsignedLongType, VolatileARGBType > conv = ( s, t ) -> {
//			final boolean isValid = s.isValid();
//			t.setValid( isValid );
//			if ( isValid )
//			{
//				final long label = s.get().get();
//				if ( !colors.contains( label ) )
//					colors.put( label, rng.nextInt() );
//				t.get().set( colors.get( label ) );
//			}
//		};
//		BdvFunctions.show( Converters.convert( VolatileViews.wrapAsVolatile( labels ), conv, new VolatileARGBType() ), "labels", BdvOptions.options().addTo( bdv ) );

	}

}
