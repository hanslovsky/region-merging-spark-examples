package org.janelia.saalfeldlab.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.hdf5.IHDF5ByteReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class HDF5UnsignedByteLoader implements CellLoader< UnsignedByteType >
{
	private final IHDF5ByteReader reader;

	private final String dataset;

	public HDF5UnsignedByteLoader( final IHDF5Reader reader, final String dataset )
	{
		super();
		this.reader = reader.uint8();
		this.dataset = dataset;
	}

	@Override
	public void load( final SingleCellArrayImg< UnsignedByteType, ? > cell ) throws Exception
	{
		final MDByteArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		for ( final UnsignedByteType c : Views.flatIterable( cell ) )
			c.set( targetCell.get( i++ ) );
	}

}
