package org.janelia.saalfeldlab.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.IHDF5LongReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class HDF5LongLoader implements CellLoader< LongType >
{
	private final IHDF5LongReader reader;

	private final String dataset;

	public HDF5LongLoader( final IHDF5Reader reader, final String dataset )
	{
		super();
		this.reader = reader.int64();
		this.dataset = dataset;
	}

	@Override
	public void load( final SingleCellArrayImg< LongType, ? > cell ) throws Exception
	{
		final MDLongArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		for ( final LongType c : Views.flatIterable( cell ) )
			c.set( targetCell.get( i++ ) );
	}

}
