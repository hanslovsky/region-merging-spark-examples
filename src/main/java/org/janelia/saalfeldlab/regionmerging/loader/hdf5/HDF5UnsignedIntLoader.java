package org.janelia.saalfeldlab.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.hdf5.IHDF5IntReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class HDF5UnsignedIntLoader implements CellLoader< UnsignedIntType >
{
	private final IHDF5IntReader reader;

	private final String dataset;

	public HDF5UnsignedIntLoader( final IHDF5Reader reader, final String dataset )
	{
		super();
		this.reader = reader.uint32();
		this.dataset = dataset;
	}

	@Override
	public void load( final SingleCellArrayImg< UnsignedIntType, ? > cell ) throws Exception
	{
		final MDIntArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		for ( final UnsignedIntType c : Views.flatIterable( cell ) )
			c.set( targetCell.get( i++ ) );
	}

}
