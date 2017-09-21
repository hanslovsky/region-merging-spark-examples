package de.hanslovsky.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.hdf5.IHDF5LongReader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class HDF5UnsignedLongLoader implements CellLoader< UnsignedLongType >
{
	private final IHDF5LongReader reader;

	private final String dataset;

	public HDF5UnsignedLongLoader( final IHDF5LongReader reader, final String dataset )
	{
		super();
		this.reader = reader;
		this.dataset = dataset;
	}

	@Override
	public void load( final Img< UnsignedLongType > cell ) throws Exception
	{
		final MDLongArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		for ( final UnsignedLongType c : Views.flatIterable( cell ) )
			c.set( targetCell.get( i++ ) );
	}

}
