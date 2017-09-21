package de.hanslovsky.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.hdf5.IHDF5FloatReader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class HDF5FloatLoader implements CellLoader< FloatType >
{
	private final IHDF5FloatReader reader;

	private final String dataset;

	public HDF5FloatLoader( final IHDF5FloatReader reader, final String dataset )
	{
		super();
		this.reader = reader;
		this.dataset = dataset;
	}

	@Override
	public void load( final Img< FloatType > cell ) throws Exception
	{
		final MDFloatArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		for ( final FloatType c : Views.flatIterable( cell ) )
			c.set( targetCell.get( i++ ) );
	}

}
