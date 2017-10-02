package org.janelia.saalfeldlab.regionmerging.loader.hdf5;

import java.util.Arrays;

import bdv.img.hdf5.Util;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.hdf5.IHDF5FloatReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.Cursor;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class HDF5FloatLoaderFlipLastDimension implements CellLoader< FloatType >
{
	private final IHDF5FloatReader reader;

	private final String dataset;

	public HDF5FloatLoaderFlipLastDimension( final IHDF5Reader reader, final String dataset )
	{
		super();
		this.reader = reader.float32();
		this.dataset = dataset;
	}

	@Override
	public void load( final SingleCellArrayImg< FloatType, ? > cell ) throws Exception
	{
		System.out.println( "LOADING CELL AT " + Arrays.toString( Intervals.minAsLongArray( cell ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( cell ) ) + " " + reader.toString() + " " + dataset );
		final MDFloatArray targetCell = reader.readMDArrayBlockWithOffset(
				dataset,
				Arrays.stream( Util.reorder( Intervals.dimensionsAsLongArray( cell ) ) ).mapToInt( val -> ( int ) val ).toArray(),
				Util.reorder( Intervals.minAsLongArray( cell ) ) );
		int i = 0;
		final ArrayImg< FloatType, FloatArray > tmp = ArrayImgs.floats( Intervals.dimensionsAsLongArray( cell ) );
		for ( final FloatType c : tmp )
			c.set( targetCell.get( i++ ) );

		final int nDim = tmp.numDimensions();
		final int lastDim = nDim - 1;
		final long lastDimSize = tmp.dimension( lastDim );
		for ( long channel = 0, other = lastDimSize - 1; channel < lastDimSize; ++channel, --other )
		{
			final IntervalView< FloatType > hsCell = Views.hyperSlice( cell, lastDim, channel );
			final IntervalView< FloatType > hsTmp = Views.hyperSlice( tmp, lastDim, other );
			final Cursor< FloatType > s = hsTmp.cursor();
			final Cursor< FloatType > t = hsCell.cursor();
			while ( s.hasNext() )
				t.next().set( s.next() );
		}
	}

}
