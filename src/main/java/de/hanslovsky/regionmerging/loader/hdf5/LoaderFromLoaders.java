package de.hanslovsky.regionmerging.loader.hdf5;

import de.hanslovsky.regionmerging.DataPreparation.Loader;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;

public class LoaderFromLoaders< LABEL extends NativeType< LABEL >, AFF extends NativeType< AFF >, LABEL_A, AFF_A > implements Loader< LABEL, AFF, LABEL_A, AFF_A >
{

	CellGrid labelGrid;

	CellGrid affinitiesGrid;

	private final CellLoader< LABEL > labelLoader;

	private final CellLoader< AFF > affinitiesLoader;

	private final LABEL l;

	private final AFF a;

	private final LABEL_A labelAccess;

	private final AFF_A affinityAccess;

	public LoaderFromLoaders(
			final CellGrid labelsGrid,
			final CellGrid affinitiesGrid,
			final CellLoader< LABEL > labelLoader,
			final CellLoader< AFF > affinitiesLoader,
			final LABEL l,
			final AFF a,
			final LABEL_A labelAccess,
			final AFF_A affinityAccess )
	{
		super();
		this.labelGrid = labelsGrid;
		this.affinitiesGrid = affinitiesGrid;
		this.labelLoader = labelLoader;
		this.affinitiesLoader = affinitiesLoader;
		this.l = l;
		this.a = a;
		this.labelAccess = labelAccess;
		this.affinityAccess = affinityAccess;
	}

	@Override
	public CellGrid labelGrid()
	{
		return this.labelGrid;
	}

	@Override
	public CellGrid affinitiesGrid()
	{
		return this.affinitiesGrid;
	}

	@Override
	public CellLoader< LABEL > labelLoader()
	{
		return labelLoader;
	}

	@Override
	public CellLoader< AFF > affinitiesLoader()
	{
		return affinitiesLoader;
	}

	@Override
	public LABEL labelType()
	{
		return l;
	}

	@Override
	public AFF affinityType()
	{
		return a;
	}

	@Override
	public LABEL_A labelAccess()
	{
		return labelAccess;
	}

	@Override
	public AFF_A affinityAccess()
	{
		return affinityAccess;
	}

}
