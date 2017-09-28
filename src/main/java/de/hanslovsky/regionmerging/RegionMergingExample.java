package de.hanslovsky.regionmerging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.javafx.application.PlatformImpl;

import bdv.bigcat.viewer.atlas.Atlas;
import bdv.bigcat.viewer.atlas.data.RandomAccessibleIntervalSpec;
import bdv.img.h5.H5Utils;
import bdv.img.hdf5.Util;
import bdv.util.Prefs;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import de.hanslovsky.graph.edge.EdgeCreator;
import de.hanslovsky.graph.edge.EdgeCreator.AffinityHistogram;
import de.hanslovsky.graph.edge.EdgeMerger;
import de.hanslovsky.graph.edge.EdgeMerger.MEDIAN_AFFINITY_MERGER;
import de.hanslovsky.graph.edge.EdgeWeight;
import de.hanslovsky.graph.edge.EdgeWeight.MedianAffinityWeight;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Data;
import de.hanslovsky.regionmerging.BlockedRegionMergingSpark.Options;
import de.hanslovsky.regionmerging.DataPreparation.Loader;
import de.hanslovsky.regionmerging.loader.hdf5.HDF5FloatLoader;
import de.hanslovsky.regionmerging.loader.hdf5.HDF5LongLoader;
import de.hanslovsky.util.unionfind.HashMapStoreUnionFind;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TIntHashSet;
import javafx.application.Platform;
import javafx.stage.Stage;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.TypeIdentity;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class RegionMergingExample
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args )
	{
		final String affinitiesFile = HOME_DIR + "/Downloads/excerpt.h5";
		final String affinitiesPath = "main";// "affs-0-6-120x60+150+0";
		final String superVoxelFile = affinitiesFile;
		final String superVoxelPath = "zws";// "zws-0-6-120x60+150+0";

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/raw-and-affinities.h5";
//		final String affinitiesPath = "volumes/predicted_affs";
//		final String superVoxelFile = HOME_DIR + "/local/tmp/data-jan/labels.h5";
//		final String superVoxelPath = "volumes/labels/neuron_ids";

//		final String affinitiesFile = HOME_DIR + "/local/tmp/data-jan/cutout.h5";
//		final String affinitiesPath = "aff";
//		final String superVoxelFile = affinitiesFile;
//		final String superVoxelPath = "seg";

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

		final HDF5FloatLoader affinitiesCellLoader = new HDF5FloatLoader( affinitiesLoader, affinitiesPath );
		final HDF5LongLoader superVoxelCellLoader = new HDF5LongLoader( superVoxelLoader, superVoxelPath );

		final Cache< Long, Cell< FloatArray > > affinitiesCache = new SoftRefLoaderCache< Long, Cell< FloatArray > >().withLoader( LoadedCellCacheLoader.get( affinitiesGrid, affinitiesCellLoader, new FloatType() ) );
		final Cache< Long, Cell< LongArray > > superVoxelCache = new SoftRefLoaderCache< Long, Cell< LongArray > >().withLoader( LoadedCellCacheLoader.get( superVoxelGrid, superVoxelCellLoader, new LongType() ) );

		final RandomAccessibleInterval< FloatType > input = new CachedCellImg<>( affinitiesGrid, new FloatType(), affinitiesCache, new FloatArray( 1 ) );
		final RandomAccessibleInterval< LongType > labels = new CachedCellImg<>( superVoxelGrid, new LongType(), superVoxelCache, new LongArray( 1 ) );

		System.out.println( "Loaded affinities from " + affinitiesFile + "/" + affinitiesFile );
		final RandomAccessibleInterval< FloatType > affs = ArrayImgs.floats( Intervals.dimensionsAsLongArray( input ) );

		final int[] perm = getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( inputPerm, affs ), affs ) )
			p.getB().set( p.getA() );

		for ( int d = 0; d < 3; ++d )
			for ( final RealComposite< FloatType > aff : Views.hyperSlice( Views.collapseReal( affs ), d, 0l ) )
				aff.get( d ).set( Float.NaN );

		System.out.println( "Loaded labels from " + superVoxelFile + "/" + superVoxelPath );

		final int stepZ = 5;


		final Random rng = new Random( 100 );
		final TLongIntHashMap colors = new TLongIntHashMap();
		for ( final LongType l : Views.flatIterable( labels ) )
			if ( !colors.contains( l.get() ) )
			{
				final int r = rng.nextInt( 255 );// + 255 - 100;
				final int g = rng.nextInt( 255 );// + 255 - 100;
				final int b = rng.nextInt( 255 );// + 255 - 100;
				colors.put( l.get(), 0xff << 24 | r << 16 | g << 8 | b << 0 );
			}
		colors.put( 0, 0 );

		final RandomAccessibleInterval< ARGBType > rgbAffs = Converters.convert( Views.collapseReal( affs ), ( s, t ) -> {
			t.set( ARGBType.rgba( 255 * ( 1 - s.get( 0 ).get() ), 255 * ( 1 - s.get( 1 ).get() ), 255 * ( 1 - s.get( 2 ).get() ), 255.0 ) );
		}, new ARGBType() );
		final double factor = 1.0;// 0.52;

		final double[][] resolution = { { 1.0, 1.0, 10.0 } };

		Prefs.showMultibox( false );
		Prefs.showTextOverlay( false );

		final Converter< LongType, ARGBType > colorConv = ( s, t ) -> {
			t.set( colors.get( s.get() ) );
		};

		PlatformImpl.startup( () -> {} );
		Platform.setImplicitExit( true );
		final Atlas atlas = new Atlas( labels );
		Platform.runLater( () -> {
			final Stage stage = new Stage();
			atlas.start( stage );
			stage.show();
			final RandomAccessibleIntervalSpec< ARGBType, ARGBType > spec = new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { rgbAffs }, new RandomAccessibleInterval[] { rgbAffs }, resolution, null, "affs" );
			atlas.addARGB( spec );

			final RandomAccessibleInterval< ARGBType > colored = Converters.convert( labels, colorConv, new ARGBType() );
			final RandomAccessibleIntervalSpec< ?, ARGBType > voxelSpec = new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { labels }, new RandomAccessibleInterval[] { colored }, resolution, null, "super voxels " );
			atlas.addARGB( voxelSpec );
		} );
//		for ( final FloatType a : Views.hyperSlice( affs, 3, 2l ) )
//			a.mul( factor );
//
//		{
//			final IntervalView< FloatType > hs = Views.hyperSlice( affs, 3, 2l );
//			for ( long z = 0; z < hs.dimension( 2 ); ++z ) {
//				final IntervalView< FloatType > hs2 = Views.hyperSlice( hs, 2, z );
//				Erosion.erodeInPlace( Views.extendBorder( hs2 ), hs2, new RectangleShape( 10, false ), Runtime.getRuntime().availableProcessors() );
//			}
//		}

		final RealComposite< FloatType > extension = Views.collapseReal( ArrayImgs.floats( new float[ affs.numDimensions() ], 1, affs.numDimensions() ) ).randomAccess().get();

//		final RealRandomAccess< RealComposite< FloatType > > rra = Views.interpolate( Views.extendValue( Views.collapseReal( affs ), extension ), new NearestNeighborInterpolatorFactory<>() ).realRandomAccess();
//		final ValueDisplayListener vdl = new ValueDisplayListener( bdv.getBdvHandle().getViewerPanel() );
//		final Function< RealComposite< FloatType >, String > ts = ( Function< RealComposite< FloatType >, String > ) col -> String.format( "(%.3f,%.3f,%.3f)", col.get( 0 ).get(), col.get( 1 ).get(), col.get( 2 ).get() );
//		vdl.addSource( bdv.getSources().get( 0 ).getSpimSource(), rra, ts );
//		final BdvStackSource< ARGBType > bdvLabels = BdvFunctions.show( Converters.convert( labels, colorConv, new ARGBType() ), "labels", BdvOptions.options().addTo( bdv ) );
//		vdl.addSource( bdvLabels.getSources().get( 0 ).getSpimSource(), Views.interpolate( Views.extendValue( labels, new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ).realRandomAccess(), v -> v.toString() );

		final int nBins = 256;
		final AffinityHistogram creator = new EdgeCreator.AffinityHistogram( nBins, 0.0, 1.0 );
		final MEDIAN_AFFINITY_MERGER merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
		final MedianAffinityWeight edgeWeight = new EdgeWeight.MedianAffinityWeight( nBins, 0.0, 1.0 );

//		final NoDataSerializableCreator creator = new EdgeCreator.NoDataSerializableCreator();
//		final MIN_AFFINITY_MERGER merger = new EdgeMerger.MIN_AFFINITY_MERGER();
//		final EdgeWeight edgeWeight = new EdgeWeight.OneMinusAffinity();

		final long[] dimensions = Intervals.dimensionsAsLongArray( labels );
		final int[] blockSize = { ( int ) dimensions[ 0 ], ( int ) dimensions[ 1 ], stepZ };

		final CellLoader< LongType > ll = cell -> {
			burnIn( labels, cell );
		};

		final CellLoader< FloatType > al = cell -> {
			burnIn( affs, cell );
		};
		Logger.getLogger( "org" ).setLevel( Level.WARN );
		Logger.getLogger( "akka" ).setLevel( Level.WARN );
		Logger.getLogger( "spark" ).setLevel( Level.WARN );

		Logger.getLogger( "org" ).setLevel( Level.OFF );
		Logger.getLogger( "akka" ).setLevel( Level.OFF );

		final SparkConf conf = new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( DataPreparation.class.toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() );

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "OFF" );

		sc.setLogLevel( "ERROR" );

		final JavaPairRDD< HashWrapper< long[] >, Data > graph = DataPreparation.createGraphPointingBackwards( sc, new FloatAndLongLoader( dimensions, blockSize, ll, al ), creator, merger, blockSize );
		graph.cache();
		final long nBlocks = graph.count();
		System.out.println( "Starting with " + nBlocks + " blocks." );

		final MergesAccumulator accu = new MergesAccumulator();
		sc.sc().register( accu, "mergesAccumulator" );

		final IntFunction< MergeNotifyWithFinishNotification > mergeNotifyGenerator = new MergeNotifyGenerator( accu );


		final BlockedRegionMergingSpark rm = new BlockedRegionMergingSpark( merger, edgeWeight, mergeNotifyGenerator, 2 );

		final Options options = new BlockedRegionMergingSpark.Options( 0.5, StorageLevel.MEMORY_ONLY() );

		final TIntObjectHashMap< TLongArrayList > mergesLog = new TIntObjectHashMap<>();

		final BiConsumer< Integer, JavaPairRDD< HashWrapper< long[] >, Tuple2< TLongArrayList, HashMapStoreUnionFind > > > mergesLogger = ( i, rdd ) -> {
			final TLongArrayList merges = new TLongArrayList();
			rdd.values().collect().stream().map( Tuple2::_1 ).forEach( merges::addAll );
			final int newIndex = mergesLog.size();
			mergesLog.put( newIndex, merges );
		};

		System.out.println( "Start agglomerating!" );
		rm.agglomerate( sc, graph, mergesLogger, options );
		System.out.println( "Done agglomerating!" );

		final TIntObjectHashMap< TLongArrayList > merges = accu.value();

		final HashMapStoreUnionFind[] ufs = Stream.generate( HashMapStoreUnionFind::new ).limit( merges.size() ).toArray( HashMapStoreUnionFind[]::new );

		for ( int iteration = 0; iteration < ufs.length; ++iteration )
		{

			final TLongArrayList list = merges.get( iteration );

			System.out.println( "Got " + list.size() / 4 + " merges!" );

			for ( int ufIndex = iteration; ufIndex < ufs.length; ++ufIndex )
			{

				final HashMapStoreUnionFind uf = ufs[ ufIndex ];
				for ( int i = 0; i < list.size(); i += 4 )
				{
					final long r1 = uf.findRoot( list.get( i + 0 ) );
					final long r2 = uf.findRoot( list.get( i + 1 ) );
//				System.out.println( list.get( i ) + " " + list.get( i + 1 ) + " " + r1 + " " + r2 );
					if ( r1 != r2 )
						uf.join( r1, r2 );
				}
			}
		}

//		final TLongArrayList list = mergesLog.get( 0 );
//		System.out.println( "Got " + lst.size() / 2 + " merges!" );
//		for ( int i = 0; i < list.size(); i += 2 )
//		{
//			final long r1 = uf.findRoot( list.get( i + 0 ) );
//			final long r2 = uf.findRoot( list.get( i + 1 ) );
////			System.out.println( list.get( i ) + " " + list.get( i + 1 ) + " " + r1 + " " + r2 );
//			if ( r1 != r2 )
//				uf.join( r1, r2 );
//		}

		final ArrayList< RandomAccessibleIntervalSpec< LongType, ARGBType > > raiSpecs = new ArrayList<>();

		for ( int i = ufs.length - 1; i < ufs.length; ++i )
		{
			final HashMapStoreUnionFind uf = ufs[ i ];
			final RandomAccessibleInterval< LongType > firstJoined = Converters.convert( labels, ( s, t ) -> {
				t.set( uf.findRoot( s.get() ) );
			}, new LongType() );
			H5Utils.saveLong( firstJoined, "merged-" + stepZ + "-" + i + ".h5", "labels", Intervals.dimensionsAsIntArray( firstJoined ) );
			final RandomAccessibleInterval< ARGBType > colored = Converters.convert( firstJoined, colorConv, new ARGBType() );
			raiSpecs.add( new RandomAccessibleIntervalSpec<>( new TypeIdentity<>(), new RandomAccessibleInterval[] { firstJoined }, new RandomAccessibleInterval[] { colored }, resolution, null, "iteratrion " + i ) );
		}

		Platform.runLater( () -> {
			raiSpecs.forEach( atlas::addARGB );
		} );



	}

	public static class FloatAndLongLoader implements Loader< LongType, FloatType, LongArray, FloatArray >
	{

		private final long[] dimensions;

		private final int[] blockSize;

		private final CellLoader< LongType > labelLoader;

		private final CellLoader< FloatType > affinitiesLoader;

		public FloatAndLongLoader( final long[] dimensions, final int[] blockSize, final CellLoader< LongType > labelLoader, final CellLoader< FloatType > affinitiesLoader )
		{
			super();
			this.dimensions = dimensions;
			this.blockSize = blockSize;
			this.labelLoader = labelLoader;
			this.affinitiesLoader = affinitiesLoader;
		}

		@Override
		public CellGrid labelGrid()
		{
			return new CellGrid( dimensions, blockSize );
		}

		@Override
		public CellGrid affinitiesGrid()
		{
			final long[] d = new long[ dimensions.length + 1 ];
			final int[] b = new int[ dimensions.length + 1 ];
			System.arraycopy( dimensions, 0, d, 0, dimensions.length );
			System.arraycopy( blockSize, 0, b, 0, dimensions.length );
			d[ dimensions.length ] = dimensions.length;
			b[ dimensions.length ] = dimensions.length;
			return new CellGrid( d, b );
		}

		@Override
		public CellLoader< LongType > labelLoader()
		{
			return labelLoader;
		}

		@Override
		public CellLoader< FloatType > affinitiesLoader()
		{
			return affinitiesLoader;
		}

		@Override
		public LongType labelType()
		{
			return new LongType();
		}

		@Override
		public FloatType affinityType()
		{
			return new FloatType();
		}

		@Override
		public LongArray labelAccess()
		{
			return new LongArray( 0 );
		}

		@Override
		public FloatArray affinityAccess()
		{
			return new FloatArray( 0 );
		}

	}

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}

	public static < T extends Type< T > > void burnIn( final RandomAccessible< T > source, final RandomAccessibleInterval< T > target )
	{
		for ( final Pair< T, T > p : Views.flatIterable( Views.interval( Views.pair( source, target ), target ) ) )
			p.getB().set( p.getA() );
	}

	public static class Registrator implements KryoRegistrator
	{

		@Override
		public void registerClasses( final Kryo kryo )
		{
//			kryo.register( HashMap.class );
//			kryo.register( TIntHashSet.class, new TIntHashSetSerializer() );
			kryo.register( Data.class, new DataSerializer() );
//			kryo.register( TDoubleArrayList.class, new TDoubleArrayListSerializer() );
//			kryo.register( TLongLongHashMap.class, new TLongLongHashMapListSerializer() );
//			kryo.register( HashWrapper.class, new HashWrapperSerializer<>() );
		}

	}

	public static class DataSerializer extends Serializer< Data >
	{

		@Override
		public void write( final Kryo kryo, final Output output, final Data object )
		{
			// edges
			output.writeInt( object.edges().size() );
			output.writeDoubles( object.edges().toArray() );

			// non-contracting edges
			output.writeInt( object.nonContractingEdges().size() );
			for ( final Entry< HashWrapper< long[] >, TIntHashSet > entry : object.nonContractingEdges().entrySet() )
			{
//				System.out.println( "Writing entry: " + entry );
				output.writeInt( entry.getKey().getData().length );
				output.writeLongs( entry.getKey().getData() );
				output.writeInt( entry.getValue().size() );
				output.writeInts( entry.getValue().toArray() );
			}

			// counts
			output.writeInt( object.counts().size() );
			output.writeLongs( object.counts().keys() );
			output.writeLongs( object.counts().values() );
		}

		@Override
		public Data read( final Kryo kryo, final Input input, final Class< Data > type )
		{
			// edges
			final int numEdges = input.readInt();
			final TDoubleArrayList edgeStore = new TDoubleArrayList( input.readDoubles( numEdges ) );

			// non-contracting edges
			final int size = input.readInt();
			final HashMap< HashWrapper< long[] >, TIntHashSet > nonContractingEdges = new HashMap<>();
			for ( int i = 0; i < size; ++i )
			{
//				System.out.println( "reading key" );
				final int nDim = input.readInt();
				final HashWrapper< long[] > key = HashWrapper.longArray( input.readLongs( nDim ) );
//				System.out.println( "reading value" );
				final int setSize = input.readInt();
				final TIntHashSet value = new TIntHashSet( input.readInts( setSize ) );
//				System.out.println( "ok" );
				nonContractingEdges.put( key, value );
			}

			// counts
			final int numNodes = input.readInt();
			final long[] keys = input.readLongs( numNodes );
			final long[] values = input.readLongs( numNodes );
			final TLongLongHashMap counts = new TLongLongHashMap( keys, values );
			return new Data(
					edgeStore,
					nonContractingEdges,
					counts );
		}
	}

	public static class MergeNotifyGenerator implements IntFunction< MergeNotifyWithFinishNotification >, Serializable
	{

		private final MergesAccumulator merges;

		public MergeNotifyGenerator( final MergesAccumulator merges )
		{
			super();
			this.merges = merges;
		}

		@Override
		public MergeNotifyWithFinishNotification apply( final int value )
		{
			final TLongArrayList mergesInBlock = new TLongArrayList();
			return new MergeNotifyWithFinishNotification()
			{

				@Override
				public void addMerge( final long node1, final long node2, final long newNode, final double weight )
				{
					mergesInBlock.add( node1 );
					mergesInBlock.add( node2 );
					mergesInBlock.add( newNode );
					mergesInBlock.add( Double.doubleToRawLongBits( weight ) );
//					System.out.println( "Added merge " + node1 + " " + node2 + " " + newNode + " " + weight );
				}

				@Override
				public void notifyDone()
				{
					synchronized ( merges )
					{
						final TIntObjectHashMap< TLongArrayList > m = new TIntObjectHashMap<>();
						m.put( value, mergesInBlock );
						merges.add( m );
					}
					System.out.println( "Added " + mergesInBlock.size() / 4 + " merges at iteration " + value );
				}
			};
		}

	}

	public static class MergesAccumulator extends AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > >
	{

		private final TIntObjectHashMap< TLongArrayList > data;

		public MergesAccumulator()
		{
			this( new TIntObjectHashMap<>() );
		}

		public MergesAccumulator( final TIntObjectHashMap< TLongArrayList > data )
		{
			super();
			this.data = data;
		}

		@Override
		public void add( final TIntObjectHashMap< TLongArrayList > data )
		{
			synchronized ( this.data )
			{
				for ( final TIntObjectIterator< TLongArrayList > it = data.iterator(); it.hasNext(); )
				{
					it.advance();
					if ( !this.data.contains( it.key() ) )
						this.data.put( it.key(), new TLongArrayList() );
					this.data.get( it.key() ).addAll( it.value() );
				}
			}
		}

		@Override
		public AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > > copy()
		{
			synchronized ( data )
			{
				final TIntObjectHashMap< TLongArrayList > copy = new TIntObjectHashMap<>( data );
				return new MergesAccumulator( copy );
			}
		}

		@Override
		public boolean isZero()
		{
			synchronized ( data )
			{
				return data.size() == 0;
			}
		}

		@Override
		public void merge( final AccumulatorV2< TIntObjectHashMap< TLongArrayList >, TIntObjectHashMap< TLongArrayList > > other )
		{
			synchronized ( data )
			{
				add( other.value() );
			}
		}

		@Override
		public void reset()
		{
			synchronized ( data )
			{
				this.data.clear();
			}
		}

		@Override
		public TIntObjectHashMap< TLongArrayList > value()
		{
			synchronized ( data )
			{
				return data;
			}
		}

	}

}
