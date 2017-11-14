package org.janelia.saalfeldlab.regionmerging;

import java.lang.invoke.MethodHandles;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyExample
{

	public static Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static void main( final String[] args )
	{

		final SparkConf conf = new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( DataPreparation.class.toString() )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" );

		final JavaSparkContext sc = new JavaSparkContext( conf );
		final int start = 1;
		final int stop = 1000001;
		final Long sum = sc.parallelize( IntStream.range( start, stop ).mapToObj( Long::new ).collect( Collectors.toList() ) ).treeAggregate( 0l, Long::sum, Long::sum );
		sc.close();

		LOG.error( "The sum of all integers from {} to {} (inclusive) is {}", start, stop - 1, sum );


	}



}
