package bigdata;

import java.util.*;

import java.util.regex.Pattern;
import java.util.Map.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.util.*;

import com.google.common.cache.AbstractCache.StatsCounter;

import scala.Tuple2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;


public class Project {
	static StringBuilder sb = new StringBuilder();
	public static void calculStats(JavaRDD<Long> rdd){
		double mediane;
		long middle=rdd.count()/2;
		long premierquadrant=rdd.count()/4;
		long troisiemequadrant=(rdd.count()*3)/4;
		Map<Integer,Long> histogram=null;


		JavaPairRDD<Long,Long> rddwithindex=rdd.sortBy(f-> f,true, rdd.getNumPartitions()).zipWithIndex().mapToPair(f-> {return new Tuple2<Long,Long>(f._2,f._1);});


		if(middle%2==1){
			mediane=rddwithindex.lookup(middle).get(0);
		}
		else{
			mediane=(rddwithindex.lookup(middle-1).get(0)+rddwithindex.lookup(middle).get(0))/2;
		}
		if(premierquadrant%4==0){
			premierquadrant=rddwithindex.lookup(premierquadrant).get(0);
			troisiemequadrant=rddwithindex.lookup(troisiemequadrant).get(0);
		}
		else{
			premierquadrant=rddwithindex.lookup(premierquadrant+1).get(0);
			troisiemequadrant=rddwithindex.lookup(troisiemequadrant+1).get(0);
		}
		double mean=rdd.mapToDouble(phase->phase).mean();
		double max=rdd.mapToDouble(phase->phase).max();
		double min=rdd.mapToDouble(phase->phase).min();
		histogram=rdd.mapToPair(f->new Tuple2<Integer,Long>((int)Math.pow(10,Math.floor(Math.log10(f))),f)).countByKey();
		TreeMap<Integer, Long> histogramT = new TreeMap<Integer, Long>(histogram);

		System.out.println("Max  : " + max+" ");

		System.out.println("Min  : " + min+" ");

		System.out.println("Moyenne  : " + mean+" ");

		System.out.println("Mediane  : "+ mediane+" " );

		System.out.println("premier quadrant  : "+premierquadrant+" ");

		System.out.println("troisieme quadrant  : "+troisiemequadrant+" ");

		System.out.println("Histogram");
		for(Map.Entry<Integer, Long> entry :  histogramT.entrySet()){
			System.out.println(entry.getKey() + " => " + ": " + entry.getValue());
		}

	}

	public static void WriteToFile(JavaSparkContext context , String FileName) {
		ArrayList<String> output=new ArrayList<String>();
		output.add(sb.toString());
		context.parallelize(output).saveAsTextFile(FileName);
		sb= new StringBuilder();
	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Project");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd = context.textFile(args[0]);
		JavaRDD<PhaseWritable> Phases = rdd.map(new ParseTophaseWritable()).persist(StorageLevel.MEMORY_AND_DISK());


		

		/*Question 1.a*/

		JavaRDD<Long> PhasesWithoutIdle =Phases.filter(f -> !f.getPatterns().equals("-1")).map(f->f.getDuration());
		//sb.append("Phases non IDLE");
		calculStats(PhasesWithoutIdle);
		//WriteToFile(context,"NonIDLE");

       /*Question 1.b
		System.out.println("Phases IDLE");
		JavaRDD<Long> PhasesIdle =Phases.filter(f -> f.getPatterns().equals("-1")).map(f->f.getDuration());
		calculStats(PhasesIdle);
		
		/*Question 1.c*
		System.out.println("la distribution de la durée des phases par motif d'acces");
		for(int i =0;i<22;i++)
		{   
			final int k=i;
			JavaRDD<Long> MotifAcces=Phases.filter(f -> f.getPatterns().equals(k+"")).map(f->f.getDuration());
			calculStats(MotifAcces);
		}

		/*Question 2*
		System.out.println("Nombre de motifs d'acces  par phase");
		JavaRDD<Long> MotifAccesPhases=Phases.filter(f -> f.getNpatterns()!=0).map(f->f.getNpatterns());
		calculStats(MotifAccesPhases);	

		/*Question 3
		System.out.println("Nombre de jobs par phase");
		JavaRDD<Long> PhasesNbjob=Phases.filter(f -> f.getNpatterns()!=0).map(f->f.getNjobs());
		calculStats(PhasesNbjob);		



		/*Question 4.a */
		
		System.out.println("Distribution du temps total d’accès au PFS par job");
		JavaPairRDD<String,Long> PhaseJob=Phases.mapToPair(f-> {return new Tuple2<String,Long>(f.getJobs(),f.getDuration());});
		JavaPairRDD<String,Long> PhaseJobReduce= PhaseJob.reduceByKey((x,y) -> x + y);		 
		JavaRDD<Long> Phasejobduration=PhaseJobReduce.map(f->f._2);
		calculStats(Phasejobduration);

		/*Question 4.b 
		JavaRDD<Long> Phasejobduration2= Phasejobduration.sortBy(f->f,true, Phasejobduration.getNumPartitions());
		List<Long> top10=Phasejobduration2.top(10);
		System.out.println("Top 10 jobs en temps total d’accès au PFS");
		for(Long i:top10)
		{System.out.println("Top"+top10.indexOf(i)+"="+i);	}


		/*Question 5 

		JavaRDD<Long> PhasesIDLE=Phases.filter(f -> f.getPatterns().equals("-1")).map(f->f.getDuration());
		Double sum=PhasesIDLE.mapToDouble(phase->phase).sum();
	    System.out.println("Temps total en ​ idle ​ du système =" +sum);*/



		/*Question 6.a
		JavaPairRDD<String,Long> listPhase=Phases.filter(f -> !f.getPatterns().equals("-1"))
				.map(f->{return new Tuple2<>(f.getDuration(),f.getListPatterns());})
				.flatMapToPair(listTuple -> {
					List<Tuple2<String,Long>> phaseDuration = new ArrayList<>();
					for(String phase : listTuple._2){
						phaseDuration.add(new Tuple2<>(phase,listTuple._1));
					}
					return phaseDuration.iterator();
				}).reduceByKey((a, b) -> a+ b );

		StatCounter Counter = listPhase.mapToDouble(stringLongTuple2 -> stringLongTuple2._2).stats();
		Double sum = Counter.sum();

		JavaPairRDD<String,Double> durationPercentage =  listPhase.mapValues(duration  ->  duration * (100/ sum));


		for(Tuple2<String,Double> tuple2 : durationPercentage.collect()){
			System.out.println("Pattern = "+tuple2._1+" : Pourcentage duration = "+tuple2._2);
		}

		/*Question 6.b
		JavaRDD<Long> listPhaseDuration=listPhase.map(f->f._2);
		JavaRDD<Long> listPhaseSort= listPhaseDuration.sortBy(f-> f,true, listPhaseDuration.getNumPartitions());
		List<Long> top10=listPhaseSort.top(10);
		System.out.println("Top 10 jobs en temps total d’accès au PFS");
		for(Long i:top10)
		{System.out.println("Top"+top10.indexOf(i)+"="+i);	}
*/

		context.close();
	}


}
