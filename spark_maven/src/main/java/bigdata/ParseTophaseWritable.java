package bigdata;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

class ParseTophaseWritable implements Function<String, PhaseWritable>{
	   private static final Pattern SPLIT=Pattern.compile(";");
		@Override
		public PhaseWritable call(String line)  {
			String[] phases=SPLIT.split(line);
			long duration=Long.parseLong(phases[2]);
			long npatterns=Long.parseLong(phases[4]);
			long njobs=Long.parseLong(phases[6]);
			long ndays=Long.parseLong(phases[8]);
			
			return new PhaseWritable(duration,phases[3],npatterns,phases[5],njobs,phases[7],ndays);
		}

		
	}