package bigdata;
import java.io.Serializable;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PhaseWritable implements Writable, Serializable{
	private long duration,npatterns,njobs,ndays;
	private String  patterns,jobs,days;
	public PhaseWritable() {
		
	}

	public PhaseWritable(long duration, String patterns, long npatterns, String jobs,long njobs,
			String days,long ndays) {
		this.duration = duration;
		this.npatterns = npatterns;
		this.njobs = njobs;
		this.ndays = ndays;
		this.patterns = patterns;
		this.jobs = jobs;
		this.days = days;
	}


	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public long getNpatterns() {
		return npatterns;
	}

	public void setNpatterns(long npatterns) {
		this.npatterns = npatterns;
	}

	public long getNjobs() {
		return njobs;
	}

	public void setNjobs(long njobs) {
		this.njobs = njobs;
	}

	public long getNdays() {
		return ndays;
	}

	public void setNdays(long ndays) {
		this.ndays = ndays;
	}

	public String getPatterns() {
		return patterns;
	}
    public List<String> getListPatterns(){
    	List<String> list=new ArrayList<String>();
    	String patterns[]=this.patterns.split(",");
    	for (int i = 0; i < patterns.length; i++) {
           list.add(patterns[i]);
        }
    	return list;
    }
	public void setPatterns(String patterns) {
		this.patterns = patterns;
	}

	public String getJobs() {
		return jobs;
	}

	public void setJobs(String jobs) {
		this.jobs = jobs;
	}

	public String getDays() {
		return days;
	}

	public void setDays(String days) {
		this.days = days;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
	
		duration = in.readLong();
		patterns = in.readUTF();
		npatterns = in.readLong();
		jobs = in.readUTF();
		njobs = in.readLong();
		days= in.readUTF();
		ndays= in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(duration);
		out.writeLong(npatterns);
		out.writeLong(njobs);
		out.writeLong(ndays);
		out.writeUTF(jobs);
		out.writeUTF(days);
		out.writeUTF(patterns);
		
	}

}
