package cn.edu.bjtu.cdh.catla.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TuningMetrics {
	
	private String project_folder;

	public TuningMetrics(String project_folder) {
		this.project_folder = project_folder;
	}
	
	public String getProjectName() {
		return new File(project_folder).getName();
	}
	
	private List<Long> timestamp_list = new ArrayList<Long>();
	private List<Long> jobtime_list = new ArrayList<Long>();
	private long max_time = -1;
	private long min_time = Long.MAX_VALUE;
	private long max_jobtime = -1;
	private long min_jobtime = Long.MAX_VALUE;
	private int max_jobtime_id = -1;
	private int min_jobtime_id = -1;
	
	private long max_tuning_time = 0;
	private long min_tuning_time = Long.MAX_VALUE;
	private int max_tuning_id = -1;
	private int min_tuning_id = -1;
	
	// total tuning time
	private long total_tuning_time;

	// average tuning time
	private double avg_tuning_time;
	
	private double avg_job_time = 0;
	
	private long diff_tuning_time;
	
	public long getTotalTuningTime() {
		return total_tuning_time;
	}
	
	public double getAvgTuningTime() {
 
		return Double.parseDouble(new java.text.DecimalFormat("#.00").format(avg_tuning_time));
	}
	
	public double getAvgJobTime() {
		return Double.parseDouble(new java.text.DecimalFormat("#.00").format(avg_job_time));
	}
	
	public long getDifferenceOfTuningTime() {
		return diff_tuning_time;
	}
	
	public int getNumberOfTunings() {
		return timestamp_list.size();
	}
	
	public long getMaxTimeStamp() {
		return max_time;
	}
	
	public long getMinTimeStamp() {
		return min_time;
	}
	
	public long getMaxJobTime() {
		return max_jobtime;
	}
	
	public long getMinJobTime() {
		return min_jobtime;
	}
	
	public int getMaxJobTimeId() {
		return max_jobtime_id;
	}
	
	public int getMinJobTimeId() {
		return min_jobtime_id;
	}
	
	public long getMaxTuningTime() {
		return max_tuning_time;
	}
	
	public long getMinTuningTime() {
		return min_tuning_time;
	}
	
	public long getMaxTuningId() {
		return max_tuning_id;
	}
	
	public long getMinTuningId() {
		return min_tuning_id;
	}
	
	

	public void extract(String group) {

		String metric_file = this.project_folder + "\\history\\timecost_"+group+".csv";

		ArrayList<String> arrayList = new ArrayList<>();

		
		try {
			FileReader fr = new FileReader(metric_file);
			BufferedReader bf = new BufferedReader(fr);
			String str;

			while ((str = bf.readLine()) != null) {
				arrayList.add(str);
				// System.out.println(str);
				if (str.startsWith("TimeStamp"))
					continue;

				String[] vs = str.split("\t");
				timestamp_list.add(Long.parseLong(vs[0]));
				jobtime_list.add(Long.parseLong(vs[4]));

			}
			bf.close();
			fr.close();

			// find maximum and minimum value of job time
		
			for (int i = 0; i < timestamp_list.size(); i++) {
				if (timestamp_list.get(i) > max_time)
					max_time = timestamp_list.get(i);
				if (timestamp_list.get(i) < min_time)
					min_time = timestamp_list.get(i);
			}

			

			for (int i = 0; i < jobtime_list.size(); i++) {
				if (jobtime_list.get(i) > max_jobtime) {
					max_jobtime = jobtime_list.get(i);
					max_jobtime_id = i;
				}
				if (jobtime_list.get(i) < min_jobtime) {
					min_jobtime = jobtime_list.get(i);
					min_jobtime_id = i;
				}
			}
			// total tuning time
			long total_tuning_time = max_time - min_time + jobtime_list.get(jobtime_list.size() - 1);

			// average tuning time
			double avg_tuning_time = total_tuning_time * 1.0 / timestamp_list.size();

			// average job time
		
			for (int i = 0; i < jobtime_list.size(); i++) {
				avg_job_time += jobtime_list.get(i);
			}
			avg_job_time = avg_job_time * 1.0 / jobtime_list.size();

			// max tuning time
		

			for (int i = 0; i < timestamp_list.size() - 2; i++) {
				long len_tuning_time = timestamp_list.get(i + 1) - timestamp_list.get(i);
				if (len_tuning_time > max_tuning_time) {
					max_tuning_time = len_tuning_time;
					max_tuning_id = i;
				}
				if (len_tuning_time < min_tuning_time) {
					min_tuning_time = len_tuning_time;
					min_tuning_id = i;
				}
			}

			long diff_tuning_time = max_tuning_time - min_tuning_time;
			
			this.total_tuning_time=total_tuning_time;
			this.avg_tuning_time=avg_tuning_time;
			this.diff_tuning_time=diff_tuning_time;

			/*
			System.out.println("Summary of metrics for "+this.project_folder+": ");
			System.out.println("total tuning time: " + total_tuning_time);
			System.out.println("average tuning time: " + avg_tuning_time);
			System.out.println("average job time: " + avg_job_time);
			System.out.println("min jobtime id: " + min_jobtime_id);
			System.out.println("max jobtime id: " + max_jobtime_id);
			System.out.println("max tuning time: " + max_tuning_time);
			System.out.println("min tuning time: " + min_tuning_time);
			System.out.println("diff tuning time: " + diff_tuning_time);
			System.out.println("min tuning id: " + min_tuning_id);
			System.out.println("max tuning id: " + max_tuning_id);

			System.out.println();
			*/
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	public static void main(String[] args) {
		String[] projects=new String[] {"tuning_reducejoin","tuning_terasort","tuning_wordcount"};
		for(int i=0;i<projects.length;i++) {
			TuningMetrics tm=new TuningMetrics("E:\\CatlaHS\\"+projects[i]);
			tm.extract("wordcount");
			System.out.println(tm.getProjectName()+"\t"+tm.getMinTuningTime()+"\t"+tm.getMinJobTime());
		}
		
	}
	
	
}
