package cn.edu.bjtu.cdh.catla.metrics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TuningOneMetricTest {
	public static void main(String[] args) {
		
		String[] sizes=new String[] {""};
		
		for (int x=0;x<sizes.length;x++) {
		System.out.println("size = "+sizes[x]);
		
		String metric_file="E:\\CatlaHS\\tuning_reducejoin\\history"+sizes[x]+"\\timecost_wordcount.csv";
		
		ArrayList<String> arrayList = new ArrayList<>();
		
		List<Long> timestamp_list=new ArrayList<Long>();
		List<Long> jobtime_list=new ArrayList<Long>();
		
		try {
			
			FileReader fr = new FileReader(metric_file);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			// °´ÐÐ¶ÁÈ¡×Ö·û´®
			while ((str = bf.readLine()) != null) {
				arrayList.add(str);
				//System.out.println(str);
				if(str.startsWith("TimeStamp"))
					continue;
				
				String[] vs=str.split("\t");
				timestamp_list.add(Long.parseLong(vs[0]));
				jobtime_list.add(Long.parseLong(vs[4]));
				
			}
			bf.close();
			fr.close();
			
			// find maximum and minimum value of job time
			long max_time=-1;
			long min_time=Long.MAX_VALUE;
			for(int i=0;i<timestamp_list.size();i++) {
				if(timestamp_list.get(i)>max_time)
					max_time=timestamp_list.get(i);
				if(timestamp_list.get(i)<min_time)
					 min_time=timestamp_list.get(i);
			}
			
			long max_jobtime=-1;
			long min_jobtime=Long.MAX_VALUE;
			int max_jobtime_id=-1;
			int min_jobtime_id=-1;
			
			for(int i=0;i<jobtime_list.size();i++) {
				if(jobtime_list.get(i)>max_jobtime) {
					max_jobtime=jobtime_list.get(i);
					max_jobtime_id=i;
				}
				if(jobtime_list.get(i)<min_jobtime) {
					min_jobtime=jobtime_list.get(i);
					min_jobtime_id=i;
				}
			}
			// total tuning time
			long total_tuning_time=max_time-min_time+jobtime_list.get(jobtime_list.size()-1);
			
			// average tuning time
			double avg_tuning_time=total_tuning_time*1.0/timestamp_list.size();
			
			// average job time
			double avg_job_time=0;
			for(int i=0;i<jobtime_list.size();i++) {
				avg_job_time+=jobtime_list.get(i);
			}
			avg_job_time=avg_job_time*1.0/jobtime_list.size();
			
			//max tuning time
			long max_tuning_time=0;
			long min_tuning_time=Long.MAX_VALUE;
			int max_tuning_id=-1;
			int min_tuning_id=-1;
			
			for(int i=0;i<timestamp_list.size()-2;i++) {
				long len_tuning_time=timestamp_list.get(i+1)-timestamp_list.get(i);
				if(len_tuning_time>max_tuning_time) {
					max_tuning_time=len_tuning_time;
					max_tuning_id=i;
				}
				if(len_tuning_time<min_tuning_time) {
					min_tuning_time=len_tuning_time;
					min_tuning_id=i;
				}
			}
			
			long diff_tuning_time=max_tuning_time-min_tuning_time;
			
			System.out.println("total tuning time: "+total_tuning_time);
			System.out.println("average tuning time: "+avg_tuning_time);
			System.out.println("average job time: "+avg_job_time);
			System.out.println("min jobtime id: "+min_jobtime_id);
			System.out.println("max jobtime id: "+max_jobtime_id);
			System.out.println("max tuning time: "+max_tuning_time);
			System.out.println("min tuning time: "+min_tuning_time);
			System.out.println("diff tuning time: "+diff_tuning_time);
			System.out.println("min tuning id: "+min_tuning_id);
			System.out.println("max tuning id: "+max_tuning_id);
			
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		}
		
	}
}
