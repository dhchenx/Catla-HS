package cn.edu.bjtu.cdh.catla.metrics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TuningMetricsTest {
	public static void main(String[] args) {
		
		String[] sizes=new String[] {"100","200","300","400","500"};
		
		for (int x=0;x<sizes.length;x++) {
		System.out.println("size = "+sizes[x]);
		
		String metric_file="E:\\CatlaHS\\数据结果\\不同输入数据集的分析\\history-"+sizes[x]+"\\timecost_wordcount.csv";
		
		ArrayList<String> arrayList = new ArrayList<>();
		
		List<Long> timestamp_list=new ArrayList<Long>();
		List<Long> jobtime_list=new ArrayList<Long>();
		try {
			FileReader fr = new FileReader(metric_file);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			
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
			
			System.out.println("tuning time:"+(max_time-min_time));
			
			long max_jobtime=-1;
			long min_jobtime=Long.MAX_VALUE;
			for(int i=0;i<jobtime_list.size();i++) {
				if(jobtime_list.get(i)>max_jobtime)
					max_jobtime=jobtime_list.get(i);
				if(jobtime_list.get(i)<min_jobtime)
					min_jobtime=jobtime_list.get(i);
			}
			
			System.out.println("job time difference:"+(max_jobtime-min_jobtime));
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		}
		
	}
}
