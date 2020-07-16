package cn.edu.bjtu.cdh.catla.tuning;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import cn.edu.bjtu.cdh.catla.task.HadoopEnv;
import cn.edu.bjtu.cdh.catla.task.HadoopProject;
import cn.edu.bjtu.cdh.catla.task.HadoopTask;
import cn.edu.bjtu.cdh.catla.task.SparkApp;
import cn.edu.bjtu.cdh.catla.task.SparkLog;
import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;
import cn.edu.bjtu.cdh.catla.utils.UnicodeReader;

public class HadoopTuning {

	private String projectFolder;

	public HadoopTuning(String projectFolder) {
		this.setProjectFolder(projectFolder);
	}

	public Map<String, String> getMap(List<String> lines) {
		Map<String, String> map = new HashMap<String, String>();

		for (String s : lines) {
			if(s.trim().isEmpty())
				continue;
			if(!s.contains("="))
				continue;
			String key=s.substring(0,s.indexOf("=")).trim();
			String rest=s.substring(s.indexOf("=")+1).trim();
			
			if (!key.isEmpty())
				map.put(key, rest);
		}
		return map;
	}

	public static List<String> readFileByLines(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		List<String> lines = new ArrayList<String>();
		try {

			UnicodeReader read = new UnicodeReader(new FileInputStream(file), "UTF-8");
			reader = new BufferedReader(read);

			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
				System.out.println(tempString);
				if (!tempString.trim().isEmpty() && !tempString.startsWith("#"))
					lines.add(tempString);
				line++;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return lines;
	}

	public String getRange(String str, String splitLeft, String splitRight) {
		if (str.contains(splitLeft) && str.contains(splitRight)) {
			int b_i = str.indexOf(splitLeft);
			int e_i = str.indexOf(splitRight);

			String trace_id = str.substring(b_i + 1, e_i);

			return trace_id;

		} else

			return str;
	}

	private List<TuningParameter> params;
	
	private Map<String,String> aliasMap;

	public void createHadoopOtherArgPairs(String[] parameters) {
		
		aliasMap=new HashMap<String,String>();
		
		List<String> pSets = new ArrayList<String>();
		List<String> pTSets = new ArrayList<String>();
		List<List<String>> valueSets = new ArrayList<List<String>>();

		for (int i = 0; i < params.size(); i++) {
			TuningParameter tp = params.get(i);
			for (int j = 0; j < parameters.length; j++) {
				if (parameters[j].equals(tp.getName())) {
					pSets.add(tp.getName());
					pTSets.add(tp.getNumberType());
					if (tp.getType().equals(TuningParameter.RANGE)) {
						List<String> vs = new ArrayList<String>();
						for (double k = tp.getMin(); k <= tp.getMax(); k += tp.getStep()) {
							String new_k=k+"";
							if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
								new_k=new DecimalFormat("#.00").format(k);
							}else if(tp.getNumberType().equals(TuningParameter.INT)) {
								new_k=new DecimalFormat("#").format(k);
							}
							vs.add(new_k + "");
						}
						valueSets.add(vs);
						
					}
					if (tp.getType().equals(TuningParameter.ARRAY)) {
						List<String> vs = new ArrayList<String>();
						for (int k = 0; k < tp.getValueSet().length; k++) {
							String v=tp.getValueSet()[k];
							
							if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
								v=new DecimalFormat("#.00").format(Double.parseDouble(v));
							}else if(tp.getNumberType().equals(TuningParameter.INT)) {
								v=new DecimalFormat("#").format(Integer.parseInt(v));
							}else if(tp.getNumberType().equals(TuningParameter.STRING)) {
								v=tp.getValueSet()[k]+"";
							}else {
								v=tp.getValueSet()[k]+"";
							}
							
							vs.add(v);
						}
						valueSets.add(vs);
						if(tp.getAlias()!=null&&tp.getAlias().length!=0)
							for(int k=0;k<tp.getAlias().length;k++)
									aliasMap.put(tp.getValueSet()[k], tp.getAlias()[k]);
						
					}
					if (tp.getType().equals(TuningParameter.FIXED_VALUE)) {
						List<String> vs = new ArrayList<String>();
						
						String v=tp.getValue();
						
						if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
							v=new DecimalFormat("#.00").format(Double.parseDouble(v));
						}else if(tp.getNumberType().equals(TuningParameter.INT)) {
							v=new DecimalFormat("#").format(Integer.parseInt(v));
						}else if(tp.getNumberType().equals(TuningParameter.STRING)) {
							v=v+"";
						}else {
							v=v+"";
						}
						
						vs.add(v);
						valueSets.add(vs);
					}

				}
			}
		}

		int start = 0;
		
		
		List<String> tempList = new ArrayList<String>();
		for (int i = 0; i < valueSets.get(start).size(); i++) {
			
			String line = "-D " + pSets.get(start) + "=" + valueSets.get(start).get(i) + " ";
			tempList.add(line);
			
		}

		for (start = 0; start < pSets.size() - 1; start++) {
			List<String> tempList2 = new ArrayList<String>();
			for (int i = 0; i < tempList.size(); i++) {
				for (int j = 0; j < valueSets.get(start + 1).size(); j++) {
					String line = tempList.get(i);
					line += "-D " + pSets.get(start + 1) + "=" + valueSets.get(start+1).get(j) + " ";
					tempList2.add(line);
				}
			}
			tempList=tempList2;
		}
		
		System.out.println("--all parameters--");
		for(int i=0;i<tempList.size();i++) {
			
			System.out.println(tempList.get(i));
		}
		
		 this.setOtherArgLists(tempList);
		 
	}
	
	public void createSparkOtherArgPairs(String[] parameters) {
		
		aliasMap=new HashMap<String,String>();
		
		List<String> pSets = new ArrayList<String>();
		List<String> pTSets = new ArrayList<String>();
		List<List<String>> valueSets = new ArrayList<List<String>>();

		for (int i = 0; i < params.size(); i++) {
			TuningParameter tp = params.get(i);
			for (int j = 0; j < parameters.length; j++) {
				if (parameters[j].equals(tp.getName())) {
					pSets.add(tp.getName());
					pTSets.add(tp.getNumberType());
					if (tp.getType().equals(TuningParameter.RANGE)) {
						List<String> vs = new ArrayList<String>();
						for (double k = tp.getMin(); k <= tp.getMax(); k += tp.getStep()) {
							String new_k=k+"";
							if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
								new_k=new DecimalFormat("#.00").format(k);
							}else if(tp.getNumberType().equals(TuningParameter.INT)) {
								new_k=new DecimalFormat("#").format(k);
							}
							vs.add(new_k + "");
						}
						valueSets.add(vs);
						
					}
					if (tp.getType().equals(TuningParameter.ARRAY)) {
						List<String> vs = new ArrayList<String>();
						for (int k = 0; k < tp.getValueSet().length; k++) {
							String v=tp.getValueSet()[k];
							
							if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
								v=new DecimalFormat("#.00").format(Double.parseDouble(v));
							}else if(tp.getNumberType().equals(TuningParameter.INT)) {
								v=new DecimalFormat("#").format(Integer.parseInt(v));
							}else if(tp.getNumberType().equals(TuningParameter.STRING)) {
								v=tp.getValueSet()[k]+"";
							}else {
								v=tp.getValueSet()[k]+"";
							}
							
							vs.add(v);
						}
						valueSets.add(vs);
						if(tp.getAlias()!=null&&tp.getAlias().length!=0)
							for(int k=0;k<tp.getAlias().length;k++)
									aliasMap.put(tp.getValueSet()[k], tp.getAlias()[k]);
						
					}
					if (tp.getType().equals(TuningParameter.FIXED_VALUE)) {
						List<String> vs = new ArrayList<String>();
						
						String v=tp.getValue();
						
						if(tp.getNumberType().equals(TuningParameter.FLOAT)) {
							v=new DecimalFormat("#.00").format(Double.parseDouble(v));
						}else if(tp.getNumberType().equals(TuningParameter.INT)) {
							v=new DecimalFormat("#").format(Integer.parseInt(v));
						}else if(tp.getNumberType().equals(TuningParameter.STRING)) {
							v=v+"";
						}else {
							v=v+"";
						}
						
						vs.add(v);
						valueSets.add(vs);
					}

				}
			}
		}

		int start = 0;
		
		
		List<String> tempList = new ArrayList<String>();
		for (int i = 0; i < valueSets.get(start).size(); i++) {
			
			String line = "--" + pSets.get(start) + "=" + valueSets.get(start).get(i) + " ";
			tempList.add(line);
			
		}

		for (start = 0; start < pSets.size() - 1; start++) {
			List<String> tempList2 = new ArrayList<String>();
			for (int i = 0; i < tempList.size(); i++) {
				for (int j = 0; j < valueSets.get(start + 1).size(); j++) {
					String line = tempList.get(i);
					line += "--" + pSets.get(start + 1) + "=" + valueSets.get(start+1).get(j) + " ";
					tempList2.add(line);
				}
			}
			tempList=tempList2;
		}
		
		System.out.println("--all parameters--");
		for(int i=0;i<tempList.size();i++) {
			
			System.out.println(tempList.get(i));
		}
		
		 this.setOtherArgLists(tempList);
		 
	}

	public Map<String, String> getAliasMap() {
		return aliasMap;
	}

	public void setAliasMap(Map<String, String> aliasMap) {
		this.aliasMap = aliasMap;
	}

	private List<String> otherArgLists;

	public void loadParameters() {

		params = new ArrayList<TuningParameter>();

		String tuningFolder = this.getProjectFolder() + "/tuning";

		if (!new File(tuningFolder).exists())
			new File(tuningFolder).mkdirs();

		File[] pconfigs = new File(tuningFolder).listFiles();

		if (pconfigs != null && pconfigs.length > 0) {

			for (int i = 0; i < pconfigs.length; i++) {
				if (pconfigs[i].getName().startsWith("params_")) {

					Map<String, String> ps = getMap(readFileByLines(pconfigs[i].getAbsolutePath()));
					Set<String> keys = ps.keySet();
					for (String key : keys) {
						System.out.println("Value of " + key + " is: " + ps.get(key));
						String value = ps.get(key);
						TuningParameter tp = new TuningParameter();
						
						if(!key.startsWith("$")) {
							tp.setTarget("OtherArgs");
						}else {
							tp.setTarget(key);
						}
						
						tp.setName(key);
						//number type
						if (value.startsWith("[")) {
							String nType=TuningParameter.INT;
							boolean setDefault=false;
							if(value.contains(":")) {
								String[] fs=value.split(":");
								value=fs[0].trim();
								nType=fs[1].trim();
								if(fs.length>=3) {
									tp.setDefaultValue(fs[2]);
									setDefault=true;
								}
							}
							
							
							tp.setType(TuningParameter.RANGE);
							tp.setNumberType(nType);

							String ranges = this.getRange(value, "[", "]");
							double left = Double.parseDouble(ranges.split(",")[0].trim());
							double right = Double.parseDouble(ranges.split(",")[1].trim());
							// System.out.println(value);
							String step_str = value.substring(value.lastIndexOf(",") + 1);
							// System.out.println(step_str);
							double step = Double.parseDouble(step_str.trim());

							tp.setMax(right);
							tp.setMin(left);
							tp.setStep(step);
							
							if(!setDefault) {
								tp.setDefaultValue(""+((tp.getMin()+tp.getMax())/2));
							}
							

							System.out.println("left: " + left + ", right: " + right + ", step: " + step);

							//set type
						} else if (value.startsWith("{")) {
							String vtype=TuningParameter.STRING;
							boolean setDefault=false;
							if(value.contains(":")) {
								String[] fs=value.split(":");
								value=fs[0].trim();
								vtype=fs[1].trim();
								if(fs.length>=3) {
									tp.setDefaultValue(fs[2]);
									setDefault=true;
								}
							}
							
							
							
							tp.setType(TuningParameter.ARRAY);
							tp.setNumberType(vtype);
							String[] ranges = this.getRange(value, "{", "}").split(",");
							
							System.out.println(value);

							tp.setMax(-1);
							tp.setMin(-1);
							tp.setStep(-1);
							tp.setValueSet(ranges);
							
							if(value.contains("<")&&value.contains(">")) {
								String[] alias_list = this.getRange(value, "<", ">").split(",");
								tp.setAlias(alias_list);
							}
							
							if(!setDefault) {
								tp.setDefaultValue(tp.getValueSet()[0]);
							}
							
						} else {//fixed value type
							
							String vtype=TuningParameter.STRING;
							boolean setDefault=false;
							if(value.contains(":")) {
								String[] fs=value.split(":");
								value=fs[0].trim();
								vtype=fs[1].trim();
								if(fs.length>=3) {
									tp.setDefaultValue(fs[2]);
									setDefault=true;
								}
							}
							
							tp.setType(TuningParameter.FIXED_VALUE);
							tp.setNumberType(vtype);
							tp.setValue(value + "");
							if(value.contains("<")&&value.contains(">")) {
								String alias_list = this.getRange(value, "<", ">");
								tp.setAlias(new String[] {alias_list});
							}
							if(!setDefault) {
								tp.setDefaultValue(tp.getValue());
							}
						}

						params.add(tp);
					}

				}
			}

		}

	}
	
	public Map<String,String> obtainHadoopJobArgs(String line){
		
		Map<String,String> map=new HashMap<String,String>();
		
		String[] fs=line.split("-D");
		String otherArgs="";
		for(int i=0;i<fs.length;i++) {
			fs[i]=fs[i].trim();
			if(fs[i].isEmpty())
				continue;
			
			if(!fs[i].contains("="))
				continue;
			
			String key=fs[i].substring(0,fs[i].indexOf("=")).trim();
			String value=fs[i].substring(fs[i].indexOf("=")+1).trim();
			if(key.startsWith("$")) {
				map.put(key.replace("$", ""),value.trim());
			}else {
				otherArgs+="-D "+fs[i]+" ";
				map.put(key.trim(),value.trim());
			}
		}
		
		otherArgs=otherArgs.trim();
		
		map.put("OtherArgs", otherArgs);
		
		return map;
		
	}
	
public Map<String,String> obtainSparkJobArgs(String line){
		
		Map<String,String> map=new HashMap<String,String>();
		
		String[] fs=line.split(" ");
		String otherArgs="";
		for(int i=0;i<fs.length;i++) {
			fs[i]=fs[i].trim();
			if(fs[i].isEmpty())
				continue;
			
			if(!fs[i].contains("="))
				continue;
			
			String key=fs[i].substring(0,fs[i].indexOf("=")).trim();
			String value=fs[i].substring(fs[i].indexOf("=")+1).trim();
			
			map.put(key.replace("$", ""),value.trim());
			
		}
		
		otherArgs=otherArgs.trim();
		
		map.put("OtherArgs", otherArgs);
		
		return map;
		
	}

	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}
	
	public static void writeFile(String path, String content) {

		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(path));
			out.write(content);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static void deleteDirectoryRecursion(Path path) throws IOException {
		  if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
		    try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
		      for (Path entry : entries) {
		        deleteDirectoryRecursion(entry);
		      }
		    }
		  }
		  Files.delete(path);
		}

	public static HadoopEnv MapToEnv(Map<String, String> map) {
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost(map.get("MasterHost"));
		he.setMasterPassword(map.get("MasterPassword"));
		he.setMasterPort(Integer.parseInt(map.get("MasterPort")));
		he.setMasterUser(map.get("MasterUser"));
		he.setHadoopBin(map.get("HadoopBin"));
		he.setAppRoot(map.get("AppRoot"));
		if(map.keySet().contains("SparkUrl"))
		{
			he.setSparkUrl(map.get("SparkUrl"));
		}
		if(map.containsKey("AppType"))
			he.setAppType(map.get("AppType"));
		else 
			he.setAppType("Hadoop");
		return he;
	}
	
	public static void main(String[] args) throws IOException {

		if(args.length==0) {
		args = new String[] { 
				"-dir", "C:/Users/douglaschan/Desktop/大数据平台/tuning_reducejoin",
				"-clean", "true", 
				"-group", "reducejoin", 
				"-upload","false",
				"-uploadjar","false",
				"-continue","true"
			};
		}
		
		

		URL rootUrl = HadoopTask.class.getProtectionDomain().getCodeSource().getLocation();
		String jarFolder = URLDecoder.decode(rootUrl.getPath(), "utf-8");

		if (jarFolder.endsWith(".jar")) {
			jarFolder = jarFolder.substring(0, jarFolder.lastIndexOf("/"));
			if (jarFolder.contains(":"))
				jarFolder = jarFolder.substring(1);

		} else {
			jarFolder = "";
		}

		Map<String, String> options = getOptionMap(args);

		String dirFolder = options.get("-dir");

		if (!jarFolder.isEmpty()) {
			if (!options.get("-dir").contains(":") && !options.get("-dir").startsWith("/")) {
				dirFolder = jarFolder + "/" + options.get("-dir");
			}
		}
		
		if (!new File(dirFolder).exists()) {
			System.out.println("Folder: " + dirFolder + " does not exist.");
			return;
		}
		
		if(!new File(dirFolder+"/_hproj.txt").exists()) {
			System.out.println("It is not a valid project folder!");
			return;
		}
		
		
		
		boolean is_clean=false;
		
		if(options.containsKey("-clean")) {
			
			is_clean=Boolean.parseBoolean(options.get("-clean"));
			
		}
		
	    boolean is_continue=false;
		
		if(options.containsKey("-continue")) {
			
			is_continue=Boolean.parseBoolean(options.get("-continue"));
			
		}
		
	    boolean is_upload=true;
		if(options.containsKey("-upload"))
			is_upload=Boolean.parseBoolean(options.get("-upload"));
		 
		 boolean is_uploadjar=true;
			if(options.containsKey("-uploadjar"))
				is_uploadjar=Boolean.parseBoolean(options.get("-uploadjar"));
		
		
		String groupId="default";
		if(options.containsKey("-group"))
			groupId=options.get("-group");
		
		if(!is_continue&&is_clean) {
			if(new File(dirFolder+"/logs").exists())
			FileUtils.deleteDirectory(new File(dirFolder+"/logs"));
			if(new File(dirFolder+"/history").exists())
			FileUtils.deleteDirectory(new File(dirFolder+"/history"));
			if(new File(dirFolder+"/outputs").exists())
			FileUtils.deleteDirectory(new File(dirFolder+"/outputs"));
			if(new File(dirFolder+"/progress").exists())
				FileUtils.deleteDirectory(new File(dirFolder+"/progress"));
		}
	
		HadoopTuning htuning = new HadoopTuning(dirFolder);
		htuning.loadParameters();
		
		HadoopProject hp1 = HadoopProject.createInstance(new
				  File(dirFolder).getAbsolutePath());
		HadoopEnv he=MapToEnv(hp1.getEnvMaps().get(0));
		//support spark
		String appType=he.getAppType();
		
		String quickTuningCofigPath=dirFolder+"/tuning/current.txt";
		String[] useParameters=new String[] {};
		
		if(options.containsKey("-params")) {
			useParameters=options.get("-params").split(";");
		 }else {
			 System.out.println("using param settings from tuning/current.txt");
				if(new File(quickTuningCofigPath).exists()) {
					List<String> lines=readFileByLines(quickTuningCofigPath);
					useParameters=new String[lines.size()];
					for(int i=0;i<lines.size();i++)
						useParameters[i]=lines.get(i);
				}
		 }
		
		if(appType.equals("Spark")) {
			htuning.createSparkOtherArgPairs(useParameters);
		}else
		if(appType.equals("Hadoop")) {
			htuning.createHadoopOtherArgPairs(useParameters);
		}else {
			System.out.println("No valid platform name!");
		}
		
		String argStr="";
		for(int i=0;i<htuning.getOtherArgLists().size();i++) {
			argStr+=htuning.getOtherArgLists().get(i)+"\r\n";
		}
		
		String progressFolder=dirFolder+"/progress";
		if(!new File(progressFolder).exists())
			new File(progressFolder).mkdirs();
		
		writeFile(progressFolder+"/running_parameters.txt",argStr);
		
		List<String> spark_id_list=new ArrayList<String>();
		List<String> spark_log_list=new ArrayList<String>();
		
		if(htuning.getOtherArgLists()!=null&&htuning.getOtherArgLists().size()>0) {
			
			TuningLog tl=new TuningLog(dirFolder,htuning.getAliasMap());
			
			if(htuning.getAliasMap()!=null) {
				System.out.println("Alias Map for input parameter value: ");
				 for (Map.Entry<String,String> entry : htuning.getAliasMap().entrySet())  
				 {
					 System.out.println(entry.getKey()+"="+entry.getValue());
				 }
			}

			for(int i=0;i<htuning.getOtherArgLists().size();i++) {
				
			
				
				String otherArgs=htuning.getOtherArgLists().get(i);
				
				Map<String,String> currentParameters=null;
				
				if(appType.equals("Hadoop")) {
					currentParameters=htuning.obtainHadoopJobArgs(otherArgs);
				}else if (appType.equals("Spark")) {
					currentParameters=htuning.obtainSparkJobArgs(otherArgs);
				}
				
				System.out.println("Using current job's parameter list as below: ");
				 for (Map.Entry<String,String> entry : currentParameters.entrySet())  
				 {
					 System.out.println(entry.getKey()+"="+entry.getValue());
				 }
				
				boolean useOldTraceId=false; 
				String oldTraceId="";
				
				if(is_continue) {
					String progressFile= new File(dirFolder).getAbsolutePath()+ "/progress/running_number.txt";
					if(new File(progressFile).exists()) {
					String progressNum=Files.readAllLines(Paths.get(progressFile)).get(0).trim();
					  int pn=Integer.parseInt(progressNum);
					  if(i<pn) {
						  continue;
					  }else {
						  String currentTraceIdFile= new File(dirFolder).getAbsolutePath()+ "/progress/running_traceId.txt";
						  if(new File(currentTraceIdFile).exists()) {
							  oldTraceId=Files.readAllLines(Paths.get(currentTraceIdFile)).get(0).trim();
							  
							  useOldTraceId=true;
							  
						  }
					  }
					
					}
			
				}
				
				HadoopProject hp = HadoopProject.createInstance(new
						  File(dirFolder).getAbsolutePath(),currentParameters);
				
				if(useOldTraceId) {
					hp.setUseOldTraceId(true);
				}else {
					hp.setUseOldTraceId(false);
				}
				
				if(is_upload)
					hp.callUpload2HDFS();
				
				if(is_uploadjar)	  
					hp.callUploads();
				
				if(useOldTraceId) {
					hp.setOldTraceId(oldTraceId);
				}
				
				boolean flag=hp.callSubmit(false, true);
				
				//record the running parameters
				if(hp.getRunningTraceId()!=null) {
					String historyFolder=hp.getRootFolder()+"/history/log-"+hp.getRunningTraceId();
					
					if(!new File(historyFolder).exists())
						new File(historyFolder).mkdirs();
					
					String historyRunningParameterPath=historyFolder+"/running_parameters.txt";
					
					writeFile(historyRunningParameterPath,otherArgs);
					
					String historyIterationPath=historyFolder+"/iteration_"+(i+1)+"";
					writeFile(historyIterationPath,"");
					
					
				}
				
				System.out.println("#"+(i+1)+", Tuning for "+otherArgs+", "+flag);
				
				writeFile(progressFolder+"/running_number.txt",i+"");
				writeFile(progressFolder+"/running_traceId.txt",hp.getRunningTraceId()+"");
				
				if(flag) {
					if(appType.equals("Hadoop")) {
						long timecost=tl.getTimeCost(hp.getRunningTraceId());
						System.out.println("----------Hadoop jos's time cost="+timecost+"--------------");
						
					}else if (appType.equals("Spark")) {
						
						String historyFolder=hp.getRootFolder()+"/history/log-"+hp.getRunningTraceId();
						SparkLog sparkLog=new SparkLog(he);;
						String appId=sparkLog.obtainAppId(hp.getRunningTraceId());
						
						if(!new File(historyFolder).exists()) {
							new File(historyFolder).mkdirs();
						}
						
						 writeFile(historyFolder+"/spark.app.id",appId);
						 
						 spark_id_list.add(appId);
						 spark_log_list.add(hp.getRunningTraceId());
						 
						 sparkLog.downloadSparkLog2Local_Indiv(appId, historyFolder);
						
						 sparkLog.analyzeLog(historyFolder+"/"+appId);
						 System.out.println("----------Spark job's time cost="+sparkLog.getTimeAppCost()+"--------------");
					}
					
				}
				
			}
			
			//summarize hadoop logs
			if(appType.equals("Hadoop")) {
				
			System.out.println("Summarizing the results...");
			
			
			tl.exportToCSV(groupId);
			
			}else {
				//summarize spark logs
				//export
				
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(new File(dirFolder+"/history" + "/timecost_" + groupId + ".csv"))));

				 
				String pl0=CatlaFileUtils.readFile(dirFolder+"/history/log-"+spark_log_list.get(0)+"/running_parameters.txt");
				 String[] ps0=pl0.replace("\n", "").trim().split(" ");
				 String header="";
				 for(int j=0;j<ps0.length;j++) {
					 String[] kv=ps0[j].split("=");
					 if(kv.length<2)continue;
					 if(j!=ps0.length-1)
						 header+=kv[0]+"\t";
					 else
						 header+=kv[0];
				 }
				 
				header = header.trim();
				
				String jobHeader = "TimeStamp\t"+ "Order" + "\t" + header + "\t" + "totalTimeCost";
				bw.write(jobHeader.trim() + "\n");
				
				SparkLog sparkLog=new SparkLog(he);
				for (int i=0;i<spark_id_list.size();i++) {
					String appId=spark_id_list.get(i);
					 String historyFolder=dirFolder+"/history/log-"+spark_log_list.get(i);
					 sparkLog.downloadSparkLog2Local_Indiv(spark_id_list.get(i), historyFolder);
						
					 sparkLog.analyzeLog(historyFolder+"/"+appId);
					 
					 String pl=CatlaFileUtils.readFile(historyFolder+"/running_parameters.txt");
					 String[] ps=pl.replace("\n", "").trim().split(" ");
					 String vs="";
					 for(int j=0;j<ps.length;j++) {
						 String[] kv=ps[j].split("=");
						 if(kv.length<2)continue;
						 if(j!=ps.length-1)
							 vs+=kv[1]+"\t";
						 else
							 vs+=kv[1];
					 }
					 
					 bw.write(spark_log_list.get(i)+"\t"+(i+1)+"\t"+vs+"\t"+sparkLog.getTimeAppCost()+"\n");
					 
					 System.out.println("----------Spark job's time cost="+sparkLog.getTimeAppCost()+"--------------");
					
					 
				}
				
				bw.close();
				
			}
			
			
			
		}
		
		System.out.println("Tuning Finished!");

		System.exit(0);
		
	}

	public String getProjectFolder() {
		return projectFolder;
	}

	public void setProjectFolder(String projectFolder) {
		this.projectFolder = projectFolder;
	}

	public List<TuningParameter> getParams() {
		return params;
	}

	public void setParams(List<TuningParameter> params) {
		this.params = params;
	}

	public List<String> getOtherArgLists() {
		return otherArgLists;
	}

	public void setOtherArgLists(List<String> otherArgLists) {
		this.otherArgLists = otherArgLists;
	}

}
