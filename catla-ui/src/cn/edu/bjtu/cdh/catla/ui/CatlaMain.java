package cn.edu.bjtu.cdh.catla.ui;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.experimental.chart.swt.ChartComposite;

import cn.edu.bjtu.cdh.catla.CatlaRunner;
import cn.edu.bjtu.cdh.catla.tuning.TuningLog;
import cn.edu.bjtu.cdh.catla.utils.UnicodeReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.ImageIcon;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.wb.swt.SWTResourceManager;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.swt.custom.CLabel;

public class CatlaMain {

	protected Shell shell;
	private Text txtProjectFolder;
	private Display display;
	private boolean isStop=false;
	Thread taskThread=null;
	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			CatlaMain window = new CatlaMain();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
	    display = Display.getDefault();
		createContents();
		shell.open();
		shell.layout();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}
	
	public static String readFile(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		String lines="";
		try {

			UnicodeReader read = new UnicodeReader(new FileInputStream(file), "UTF-8");
			reader = new BufferedReader(read);

			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
 
				lines+=tempString+"\n";
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

	
	private class LogTaskRunnable implements Runnable{
		   
	    private String projectFolder;
	    private TuningLog tlog=null;
	  
	    public LogTaskRunnable(String projectFolder){
	    	this.projectFolder=projectFolder;
	        tlog=new TuningLog(projectFolder);
	        traceList=new ArrayList<String>();
	    }
	    
	    private List<String> traceList;
	    private List<String> headerList;
	    public List<String> getNewLogs(){
	    	List<String> newTraceList=new ArrayList<String>();
	    	  File[] logFiles=new File(projectFolder+"/history").listFiles();
	    	   if(logFiles!=null) {
		        	for(int i=0;i<logFiles.length;i++) {
		        		if(logFiles[i].isDirectory()) {
			        		String[] fileName=logFiles[i].getName().split("-");
			        		String traceId=fileName[1];
			        		 File[] subfiles=new File(projectFolder+"/history/log-"+traceId).listFiles();
			        		 long timeCost=-1;
			 	        		long iteration=-1;
			 	        		boolean has_rumen_log=false;
			                		for(File f: subfiles) {
				        				if(f.getName().startsWith("cost_")) {
				        					timeCost=Long.parseLong(f.getName().split("_")[1]);
				        				}
				        				if(f.getName().startsWith("iteration_")) {
				        					iteration=Long.parseLong(f.getName().split("_")[1]);
				        				}
				        				if(f.getName().startsWith("rumen_")) {
				        					has_rumen_log=true;
				        				}
				        			}
			        		
			        		if(has_rumen_log&&!traceList.contains(traceId)&&timeCost!=-1&&iteration!=-1) {
			        			traceList.add(traceId);
			        			newTraceList.add(traceId);
			        		}
		        		}
		        	}
	    	   }
		      return newTraceList;
	    }
	    
	    public void run(){

	    	 Timer timer = new Timer();
	         timer.schedule(new TimerTask() {
	             public void run() {
	                 System.out.println("detecting new logs...");
	                 
	                 List<String> newTraceList=getNewLogs();
	                 
	                 for(int i=0;i<newTraceList.size();i++) {
	                	 System.out.println("new: "+newTraceList.get(i));
	                	 
	                	tlog.exportToLogFolder(newTraceList.get(i));
	                	
	                	String statFilePath=projectFolder+"/history/log-"+newTraceList.get(i)+"/stat_timecost.csv";
	                	
	                	if(new File(statFilePath).exists()) {
	                		
	                		String statContent=readFile(statFilePath).trim();
	                		
	                		Map<String,String> dataMap=new HashMap<String,String>();
	                		String[] ls=statContent.split("\n");
	                		String[] header=ls[0].trim().split("\t");
	                		String[] values=ls[1].trim().split("\t");
	                		for(int j=0;j<header.length;j++) {
	                			dataMap.put(header[j], values[j]);
	                		}
	                		
	                		 File[] subfiles=new File(projectFolder+"/history/log-"+newTraceList.get(i)).listFiles();
		                	 long timeCost=-1;
		 	        		long iteration=-1;
		                		for(File f: subfiles) {
			        				if(f.getName().startsWith("cost_")) {
			        					timeCost=Long.parseLong(f.getName().split("_")[1]);
			        				}
			        				if(f.getName().startsWith("iteration_")) {
			        					iteration=Long.parseLong(f.getName().split("_")[1]);
			        				}
			        			}
	                		
		                	final String iterationFinal=iteration+"";
	                	
	                		display.asyncExec(new Runnable() {

								@Override
								public void run() {
									String selectedField="totalTimeCost";
									if(coFields.getItemCount()>0&&coFields.getSelectionIndex()!=-1) {
			                			selectedField=coFields.getText();
			                		}
			                		
			                		String selectedValue=dataMap.get(selectedField);
			                		final String selectedFieldFinal=selectedField;
			                		final String seletedValueFinal=selectedValue;
			                		
			                		final String line_name=new File(projectFolder).getName();
			                		
									// TODO Auto-generated method stub
									dataset.addValue(Double.parseDouble(seletedValueFinal) , line_name+","+ selectedFieldFinal , iterationFinal );
								}
                				
                			});
	                		
	                	}
	                	
	                	 
	                	 /*
	                	 File[] subfiles=new File(projectFolder+"/history/log-"+newTraceList.get(i)).listFiles();
	                	 long timeCost=-1;
	 	        		long iteration=-1;
	                		for(File f: subfiles) {
		        				if(f.getName().startsWith("cost_")) {
		        					timeCost=Long.parseLong(f.getName().split("_")[1]);
		        				}
		        				if(f.getName().startsWith("iteration_")) {
		        					iteration=Long.parseLong(f.getName().split("_")[1]);
		        				}
		        			}
	                	 
	                		if(timeCost!=-1&&iteration!=-1) {
	                			
	                			final long timeCostFinal=timeCost;
	                			final long iterationFinal=iteration;
	                			display.asyncExec(new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub
										dataset.addValue( timeCostFinal , "optimizer" , ""+iterationFinal );
									}
	                				
	                			});
	                		
	                			
	                		}
	                		*/
	                	 
	                	 
	                	 
	                 }
	                	 
	                 
	             }
	         }, 5000, 3000);
	    	
	    }
		public String getProjectFolder() {
			return projectFolder;
		}
		public void setProjectFolder(String projectFolder) {
			this.projectFolder = projectFolder;
		}
	 
	}
	
	public String getJobHeaders() {
		return "TimeStamp\t"+ "Order" + "\t" + "totalTimeCost"+"\t"+"jobTimeCost"+"\t" +"avgMapTimeCost"+"\t"+"avgReduceTimeCost\tavgShuffleTimeCost\tavgSortTimeCost\tsetupTimeCost";
		
	}
	
	void updateArgs(Combo coTool,Combo coOptimizer ) {
		String template="";
		if(coTool.getText().equals("Hadoop Task")) {
			template="-tool task -dir {PROJECT_FOLDER}";
		}
		if(coTool.getText().equals("Hadoop Project")) {
			template="-tool project -dir {PROJECT_FOLDER} -task pipeline -download true -sequence true";
		}
		if(coTool.getText().equals("Hadoop Tuning")) {
			template="-tool tuning -dir {PROJECT_FOLDER} -clean true -upload true -uploadjar true";
		}
		if(coTool.getText().equals("Hadoop Optimizer")) {
			template="-tool optimizer -dir {PROJECT_FOLDER} -clean true -group wordcount -upload true -uploadjar true -maxinter 100 -optimizer BOBYQA -BOBYQA-initTRR 50 -BOBYQA-stopTRR 1.0E-4";
			int selectedIndexOfOptimizer=coOptimizer.getSelectionIndex();
			if(selectedIndexOfOptimizer==0) {
				template="-tool optimizer -dir {PROJECT_FOLDER} -clean true -group wordcount -upload true -uploadjar true -maxinter 100 -optimizer BOBYQA -BOBYQA-initTRR 50 -BOBYQA-stopTRR 1.0E-4";
			}
			if(selectedIndexOfOptimizer==1) {
				template="-tool optimizer -dir {PROJECT_FOLDER} -clean true -group wordcount -upload true -uploadjar true -maxinter 100 -optimizer Powell -powell-rel 1e-4 -powell-abs 1e-4";
			}
			if(selectedIndexOfOptimizer==2) {
				template="-tool optimizer -dir {PROJECT_FOLDER} -clean true -group wordcount -upload true -uploadjar true -maxinter 100 -optimizer CMAES -cmaes-sigma 20,0.4 -cmaes-ftol 10 -cmaes-pointtol 1e-1 -cmaes-expectedvalue 23 -cmaes-diagnoalonly 0 -cmaes-stopvalue 23 -cmaes-checkfeasiblepoint 0 -cmaes-maxeval 3000";
			}
			if(selectedIndexOfOptimizer==3) {
				template="-tool optimizer -dir {PROJECT_FOLDER} -clean true -group wordcount -upload true -uploadjar true -maxinter 100 -optimizer Simplex -simplex-rel 1e-4 -simplex-abs 1e-4";
			}
		}
		if(coTool.getText().equals("Log Aggregation")) {
			template="-tool log -dir {PROJECT_FOLDER}";
		}
		txtArg.setText(template);
	}
	
	JFreeChart lineChart;
	Combo coFields;

	/**
	 * Create contents of the window.
	 */
	protected void createContents() {
	 
		shell = new Shell();
		shell.setImage(SWTResourceManager.getImage(CatlaMain.class, "/cn/edu/bjtu/cdh/catla/ui/catla-logo.png"));
		
		shell.setSize(1020, 1033);
		shell.setText("Catla UI - Developed by Donghua Chen");
		
		txtProjectFolder = new Text(shell, SWT.BORDER);
		txtProjectFolder.setBounds(176, 112, 812, 30);
		
	
	
		 
	
		Button btnStop = new Button(shell, SWT.NONE);
		btnStop.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
			}
		});
		btnStop.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				 
						System.exit(0);

			}
		});
	 
		btnStop.setBounds(496, 21, 178, 34);
		btnStop.setText("Stop and Close");
		
		Group grpMoniter = new Group(shell, SWT.NONE);
		grpMoniter.setText("Log Aggregation");
		grpMoniter.setBounds(30, 196, 958, 503);
		
		Combo coOptimizer = new Combo(shell, SWT.NONE);
		coOptimizer.setItems(new String[] {"BOBYQA", "Powell", "CMAES", "Simplex"});
		coOptimizer.setBounds(597, 74, 189, 32);
		coOptimizer.select(0);
		
		Label lblOptimizer = new Label(shell, SWT.NONE);
		lblOptimizer.setBounds(489, 77, 90, 24);
		lblOptimizer.setText("Optimizer:");
		
		   lineChart = ChartFactory.createLineChart(
			         "Change of time cost over iteration",
			         "Iteration","Time cost",
			         createDataset(),
			         PlotOrientation.VERTICAL,
			         true,true,false);
			//  ChartPanel chartPanel = new ChartPanel( lineChart );
			 //   chartPanel.setPreferredSize( new java.awt.Dimension( 500 , 500 ) );
		   
	    	ChartComposite frame = new ChartComposite(grpMoniter, SWT.NONE, lineChart,
	                true);
	    	
	    	
	     
		   frame.setBounds(22, 32, 913, 401);
		   
		    coFields = new Combo(grpMoniter, SWT.NONE);
		    
		    
		   coFields.setBounds(22, 450, 207, 32);
		   formToolkit.adapt(coFields);
		   formToolkit.paintBordersFor(coFields);
		   
		   String[] data_fields=this.getJobHeaders().split("\t");
		   for(String s:data_fields) {
			   coFields.add(s);
		   }
		   coFields.setText("totalTimeCost");
		   
		   Label logoLabel = new Label(shell, SWT.NONE);
		   logoLabel.setAlignment(SWT.CENTER);
		   logoLabel.setBounds(30, 10, 120, 50);
		   logoLabel.setText("Catla");
		  Image logoImage=SWTResourceManager.getImage(CatlaMain.class, "/cn/edu/bjtu/cdh/catla/ui/catla-logo.png");
		  ImageData imgData=logoImage.getImageData();
		  Image newImage = new Image(null, imgData.scaledTo(120,
                 50));
		  
		   // logoImage=resize(logoImage,120,50);
		    logoLabel.setImage(newImage);
		    
		    Label lblCurrentFolder = new Label(shell, SWT.NONE);
		    lblCurrentFolder.setBounds(30, 115, 141, 24);
		    lblCurrentFolder.setText("Current Project:");
		    
		    Label lblTool = new Label(shell, SWT.NONE);
		    lblTool.setBounds(64, 77, 90, 24);
		    lblTool.setText("Catla Tool:");
		    
		    Combo coTool = new Combo(shell, SWT.NONE);
		    coTool.addSelectionListener(new SelectionAdapter() {
		    	@Override
		    	public void widgetSelected(SelectionEvent e) {
		    		updateArgs(coTool,coOptimizer);
		    	}
		    });
		    
		    coTool.setItems(new String[] {"Hadoop Task", "Hadoop Project", "Hadoop Tuning", "Hadoop Optimizer", "Log Aggregation"});
		    coTool.setBounds(176, 74, 286, 32);
		    coTool.select(3);
		    
		    Label lblArguments = new Label(shell, SWT.NONE);
		    lblArguments.setBounds(65, 151, 106, 24);
		    lblArguments.setText("Arguments:");
		    
		    txtArg = new Text(shell, SWT.BORDER);
		    txtArg.setBounds(178, 148, 810, 30);
		    
		   
		
		    
			Button btnStart = new Button(shell, SWT.NONE);
			btnStart.addSelectionListener(new SelectionAdapter() {
				@Override
				public void widgetSelected(SelectionEvent e) {
				}
			});
			btnStart.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseDown(MouseEvent e) {
					
					try {
						
						if(!new File(txtProjectFolder.getText()).exists()) {
							 MessageBox box = new MessageBox(shell,  SWT.OK);
							   box.setText("Tips");
							   box.setMessage("Please specify a valid folder");
							   box.open();
							   return;
						}
						
					   final int selectedIndex=coTool.getSelectionIndex();
						
						final String folderPath=txtProjectFolder.getText();
					 
						final String arg_str=txtArg.getText().replace("{PROJECT_FOLDER}",txtProjectFolder.getText());
						
				        taskThread=new Thread(new Runnable() {
							@Override
							public void run() {
								// TODO Auto-generated method stub
							
								
								
								String[] args =arg_str.split(" ");
								
								CatlaRunner.main(args);
								
								display.asyncExec(new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub
										 MessageBox box = new MessageBox(shell,  SWT.OK);
										   box.setText("Tips");
										   box.setMessage("Finished!");
										   box.open();
									}
									
								});
								
							}
				        	
				        },"optimizer") ;
						
				        
				        taskThread.start();
				        
				        
				        Thread logThread=new Thread(new LogTaskRunnable(folderPath),"log");
				        
				        logThread.start();
				 
				        
						}catch(Exception ex) {
							ex.printStackTrace();
						}
					
				}
			});
			btnStart.setBounds(339, 21, 126, 34);
			
			btnStart.setText("Start");
			
			Button btnNewButton = new Button(shell, SWT.NONE);
			btnNewButton.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseDown(MouseEvent e) {
					 DirectoryDialog dialog = new DirectoryDialog(shell);
					   // dialog.setFilterPath("c:\\"); // Windows specific
					  //  System.out.println("RESULT=" + dialog.open());
					   String folderPath=dialog.open();
					   if(folderPath!=null&&new File(folderPath).exists()) {
						   txtProjectFolder.setText(folderPath);
						   
						   updateArgs(coTool,coOptimizer);
						   
					   }else {
						   /*
						   MessageBox box = new MessageBox(shell,  SWT.OK);
						   box.setText("Tips");
						   box.setMessage("Please open a valid folder");

						   box.open(); // Call this on button pressed. Returns SWT.OK or SWT.CANCEL
						   */
						   txtProjectFolder.setText("");
					   }
					 
					  
				}
			});
			
		 
			btnNewButton.setBounds(176, 21, 126, 34);
			btnNewButton.setText("Open");
			
			txtConsole = new Text(shell, SWT.READ_ONLY | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
			txtConsole.setBounds(29, 716, 959, 234);
			formToolkit.adapt(txtConsole, true, true);
			 
		
			  OutputStream out = new OutputStream() {
		            StringBuffer buffer = new StringBuffer();

		            @Override
		            public void write(final int b) throws IOException {
		                if (txtConsole.isDisposed())
		                    return;
		                buffer.append((char) b);
		            }

		            @Override
		            public void write(byte[] b, int off, int len) throws IOException {
		                super.write(b, off, len);
		                flush();
		            }

		            @Override
		            public void flush() throws IOException {
		                final String newText = buffer.toString();
		                Display.getDefault().asyncExec(new Runnable() {
		                    public void run() {
		                    	txtConsole.append(newText);
		                    }
		                });
		                buffer = new StringBuffer();
		            }
		        };

		        System.setOut(new PrintStream(out));
		        final PrintStream oldOut = System.out;

		        txtConsole.addDisposeListener(new DisposeListener() {
		            public void widgetDisposed(DisposeEvent e) {
		                System.setOut(oldOut);
		            }
		        });
			 
	}
	DefaultCategoryDataset dataset;
	private Text txtArg;
	private final FormToolkit formToolkit = new FormToolkit(Display.getDefault());
	private Text txtConsole;

	 private DefaultCategoryDataset createDataset( ) {
	      dataset = new DefaultCategoryDataset( );
	      /*
	      dataset.addValue( 1 , "schools" , "1" );
	      dataset.addValue( 2 , "schools" , "2" );
	      dataset.addValue( 3 , "schools" ,  "3" );
	      dataset.addValue( 4 , "schools" , "4" );
	      dataset.addValue( 5 , "schools" , "5" );
	      dataset.addValue( 6 , "schools" , "6" );
	      */
	      return dataset;
	   }
}
