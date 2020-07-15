package cn.edu.bjtu.cdh.catla.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CatlaFileUtils {
	

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
	
	
	public static List<String> readFileByLine(String fileName) {
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
				//System.out.println(tempString);
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
}
