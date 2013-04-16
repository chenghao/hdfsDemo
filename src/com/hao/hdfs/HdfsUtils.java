package com.hao.hdfs;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtils {

	/**
	 * 上传本地文件到HDFS里面
	 * @param localFile  本地文件
	 * @param destFile	 HDFS文件 
	 * @throws IOException 
	 */
	public static void uploadToHdfs(String localFile, String hdfsFile) throws IOException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(URI.create(hdfsFile), conf);

		InputStream is = new BufferedInputStream(new FileInputStream(localFile));
		OutputStream os = system.create(new Path(hdfsFile));
		
		IOUtils.copyBytes(is, os, 5120, true);
	}
	
	public static void uploadToHdfs(String localFile){
		if(getFileName(localFile).contains("*")){
			System.out.println("aaa");
		}
	}
	
	
	/**
	 * 以append方式将内容添加到HDFS上文件的末尾.
	 * 需要在hdfs-site.xml中添
	 * <property>
	 * 		<name>dfs.support.append</name>
	 * 		<value>true</value>
	 * </property>
	 * @param hdfsFile  HDFS文件
	 * @param content   要添加的内容
	 * @throws IOException 
	 */
	public static void appendToHdfs(String hdfsFile, String content) throws IOException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(URI.create(hdfsFile), conf);
		
		FSDataOutputStream out = system.append(new Path(hdfsFile));
		InputStream in;
		
		File file = new File(content);
		if(file.isFile()){
			in = new BufferedInputStream(new FileInputStream(content));
		}else{
			in = new ByteArrayInputStream(content.getBytes());
		}
		
		IOUtils.copyBytes(in, out, 5120, true);
	}
	
	/**
	 * 从HDFS上删除文件
	 * @param hdfsFile	HDFS文件
	 * @param recursive	是否递归删除 
	 * @return  删除成功返回true, 否则返回false.
	 * @throws IOException 
	 */
	public static boolean deleteHdfsFile(String hdfsFile, boolean recursive) throws IOException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(URI.create(hdfsFile), conf);
		
		return system.delete(new Path(hdfsFile), recursive);
	}
	
	/**
	 * 从HDFS上删除文件. 默认递归删除 
	 * @param hdfsFile	HDFS文件
	 * @return
	 * @throws IOException 
	 */
	public static boolean deleteHdfsFile(String hdfsFile) throws IOException{
		return deleteHdfsFile(hdfsFile, true);
	}
	
	/**
	 * 获取文件名
	 * @param srcPath  原文件路经
	 * @return
	 */
	public static String getFileName(String srcPath){
		int index = srcPath.lastIndexOf("/");
		return srcPath.substring(index + 1, srcPath.length());
	}
	
	
	///////////////////////////////
	public static void main(String[] args) {
		/*
		String localFile = "E:/联想项目/04.01-09-log/p_log_file_date=2012-04-01/test.txt";
		String hdfsFile = "hdfs://master:9100/user/root/hdfsDemo/" + getFileName(localFile);
		HdfsWrite.uploadToHdfs(localFile, hdfsFile);
		System.out.println("上传成功...");
		*/
		
		/*
		String hdfsFile = "hdfs://master:9100/user/root/hdfsDemo/test.txt";
		StringBuffer content = new StringBuffer(64);
		content.append("\nhhhhhhhhhhhh\n");
		content.append("iiiiiiiiiii\n");
		content.append("jjjjjjjjjjjjjj\n");
		content.append("kkkkkkkkkkkkkkkkkkk\n");
		content.append("llllllllllllllllllllll\n");
		content.append("mmmmmmmmmmmmmm\n");
		content.append("nnnnnnnnnnnnnnnnnnnnn\n");
		//String content = "E:/联想项目/04.01-09-log/p_log_file_date=2012-04-01/test_append.txt";
		HdfsWrite.appendToHdfs(hdfsFile, content.toString());
		System.out.println("添加成功...");
		*/
		
		/*
		String hdfsFile = "hdfs://master:9100/user/root/hdfsDemo/rps.log*";
		if(HdfsWrite.deleteHdfsFile(hdfsFile)){
			System.out.println("删除成功...");
		}else{
			System.out.println("删除失败...");
		}
		*/
		
		HdfsUtils.uploadToHdfs("E:/联想项目/04.01-09-log/p_log_file_date=2012-04-01/rps.log*");
	}
}
