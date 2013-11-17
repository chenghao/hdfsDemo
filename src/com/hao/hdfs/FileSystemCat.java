package com.hao.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 相当于linux的cat命令
 * 
 * hadoop 包名/类名 hdfs://ip:port/路经
 * 
 * @author chenghao
 *
 */
public class FileSystemCat {

	public static void main(String[] args) throws IOException {
		if(args.length < 1){
			System.out.println("请传入hdfs上需要cat的文件名．");
			System.exit(0);
		}
		
		String uri = args[0];
		
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
		
		InputStream in = null;
		
		try{
			in = fileSystem.open(new Path(uri));
			
			IOUtils.copyBytes(in, System.out, 5120, false);
		}finally{
			IOUtils.closeStream(in);
		}

	}

}
