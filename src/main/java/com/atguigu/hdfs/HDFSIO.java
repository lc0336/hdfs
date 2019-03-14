package com.atguigu.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import java.net.URI;

public class HDFSIO {
	//1．需求：把本地e盘上的banhua.txt文件上传到HDFS根目录
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");

		// 2 创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/作业.txt"));

		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/banZHANG.txt"));

		// 4 流对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	    fs.close();
	}
	
	//1．需求：从HDFS上下载banhua.txt文件到本地e盘上
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/banZHANG.txt"));
			
		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/banZHANG.txt"));
			
		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	//1．需求：分块读取HDFS上的大文件，比如根目录下的/hadoop-2.7.2.tar.gz
	//（1）下载第一块
	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
			
		// 3 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
			
		// 4 流的拷贝
		byte[] buf = new byte[1024];
			
		for(int i =0 ; i < 1024 * 128; i++){
			fis.read(buf);
			fos.write(buf);
		}
			
		// 5关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	fs.close();
	}
	//（2）下载第二块
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
			
		// 2 打开输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
			
		// 3 定位输入数据位置
		fis.seek(1024*1024*128);
			
		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
			
		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
