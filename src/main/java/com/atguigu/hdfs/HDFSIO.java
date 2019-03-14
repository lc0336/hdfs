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
	//1�����󣺰ѱ���e���ϵ�banhua.txt�ļ��ϴ���HDFS��Ŀ¼
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");

		// 2 ����������
		FileInputStream fis = new FileInputStream(new File("e:/��ҵ.txt"));

		// 3 ��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/banZHANG.txt"));

		// 4 ���Կ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	    fs.close();
	}
	
	//1�����󣺴�HDFS������banhua.txt�ļ�������e����
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/banZHANG.txt"));
			
		// 3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("e:/banZHANG.txt"));
			
		// 4 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	//1�����󣺷ֿ��ȡHDFS�ϵĴ��ļ��������Ŀ¼�µ�/hadoop-2.7.2.tar.gz
	//��1�����ص�һ��
	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
			
		// 3 ���������
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
			
		// 4 ���Ŀ���
		byte[] buf = new byte[1024];
			
		for(int i =0 ; i < 1024 * 128; i++){
			fis.read(buf);
			fos.write(buf);
		}
			
		// 5�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	fs.close();
	}
	//��2�����صڶ���
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
			
		// 2 ��������
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
			
		// 3 ��λ��������λ��
		fis.seek(1024*1024*128);
			
		// 4 ���������
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
			
		// 5 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 6 �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
