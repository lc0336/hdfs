package com.atguigu.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {
	
	public static void main(String[] args) throws Exception {
		
		
		
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		// �����ڼ�Ⱥ������
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
		
		// 2 ����Ŀ¼
		fs.mkdirs(new Path("/1108/daxian/banzhang"));
		
		// 3 �ر���Դ
		fs.close();
		System.out.println("over");
	}
	
	
	
	//1.�ļ��ϴ�����
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

			// 1 ��ȡ�ļ�ϵͳ
			Configuration configuration = new Configuration();
			configuration.set("dfs.replication", "2");
			FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");

			// 2 �ϴ��ļ�
			fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

			// 3 �ر���Դ
			fs.close();

			System.out.println("over");
	}
	
	
	//2.�ļ�����
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

			// 1 ��ȡ�ļ�ϵͳ
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
			// 2 ִ�����ز���
			// boolean delSrc ָ�Ƿ�ԭ�ļ�ɾ��
			// Path src ָҪ���ص��ļ�·��
			// Path dst ָ���ļ����ص���·��
			// boolean useRawLocalFileSystem �Ƿ����ļ�У��
			fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);
			
			// 3 �ر���Դ
			fs.close();
	}
	
	//3.�ļ�ɾ��
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException{

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 ִ��ɾ��
		fs.delete(new Path("/1108/"), true);
			
		// 3 �ر���Դ
		fs.close();
	}
	
	
	//�ļ�����
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException{

		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu"); 
			
		// 2 �޸��ļ�����
		fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));
			
		// 3 �ر���Դ
		fs.close();
	}
	
	//�ļ�����鿴
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

		// 1��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu"); 
			
		// 2 ��ȡ�ļ�����
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
			
		while(listFiles.hasNext()){
			LocatedFileStatus status = listFiles.next();
				
			// �������
			// �ļ�����
			System.out.println(status.getPath().getName());
			// ����
			System.out.println(status.getLen());
			// Ȩ��
			System.out.println(status.getPermission());
			// ����
			System.out.println(status.getGroup());
				
			// ��ȡ�洢�Ŀ���Ϣ
			BlockLocation[] blockLocations = status.getBlockLocations();
				
			for (BlockLocation blockLocation : blockLocations) {
					
				// ��ȡ��洢�������ڵ�
				String[] hosts = blockLocation.getHosts();
					
				for (String host : hosts) {
					System.out.println(host);
				}
			}
				
			System.out.println("-----------�೤�ķָ���----------");
		}

	// 3 �ر���Դ
	fs.close();
	}
	
	//HDFS�ļ����ļ����ж�
	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
			
		// 1 ��ȡ�ļ�������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 �ж����ļ������ļ���
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
			
		for (FileStatus fileStatus : listStatus) {
			
			// ������ļ�
			if (fileStatus.isFile()) {
					System.out.println("f:"+fileStatus.getPath().getName());
				}else {
					System.out.println("d:"+fileStatus.getPath().getName());
				}
			}
			
		// 3 �ر���Դ
		fs.close();
	}
}
