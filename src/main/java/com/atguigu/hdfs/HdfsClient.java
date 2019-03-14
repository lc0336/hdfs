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
		
		
		
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);

		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
		
		// 2 创建目录
		fs.mkdirs(new Path("/1108/daxian/banzhang"));
		
		// 3 关闭资源
		fs.close();
		System.out.println("over");
	}
	
	
	
	//1.文件上传案例
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

			// 1 获取文件系统
			Configuration configuration = new Configuration();
			configuration.set("dfs.replication", "2");
			FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");

			// 2 上传文件
			fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

			// 3 关闭资源
			fs.close();

			System.out.println("over");
	}
	
	
	//2.文件下载
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

			// 1 获取文件系统
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
			// 2 执行下载操作
			// boolean delSrc 指是否将原文件删除
			// Path src 指要下载的文件路径
			// Path dst 指将文件下载到的路径
			// boolean useRawLocalFileSystem 是否开启文件校验
			fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);
			
			// 3 关闭资源
			fs.close();
	}
	
	//3.文件删除
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 执行删除
		fs.delete(new Path("/1108/"), true);
			
		// 3 关闭资源
		fs.close();
	}
	
	
	//文件改名
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu"); 
			
		// 2 修改文件名称
		fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));
			
		// 3 关闭资源
		fs.close();
	}
	
	//文件详情查看
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

		// 1获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu"); 
			
		// 2 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
			
		while(listFiles.hasNext()){
			LocatedFileStatus status = listFiles.next();
				
			// 输出详情
			// 文件名称
			System.out.println(status.getPath().getName());
			// 长度
			System.out.println(status.getLen());
			// 权限
			System.out.println(status.getPermission());
			// 分组
			System.out.println(status.getGroup());
				
			// 获取存储的块信息
			BlockLocation[] blockLocations = status.getBlockLocations();
				
			for (BlockLocation blockLocation : blockLocations) {
					
				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
					
				for (String host : hosts) {
					System.out.println(host);
				}
			}
				
			System.out.println("-----------班长的分割线----------");
		}

	// 3 关闭资源
	fs.close();
	}
	
	//HDFS文件和文件夹判断
	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
			
		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.37.101:9000"), configuration, "atguigu");
			
		// 2 判断是文件还是文件夹
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
			
		for (FileStatus fileStatus : listStatus) {
			
			// 如果是文件
			if (fileStatus.isFile()) {
					System.out.println("f:"+fileStatus.getPath().getName());
				}else {
					System.out.println("d:"+fileStatus.getPath().getName());
				}
			}
			
		// 3 关闭资源
		fs.close();
	}
}
