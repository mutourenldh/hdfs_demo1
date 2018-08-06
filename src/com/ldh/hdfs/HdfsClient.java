package com.ldh.hdfs;

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

	public static void main(String[] args) throws Exception, InterruptedException, URISyntaxException {
		// 1.获取文件系统
		Configuration conf = new Configuration();
		// 配置在集群上运行
		// hdfs://hadoop102:9000：集群地址
		// ldh:以什么用户操作集群
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ldh");
		// 2.上传文件
		// 将本地e:/hello.txt文件上传到集群根目录
		fileSystem.copyFromLocalFile(new Path("e:/hello.txt"), new Path("/user/hello1.txt"));
		// 3.关闭资源
		fileSystem.close();
		System.out.println("over...");
	}

	// 1.测试HDFS获取文件系统
	@Test
	public void initHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2 获取文件系统
		FileSystem fs = FileSystem.get(configuration);
		// 3 打印文件系统
		System.out.println(fs.toString());
	}
	//2.测试文件上传
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
//		 configuration.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "ldh");
		// 2 上传文件
		fs.copyFromLocalFile(new Path("e:/hello.txt"), new Path("/hello14.txt"));
		// 3 关闭资源
		fs.close();
		System.out.println("over");
	}
	//3.测试文件下载
	@Test
	public void testCopyToLocalFile() throws Exception {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
//		 configuration.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "ldh");
		// 2 上传文件
		/**
		 * boolean delSrc 指是否将原文件删除(第一个参数)
		 * Path src 指要下载的文件路径(第二个参数)
		 * Path dst 指将文件下载到的路径(第三个参数)
		 * boolean useRawLocalFileSystem 是否开启文件效验(第四个参数)
		 */
		fs.copyToLocalFile(false, new Path("/hello14.txt"), new Path("d:/mm.txt"), true);
		// 3 关闭资源
		fs.close();
		System.out.println("over");
	}
	//4.测试创建目录
	@Test
	public void testMkdirs() throws IOException, InterruptedException,
	URISyntaxException{
	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
	"ldh");
	// 2 创建目录
	fs.mkdirs(new Path("/0806/ldh/0806"));
	// 3 关闭资源
	fs.close();
	}
	//5.测试文件夹删除
	@Test
	public void testDelete() throws IOException, InterruptedException,
	URISyntaxException{
	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
	"ldh");
	// 2 执行删除
	fs.delete(new Path("/0806/"), true);
	// 3 关闭资源
	fs.close();
	}
//	6.测试修改文件名称
	@Test
	public void testRename() throws IOException, InterruptedException,
	URISyntaxException{
	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
	"ldh");
	// 2 修改文件名称
	fs.rename(new Path("/hello14.txt"), new Path("/hello.txt"));
	// 3 关闭资源
	fs.close();
	}
	//7.获取文件详情
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "ldh");
		// 2 获取文件详情
		//获得根目录下的所有文件集合(不包括文件夹)
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			// 输出详情
			// 文件名称
			System.out.println("文件名称==="+status.getPath().getName());
			// 长度
			System.out.println("文件长度==="+status.getLen());
			// 权限
			System.out.println("文件权限==="+status.getPermission());
			// z 组
			System.out.println("文件所属==="+status.getGroup());
			// 获取存储的块信息
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println("主机节点==="+host);
				}
			}
			System.out.println("----------------班长的分割线-----------");
		}
	}
	//8.判断是文件夹还是文件
	@Test
	public void testListStatus() throws Exception {
		// 1 获取文件配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "ldh");
		// 2 判断是文件还是文件夹
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			// 如果是文件
			if (fileStatus.isFile()) {
				System.out.println("file:" + fileStatus.getPath().getName());
			} else {
				System.out.println("dir:" + fileStatus.getPath().getName());
			}
		}
		// 3 关闭资源
		fs.close();
	}
}
