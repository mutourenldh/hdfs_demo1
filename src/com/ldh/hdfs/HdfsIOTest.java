package com.ldh.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsIOTest {
	/**
	 * hdfs文件上传
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop105:9000"), configuration, "ldh");
		// 2 创建输入流
		FileInputStream fis = new FileInputStream(new File("d:/hello.txt"));
		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));
		// 4 流对拷
		IOUtils.copyBytes(fis, fos, configuration);
		// 5 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

	// 从 HDFS 上下载文件到本地控制台上。
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop105:9000"), configuration, "ldh");
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hello4.txt"));
		// 3 获取输出流
		IOUtils.copyBytes(fis, System.out, configuration);
		// 4 流对拷
		// 5 关闭资源
		IOUtils.closeStream(fis);
	}

	// 下载第一块文件
	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop105:9000"), configuration, "ldh");
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/atguigu/hadoop-2.7.2.tar.gz"));
		// 3 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
		// 4 流的拷贝
		byte[] buf = new byte[1024];
		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}
		// 5 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

	// 下载第二块文件
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop105:9000"), configuration, "ldh");
		// 2 打开输入流
		FSDataInputStream fis = fs.open(new Path("/user/atguigu/hadoop-2.7.2.tar.gz"));
		// 3 定位输入数据位置
		fis.seek(1024 * 1024 * 128);
		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

}
