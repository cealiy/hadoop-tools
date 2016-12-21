package com.cealiy.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;


public class HttpFSFileSystemFactory {
	
	private static String user;
	
	private static String host;
	
	private static String port;
	
	
	public static HttpFSFileSystem get(){
		HttpFSFileSystem fs=null;
		try {
			URI temp=new URI("webhdfs://"+host+":"+port);
			fs=new HttpFSFileSystem();
			fs.setHODOOP_HTTPFS_USER(user);
			Configuration conf=new Configuration();
			fs.initialize(temp,conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}
	
	
	public static HttpFSFileSystem get(String uri,String user){
		HttpFSFileSystem fs=null;
		try {
			URI temp=new URI(uri);
			fs=new HttpFSFileSystem();
			fs.setHODOOP_HTTPFS_USER(user);
			Configuration conf=new Configuration();
			fs.initialize(temp,conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}


	public String getUser() {
		return user;
	}


	public void setUser(String user) {
		HttpFSFileSystemFactory.user = user;
	}


	public String getHost() {
		return host;
	}


	public void setHost(String host) {
		HttpFSFileSystemFactory.host = host;
	}


	public String getPort() {
		return port;
	}


	public void setPort(String port) {
		HttpFSFileSystemFactory.port = port;
	}
	
	

}
