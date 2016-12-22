package com.cealiy.ssh;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SSHClient {

	private String host;

	private int port;

	private String user;

	private String password;
	
	public SSHClient(){
		
	}
	
	public SSHClient(String host,int port,String user,String password){
		this.host=host;
		this.port=port;
		this.user=user;
		this.password=password;
	}
	
	public SSHClient(String host,String user,String password){
		this.host=host;
		this.port=22;
		this.user=user;
		this.password=password;
	}
	
	public void put(String localPath,String remotePath) throws JSchException,IOException,SftpException{
		JSch jsch = new JSch();
		Session session = jsch.getSession(user, host, port);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(password);
		session.connect();
		ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
		channel.connect();
		channel.put(localPath,remotePath);
		channel.disconnect();
		session.disconnect();
	}
	
	

	public String exeCommand(String command,int timeout) throws JSchException,IOException {
		JSch jsch = new JSch();
		Session session = jsch.getSession(user, host, port);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(password);
		session.setTimeout(timeout);
		session.connect();
		ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
		InputStream in = channelExec.getInputStream();
		channelExec.setCommand(command);
		channelExec.setErrStream(System.err);
		channelExec.connect();
		String out = IOUtils.toString(in, "UTF-8");
		channelExec.disconnect();
		session.disconnect();
		return out;
	}



	public String exeCommand(String command) throws JSchException,IOException {
		JSch jsch = new JSch();
		Session session = jsch.getSession(user, host, port);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(password);
		session.connect();
		ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
		InputStream in = channelExec.getInputStream();
		channelExec.setCommand(command);
		channelExec.setErrStream(System.err);
		channelExec.connect();
		String out = IOUtils.toString(in, "UTF-8");
		channelExec.disconnect();
		session.disconnect();
		return out;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	

}
