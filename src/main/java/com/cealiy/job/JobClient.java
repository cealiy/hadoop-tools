package com.cealiy.job;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cealiy.hdfs.HttpFSFileSystem;
import com.cealiy.hdfs.HttpFSFileSystemFactory;
import com.cealiy.ssh.SSHClient;

public class JobClient {
	
	private final static Logger logger=LoggerFactory.getLogger(JobClient.class);
	
	private HttpFSFileSystemFactory httpFSFileSystemFactory;
	
	private SSHClient sshClient;
	
	private String hadoopHome;
	
	private String gitPath;
	
	private String hdfsPrefix;
	
	private String jobName;
	
	private String jobJar;
	
	public final static String GIT_PULL="git pull";
	
	public final static String MVN_CLEAN_PACKAGE="mvn clean package";
	
	public final static String EXE_HADOOP_JAR="bin/hadoop jar";
	
	public final static String PARAM_SEGMENT=" ";
	
	public void execute(String statisticPath,String resultDir){
		checkConf();
		try{
			GitPullAndPackage run=new GitPullAndPackage(this.gitPath,this.sshClient);
			new Thread(run).start();
			HttpFSFileSystem fileSystem=httpFSFileSystemFactory.get();
			String hdfsIn=httpFSFileSystemFactory.getHdfsUri()+this.getHdfsInPath()+resultDir;
			Path hdfsInPath=new Path(hdfsIn);
			if(fileSystem.exists(hdfsInPath)){
				fileSystem.delete(hdfsInPath,false);
			}
			fileSystem.copyFromLocalFile(new Path(statisticPath),new Path(hdfsIn));
			StringBuilder exe=new StringBuilder();
			exe.append("cd").append(PARAM_SEGMENT).append(this.hadoopHome).append("&&");
			exe.append(EXE_HADOOP_JAR).append(PARAM_SEGMENT).append(this.gitPath).append("/target/").append(this.jobJar);
			exe.append(PARAM_SEGMENT).append(this.jobName).append(PARAM_SEGMENT).append(this.getHdfsInPath()).append(resultDir);
			exe.append(PARAM_SEGMENT).append(this.getHdfsOutPath()).append(resultDir);
			String out2=sshClient.exeCommand(exe.toString());
			logger.info(out2);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	
	private void checkConf(){
		if(this.hadoopHome==null||this.hadoopHome.isEmpty()){
			this.hadoopHome="/root/hadoop-2.7.3";
		}
		if(this.gitPath==null||this.gitPath.isEmpty()){
			this.gitPath="/root/git/mapreduce-job";
		}
		if(this.jobJar==null||this.jobJar.isEmpty()){
			this.jobJar="mapreduce-job.jar";
		}
	}

	public HttpFSFileSystemFactory getHttpFSFileSystemFactory() {
		return httpFSFileSystemFactory;
	}

	public void setHttpFSFileSystemFactory(HttpFSFileSystemFactory httpFSFileSystemFactory) {
		this.httpFSFileSystemFactory = httpFSFileSystemFactory;
	}

	public SSHClient getSshClient() {
		return sshClient;
	}

	public void setSshClient(SSHClient sshClient) {
		this.sshClient = sshClient;
	}

	public String getHadoopHome() {
		return hadoopHome;
	}

	public void setHadoopHome(String hadoopHome) {
		this.hadoopHome = hadoopHome;
	}

	public String getGitPath() {
		return gitPath;
	}

	public void setGitPath(String gitPath) {
		this.gitPath = gitPath;
	}

	public String getHdfsPrefix() {
		return hdfsPrefix;
	}

	public void setHdfsPrefix(String hdfsPrefix) {
		this.hdfsPrefix = hdfsPrefix;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	
	
	public String getJobJar() {
		return jobJar;
	}


	public void setJobJar(String jobJar) {
		this.jobJar = jobJar;
	}

	
	public String getHdfsInPath(){
		return "/input/"+this.hdfsPrefix+"/";
	}
	
	public String getHdfsOutPath(){
		return "/output/"+this.hdfsPrefix+"/";
	}
	
	private class GitPullAndPackage implements Runnable{
		
		private String gitPath;
		
		private SSHClient sshClient;
		
		public GitPullAndPackage(String gitPath,SSHClient sshClient){
			this.gitPath=gitPath;
			this.sshClient=sshClient;
		}
		
		public void run() {
			try{
				StringBuilder packageCommand=new StringBuilder();
				packageCommand.append("cd").append(PARAM_SEGMENT).append(this.gitPath).append("&&");
				packageCommand.append(GIT_PULL).append("&&");
				packageCommand.append(MVN_CLEAN_PACKAGE);
				String out1=sshClient.exeCommand(packageCommand.toString());
				logger.info(out1);
			}catch(Exception e){
				e.printStackTrace();
			}
			
			
		}
	}

	
	

}
