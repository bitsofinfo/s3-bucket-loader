package org.bitsofinfo.s3.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


public class CommandExecutor {

	private static final Logger logger = Logger.getLogger(CommandExecutor.class);

	public CmdResult execute(CommandLine cmdLine, int maxAttempts) {
		
		CmdResult lastCmdResult = null;
		
		int attempts = 0;
		while(attempts < maxAttempts) {
		
			attempts++;
			
			final StringWriter stdOut = new StringWriter();
			final StringWriter stdErr = new StringWriter();
			
			try {
			
				DefaultExecutor executor = new DefaultExecutor();
					executor.setStreamHandler(new ExecuteStreamHandler() {
						public void setProcessOutputStream(InputStream is) throws IOException {IOUtils.copy(is, stdOut, "UTF-8");}
						public void setProcessErrorStream(InputStream is) throws IOException {IOUtils.copy(is, stdErr, "UTF-8");}
						public void stop() throws IOException {}
						public void start() throws IOException {}
						public void setProcessInputStream(OutputStream os) throws IOException {}
					});
					
				System.out.println(cmdLine.toString());
					
				int exitValue = executor.execute(cmdLine);
				if (exitValue > 0) {
					logger.error("ERROR: attempt #: " + attempts+ " exitCode: "+exitValue+" cmd=" + cmdLine.toString());
				}
				
				//System.out.println("STDOUT:"+stdOut);
				//System.out.println("STDERR:"+stdErr);
				
				lastCmdResult = new CmdResult(exitValue,stdOut.toString(),stdErr.toString());
				
				// if successful return immediately...
				if (exitValue == 0) {
					return lastCmdResult;
				}
				
			} catch(Exception e) {
				logger.error("execute() attempt #: " + attempts+ " cmd:"+cmdLine.toString() + " exception:"+e.getMessage(),e);
				lastCmdResult = new CmdResult(9999, stdOut.toString(), "attempt #: " + attempts+ " exception: " + e.getMessage() + " stdErr: " + stdErr.toString());
			}
		}
		
		return lastCmdResult;
	}
	

}
