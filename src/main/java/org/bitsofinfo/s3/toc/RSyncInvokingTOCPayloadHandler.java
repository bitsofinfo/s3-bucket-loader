package org.bitsofinfo.s3.toc;

import java.io.File;

import org.apache.commons.exec.CommandLine;
import org.apache.log4j.Logger;
import org.bitsofinfo.s3.cmd.CommandExecutor;
import org.bitsofinfo.s3.cmd.CommandExecutor.CmdResult;
import org.bitsofinfo.s3.cmd.FilePathOpResult;
import org.bitsofinfo.s3.worker.WorkerState;

import com.google.gson.Gson;

public class RSyncInvokingTOCPayloadHandler implements TOCPayloadHandler {

	private static final Logger logger = Logger.getLogger(RSyncInvokingTOCPayloadHandler.class);
	
	private CommandExecutor executor = null;
	private String sourceDirectoryRootPath = null;
	private String targetDirectoryRootPath = null;
	
	private Gson gson = new Gson();
	
	public RSyncInvokingTOCPayloadHandler() {
		this.executor = new CommandExecutor();
	}
	
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception {
		

		String sourcePath = (sourceDirectoryRootPath + payload.fileInfo.getFilePath()).replaceAll("//", "/");
		String targetPath = (targetDirectoryRootPath + payload.fileInfo.getFilePath()).replaceAll("//", "/");
		
		boolean overallSuccess = true;
		
		/**
		 * MKDIR
		 */
		String mkdirCmd = null;
		CmdResult mkdirResult = null;
		try {
			
			// quote the path for files w/ spaces
			String dirPath = targetPath.substring(0,targetPath.lastIndexOf('/'));
			
			
			// mkdir -p targetPath
			CommandLine mkdirCmdLine = new CommandLine("mkdir");
			mkdirCmdLine.addArgument("-p");
			mkdirCmdLine.addArgument(dirPath,false);
			
			mkdirCmd = mkdirCmdLine.toString();
			
			// try mkdir specifically 3 times, I've seen it silently succeed
			// and its like the cmd never made it to yas3fs
			File dirFile = new File(dirPath);
			int attempts = 0;
			int maxAttempts = 3;
			
			while((mkdirResult == null || mkdirResult.getExitCode() > 0 || !dirFile.exists())
					&& 
				  (attempts < maxAttempts)) {
				
				attempts++;
				logger.debug("handlePayload() attempt#: "+attempts+ " executing mkdir: " + mkdirCmd);
				
				mkdirResult = executor.execute(mkdirCmdLine,3);
			}
			
			String resultAsJson = gson.toJson(mkdirResult);
			
			if (mkdirResult.getExitCode() > 0) {
				workerState.addFilePathWriteFailure(
						new FilePathOpResult(payload.mode, false, targetPath, mkdirCmd, resultAsJson));
				overallSuccess = false;
			}
			
		} catch(Exception e) {
			workerState.addFilePathWriteFailure(
					new FilePathOpResult(payload.mode, false, targetPath, mkdirCmd, "exception: " + e.getMessage()));
			
			logger.error("File mkdir exception: " +mkdirCmd + " " + e.getMessage(),e);
			
			overallSuccess = false;
		}
		
		if (!overallSuccess) {
			return; // exit
		}
			
		/**
		 * RSYNC
		 */
		String rsyncCmd = null;
		CmdResult rsyncResult = null;
		try {

			// rsync --inplace -avz sourcePath targetPath
			CommandLine rsyncCmdLine = new CommandLine("rsync");
			rsyncCmdLine.addArgument("--inplace");
			rsyncCmdLine.addArgument("-avz");
			rsyncCmdLine.addArgument(sourcePath,false);
			rsyncCmdLine.addArgument(targetPath,false);
			
			rsyncCmd = rsyncCmdLine.toString();
			
			logger.debug("handlePayload() executing RSYNC: " + rsyncCmd);
			rsyncResult = executor.execute(rsyncCmdLine,3);
			
			String resultAsJson = gson.toJson(rsyncResult);
			
			if (rsyncResult.getExitCode() > 0) {
				workerState.addFilePathWriteFailure(
						new FilePathOpResult(payload.mode, false, targetPath, rsyncCmd, resultAsJson));
				overallSuccess = false;
			}
			
		} catch(Exception e) {
			
			workerState.addFilePathWriteFailure(
					new FilePathOpResult(payload.mode, false, targetPath, rsyncCmd, "exception: " + e.getMessage()));
			
			logger.error("File rsync exception: " +rsyncCmd + " " + e.getMessage(),e);
			
			overallSuccess = false;
		}
		
		
		/**
		 * Record success
		 */
		if (overallSuccess) {
			String asJson = gson.toJson(new CmdResult[]{mkdirResult,rsyncResult});
			
			workerState.addFilePathWritten(
					new FilePathOpResult(payload.mode, true, targetPath, "mkdir + rsync", asJson));
		}
		
	}

	public void setSourceDirectoryRootPath(String sourceDirectoryRootPath) {
		this.sourceDirectoryRootPath = sourceDirectoryRootPath;
	}

	public void setTargetDirectoryRootPath(String targetDirectoryRootPath) {
		this.targetDirectoryRootPath = targetDirectoryRootPath;
	}

	public void handlePayload(TOCPayload payload) throws Exception {
		throw new UnsupportedOperationException("RSyncInvokingTOCPayloadHandler does not " +
				"support this method variant, call me through Worker");
	}

}
