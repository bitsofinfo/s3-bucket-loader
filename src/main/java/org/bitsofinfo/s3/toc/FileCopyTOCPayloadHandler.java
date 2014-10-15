package org.bitsofinfo.s3.toc;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.exec.CommandLine;
import org.apache.log4j.Logger;
import org.bitsofinfo.s3.cmd.CmdResult;
import org.bitsofinfo.s3.cmd.CommandExecutor;
import org.bitsofinfo.s3.cmd.TocPathOpResult;
import org.bitsofinfo.s3.worker.WorkerState;

import com.google.gson.Gson;

public class FileCopyTOCPayloadHandler implements TOCPayloadHandler {

	private static final Logger logger = Logger.getLogger(FileCopyTOCPayloadHandler.class);

	private CommandExecutor executor = null;
	private String sourceDirectoryRootPath = null;
	private String targetDirectoryRootPath = null;
	
	private boolean useRsync = true;
	private String rsyncOptions = null;
	private List<String> rsyncOptionsList = new ArrayList<String>();
	private String rsyncTolerableErrorsRegex = null;
	private Pattern rysyncTolerableErrorPattern = null;
	
	private String chown = null;
	private boolean chownDirsOnly = false;
	
	private String chmod = null;
	private boolean chmodDirsOnly = false;
	
	private int retries = 1;
	private long retriesSleepMS = 1000;
	
	private Gson gson = new Gson();
	
	public FileCopyTOCPayloadHandler() {
		this.executor = new CommandExecutor();
	}
	
	public void handlePayload(TOCPayload payload, WorkerState workerState) throws Exception {
		

		String sourceFilePath = (sourceDirectoryRootPath + payload.tocInfo.getPath()).replaceAll("//", "/");
		String targetFilePath = (targetDirectoryRootPath + payload.tocInfo.getPath()).replaceAll("//", "/");
		
		// we need this for mkdirs..
		String targetDirPath = null;
		if (payload.tocInfo.isDirectory()) {
			targetDirPath = targetFilePath;
		} else {
			// get the parent dir of the file
			targetDirPath = targetFilePath.substring(0,targetFilePath.lastIndexOf('/')); 
		}

		List<CmdResult> commandsRun = new ArrayList<CmdResult>();
		
		/**
		 * MKDIR against targetDirPath
		 */
		// mkdir -p targetDirPath
		CommandLine mkdirCmdLine = new CommandLine("mkdir");
		mkdirCmdLine.addArgument("-p");
		mkdirCmdLine.addArgument(targetDirPath,false);

		CmdResult mkdirResult = exec(getRetries(),"mkdir",mkdirCmdLine,targetDirPath);
		commandsRun.add(mkdirResult);
		if (mkdirResult.getExitCode() > 0) {
			workerState.addTocPathWriteFailure(
					new TocPathOpResult(payload.mode, false, targetFilePath, mkdirCmdLine.toString(), gson.toJson(mkdirResult)));
			
			return; // exit
		}
			
		/**
		 * RSYNC (files only)
		 */
		if (!payload.tocInfo.isDirectory()) {
			
			if (this.isUseRsync()) {
				// rsync --inplace -avz sourcePath targetPath
				CommandLine rsyncCmdLine = new CommandLine("rsync");
				for (String arg : rsyncOptionsList) {
					rsyncCmdLine.addArgument(arg);
				}
				rsyncCmdLine.addArgument(sourceFilePath,false);
				rsyncCmdLine.addArgument(targetFilePath,false);
				
				CmdResult rsyncResult = exec(getRetries(),"rsync",rsyncCmdLine,targetFilePath);
				commandsRun.add(rsyncResult);
				
				if (rsyncResult.getExitCode() > 0) {
					
					// tolerable?
					if (!this.rsyncErrorIsTolerable(rsyncResult)) {
						
						workerState.addTocPathWriteFailure(
								new TocPathOpResult(payload.mode, false, targetFilePath, rsyncCmdLine.toString(), gson.toJson(rsyncResult)));
						
						return; // exit
						
					} else {
						// record that we tolerated this error
						workerState.addTocPathErrorTolerated(
								new TocPathOpResult(payload.mode, true, targetFilePath, 
										"Error tolerated by regex: " + this.rsyncTolerableErrorsRegex,
										gson.toJson(rsyncResult)));
					}
				}
			
				
			// otherwise just use cp
			} else {
			
				CommandLine cpCmdLine = new CommandLine("cp");
				cpCmdLine.addArgument(sourceFilePath,false);
				cpCmdLine.addArgument(targetFilePath,false);
				
				CmdResult cpResult = exec(getRetries(),"cp",cpCmdLine,targetFilePath);
				commandsRun.add(cpResult);
				if (cpResult.getExitCode() > 0) {
					
					workerState.addTocPathWriteFailure(
							new TocPathOpResult(payload.mode, false, targetFilePath, cpCmdLine.toString(), gson.toJson(cpResult)));
					
					return; // exit
				}
			}
			
		}
		
		/********************
		 * HANDLE CHOWNS
		 * AND CHMOD for
		 * both files and dirs
		 * why? because w/ yas3fs
		 * "preserve" options do not
		 * properly carry through
		 * to s3, it needs to be explicit
		 *****************/

		/**
		 * CHOWN 
		 */
		boolean canChown = true;
		if (chownDirsOnly && !payload.tocInfo.isDirectory) {
			canChown = false;
		}
		
		CmdResult chownResult  = null;
		if (chown != null && canChown) {
			
			// chown -R x:y targetFilePath
			CommandLine chownCmdLine = new CommandLine("chown");
			chownCmdLine.addArgument(this.chown);
			chownCmdLine.addArgument(targetFilePath,false);
			
			chownResult = exec(1,"chown",chownCmdLine,targetFilePath);
			commandsRun.add(chownResult);
			if (chownResult.getExitCode() > 0) {
				workerState.addTocPathWriteFailure(
						new TocPathOpResult(payload.mode, false, targetFilePath, chownCmdLine.toString(), gson.toJson(chownResult)));
				return; // exit
			}
		}
		
		
		/**
		 * CHMOD
		 */
		boolean canChmod = true;
		if (chmodDirsOnly && !payload.tocInfo.isDirectory) {
			canChmod = false;
		}
		
		CmdResult chmodResult  = null;
		if (chmod != null && canChmod) {
			
			// chmod -R XXX targetFilePath
			CommandLine chmodCmdLine = new CommandLine("chmod");
			chmodCmdLine.addArgument(this.chmod);
			chmodCmdLine.addArgument(targetFilePath,false);
			
			chmodResult = exec(1,"chmod",chmodCmdLine,targetFilePath);
			commandsRun.add(chmodResult);
			if (chmodResult.getExitCode() > 0) {
				workerState.addTocPathWriteFailure(
						new TocPathOpResult(payload.mode, false, targetFilePath, chmodCmdLine.toString(), gson.toJson(chmodResult)));
				return; // exit
			}
			
		}
		
	
		/**
		 * Record success if we got here
		 */

		String asJson = gson.toJson(commandsRun.toArray());
		
		workerState.addTocPathWritten(
				new TocPathOpResult(payload.mode, true, targetFilePath, "mkdir + rsync + ?chown + ?chmod", asJson));

		
	}
	
	
	private CmdResult exec(int maxAttempts, 
						   String desc, 
						   CommandLine cmd, 
						   String retryExistancePathToCheck) {
		
		String cmdStr = null;
		CmdResult result = null;
		try {
			cmdStr = cmd.toString();

			File retryExistanceCheckFile = new File(retryExistancePathToCheck);
			int attempts = 0;
			
			while((attempts < maxAttempts) && 
				  (result == null || result.getExitCode() > 0 || !retryExistanceCheckFile.exists())) {
				
				attempts++;
				logger.debug("exec() attempt#: "+attempts+ " executing "+desc+": " + cmdStr);
				
				result = executor.execute(cmd,maxAttempts);
				
				// if fail, let it breathe
				if (result.getExitCode() > 0) {
					Thread.currentThread().sleep(getRetriesSleepMS());
				} 
			}

		} catch(Exception e) {
			String msg = "exec() "+desc+" unexpected exception: " +cmdStr + " " + e.getMessage();
			logger.error(msg,e);
			result = new CmdResult(5555, null, msg);
		}
		
		return result;
	}

	private boolean rsyncErrorIsTolerable(CmdResult result) {
		
		if (this.rsyncTolerableErrorsRegex == null) {
			return false;
		}
		
		String stdError = result.getStdErr();
		if (stdError != null && this.rysyncTolerableErrorPattern.matcher(stdError).matches()) {
			logger.debug("rsyncErrorIsTolerable? TRUE: " + stdError);
			return true;
		}
		
		String stdOut = result.getStdOut();
		if (stdOut != null && this.rysyncTolerableErrorPattern.matcher(stdOut).matches()) {
			logger.debug("rsyncErrorIsTolerable? TRUE: " + stdOut);
			return true;
		}
		
		return false;
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

	public void setChown(String chown) {
		this.chown = chown;
	}

	public void setChmod(String chmod) {
		this.chmod = chmod;
	}

	public boolean isUseRsync() {
		return useRsync;
	}

	public void setUseRsync(boolean useRsync) {
		this.useRsync = useRsync;
	}
	
	public void setChownDirsOnly(boolean dirsOnly) {
		this.chownDirsOnly = dirsOnly;
	}
	
	public void setChmodDirsOnly(boolean dirsOnly) {
		this.chmodDirsOnly = dirsOnly;
	}

	public String getRsyncOptions() {
		return rsyncOptions;
	}

	public void setRsyncOptions(String rsyncOptions) {
		this.rsyncOptions = rsyncOptions;
		for (String option : rsyncOptions.split(" ")) {
			this.rsyncOptionsList.add(option);
		}
	}

	public String getRsyncTolerableErrorsRegex() {
		return rsyncTolerableErrorsRegex;
	}

	public void setRsyncTolerableErrorsRegex(String rsyncTolerableErrorsRegex) throws PatternSyntaxException {
		this.rsyncTolerableErrorsRegex = rsyncTolerableErrorsRegex;
		this.rysyncTolerableErrorPattern = Pattern.compile(this.rsyncTolerableErrorsRegex);
		logger.debug("Set rsyncTolerableErrorsRegex="+rsyncTolerableErrorsRegex);
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public long getRetriesSleepMS() {
		return retriesSleepMS;
	}

	public void setRetriesSleepMS(long retriesSleepMS) {
		this.retriesSleepMS = retriesSleepMS;
	}
	
	
}
