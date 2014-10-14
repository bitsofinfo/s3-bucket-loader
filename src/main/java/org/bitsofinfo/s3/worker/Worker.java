package org.bitsofinfo.s3.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.log4j.Logger;
import org.bitsofinfo.s3.control.CCMode;
import org.bitsofinfo.s3.control.CCPayload;
import org.bitsofinfo.s3.control.CCPayloadHandler;
import org.bitsofinfo.s3.control.CCPayloadType;
import org.bitsofinfo.s3.control.ControlChannel;
import org.bitsofinfo.s3.toc.FileCopyTOCPayloadHandler;
import org.bitsofinfo.s3.toc.TOCPayload;
import org.bitsofinfo.s3.toc.TOCPayload.MODE;
import org.bitsofinfo.s3.toc.TOCPayloadHandler;
import org.bitsofinfo.s3.toc.TOCQueue;
import org.bitsofinfo.s3.toc.ValidatingTOCPayloadHandler;
import org.bitsofinfo.s3.util.CompressUtil;
import org.bitsofinfo.s3.yas3fs.Yas3fsS3UploadMonitor;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class Worker implements TOCPayloadHandler, CCPayloadHandler, Runnable {

	private static final Logger logger = Logger.getLogger(Worker.class);
	
	private List<TOCQueue> tocQueueConsumers = new ArrayList<TOCQueue>();
	
	private ControlChannel controlChannel = null;
	private Thread selfMonitorThread = null;
	private Map<MODE,TOCPayloadHandler> mode2TOCHandlerMap = null;
	
	// if no messages received on TOCQueue in 5m declare as idle
	private long declareWorkerIdleAtMinLastMsgReceivedMS = 60000; 
	
	private String sqsQueueName = null;
	private Integer totalConsumerThreads = null;
	private String awsAccessKey = null;
	private String awsSecretKey = null;
	private WorkerState myWorkerState = null;
	private Gson gson = new Gson();
	private WriteMonitor writeMonitor = null;
	
	private Properties properties = null;
	
	private long initializedLastSentAtMS = -1;
	
	private long sendCurrentSummariesEveryMS = 60000;
	private long currentSummaryLastSentAtMS = -1;
	
	public Worker(Properties props) {

		try {
			
			this.properties = props;
			
			String snsControlTopicName = props.getProperty("aws.sns.control.topic.name");
			this.awsAccessKey = 		 props.getProperty("aws.access.key");
			this.awsSecretKey = 		 props.getProperty("aws.secret.key");
			String userAccountPrincipalId = 	 props.getProperty("aws.account.principal.id");
			String userARN = 					 props.getProperty("aws.user.arn");

			this.sqsQueueName = 		 props.getProperty("aws.sqs.queue.name");
			
			this.totalConsumerThreads = 	Integer.valueOf(props.getProperty("worker.toc.consumer.threads.num"));
			
			mode2TOCHandlerMap = initTOCPayloadHandlers(props);
			
			// handle init command
			runInitOrDestroyCommand("initialize",props);
			
			// write monitor (optional)
			initWriteMonitor(props);
	
			// spawn control channel
			controlChannel = new ControlChannel(false, awsAccessKey, awsSecretKey, snsControlTopicName, userAccountPrincipalId, userARN, this);
			
			// for tracking my info
			this.myWorkerState = new WorkerState(
										controlChannel.getMySourceIdentifier(), 
										controlChannel.getMySourceIp());
			

			// let master know we are initialized
			myWorkerState.setCurrentMode(CCMode.INITIALIZED);
			sendInitializedState();
			
			// monitors our state...
			selfMonitorThread = new Thread(this);
			selfMonitorThread.start();
			
		} catch(Exception e) {
			logger.error("Worker() unexpected error: " + e.getMessage(),e);
			destroy();
		}
	}
	
	private void sendInitializedState() throws Exception{
		this.initializedLastSentAtMS = System.currentTimeMillis();
		this.controlChannel.send(false, CCPayloadType.WORKER_CURRENT_MODE, CCMode.INITIALIZED);
	}
	
	private void runInitOrDestroyCommand(String mode, Properties props) throws Exception {
		// Initialization command and environment vars
		String initCmd 		= 	props.getProperty("worker."+mode+".cmd");
		String initCmdEnv 	= 	props.getProperty("worker."+mode+".cmd.env");
		
		if (initCmd != null) {
			
			Map<String,String> env = null;
			if (initCmdEnv != null) {
				env = Splitter.on(",").withKeyValueSeparator("=").split(initCmdEnv);
			}
			
			// execute it!
			logger.debug("Running "+mode+" command: " + initCmd);
			CommandLine cmdLine = CommandLine.parse(initCmd);
			DefaultExecutor executor = new DefaultExecutor();
			executor.execute(cmdLine, env);
			
		}
	}
	
	private void initWriteMonitor(Properties props) throws Exception {
		String writeMonitorClass = props.getProperty("worker.write.complete.monitor.class");
		if (writeMonitorClass != null) {
			logger.debug("Attempting to create Monitor: " + writeMonitorClass);
			this.writeMonitor = (WriteMonitor)Class.forName(writeMonitorClass).newInstance();
			
			if (writeMonitor instanceof Yas3fsS3UploadMonitor) {
				Yas3fsS3UploadMonitor m = (Yas3fsS3UploadMonitor)writeMonitor;
				m.setIsIdleWhenNZeroUploads(10); // the monitor must state 10 consecutive cycles of no s3 uploads to be "idle"
				m.setCheckEveryMS(Long.valueOf(props.getProperty("worker.write.complete.monitor.yas3fs.checkEveryMS")));
				m.setPathToLogFile(props.getProperty("worker.write.complete.monitor.yas3fs.logFilePath"));
			}
		}
	}
	
	public void startConsuming() {
		for (TOCQueue consumer : tocQueueConsumers) {
			consumer.start();
		}
	}
	
	public void pauseConsuming() {
		for (TOCQueue consumer : tocQueueConsumers) {
			consumer.pauseConsuming();
		}
	}
	
	public void resumeConsuming() {
		for (TOCQueue consumer : tocQueueConsumers) {
			consumer.resumeConsuming();
		}
	}
	
	public void destroy() {
		
		try { 
			writeMonitor.destroy();
		} catch(Exception ignore){}
		
		try { 
			controlChannel.destroy();
		} catch(Exception ignore){}
		
		for (TOCQueue consumer : tocQueueConsumers) {
			consumer.stopConsuming();
		}
		
		try {
			Thread.currentThread().sleep(30000);
			
			for (TOCQueue consumer : tocQueueConsumers) {
				consumer.destroy();
			}
		} catch(Exception ignore){}
		
		try {
			runInitOrDestroyCommand("destroy",this.properties);
		} catch(Exception ignore){}
		
	}


	public void handlePayload(CCPayload payload) throws Exception {
		
		// we only care about master payloads and stuff from other than us
		if (!payload.fromMaster) {
			return;
		}
		
		// ignore messages targeted for someone other than ourself
		if (payload.onlyForHostIdOrIP != null && 
			!payload.onlyForHostIdOrIP.equalsIgnoreCase(this.myWorkerState.getWorkerHostSourceId()) &&
			!payload.onlyForHostIdOrIP.equalsIgnoreCase(this.myWorkerState.getWorkerIP())) {
			return;
		}
		
		logger.info("handlePayload() received CCPayload: fromMaster: " + payload.fromMaster + 
				" sourceHostId:" + payload.sourceHostId + 
				" sourceHostIp:" + payload.sourceHostIP + 
				" onlyForHost:" + payload.onlyForHostIdOrIP + 
				" type:" + payload.type +
				" value:" + payload.value);
		
		// the mode that the master reports we should switch to
		if (payload.type == CCPayloadType.MASTER_CURRENT_MODE) {
			
			CCMode masterMode = CCMode.valueOf(payload.value.toString());
			
			
			// do we need to change our mode?
			if (myWorkerState.getCurrentMode() != masterMode) {
			
				// set it
				myWorkerState.setCurrentMode(masterMode);
				
				// if we are now WRITE/VALIDATE mode ensure we spawn our threads
				if (myWorkerState.getCurrentMode() == CCMode.WRITE || myWorkerState.getCurrentMode() == CCMode.VALIDATE) {
					
					if (this.tocQueueConsumers.size() == 0) {
					
						logger.debug("CCMode switched to mode "+myWorkerState.getCurrentMode()+": Worker spawing " + totalConsumerThreads + " separate TOCQueue consumer threads...");
						for (int i=0; i<totalConsumerThreads; i++) {
							tocQueueConsumers.add(new TOCQueue(true, awsAccessKey, awsSecretKey, sqsQueueName, this));
						}
						
						// start the queue threads
						this.startConsuming();
						
						// if we have an additional monitor configured start that too
						if (this.writeMonitor != null) {
							this.writeMonitor.start();
						}
					
					// we have existing threads, resume consumption
					} else {
						this.resumeConsuming();
					}
				}
				
				
				// if now in validate mode, destroy the write monitor...
				if (myWorkerState.getCurrentMode() == CCMode.VALIDATE) {
					if (this.writeMonitor != null) {
						this.writeMonitor.destroy();
					}
				}
				
				// if now in REPORT_ERRORS mode...
				if (myWorkerState.getCurrentMode() == CCMode.REPORT_ERRORS) {
					
					// ensure we are paused consuming..
					this.pauseConsuming();
					
					// build report...
					ErrorReport errorReport = new ErrorReport();
					errorReport.failedValidates = myWorkerState.getTocPathValidateFailures();
					errorReport.failedWrites = myWorkerState.getTocPathsWriteFailures();
					errorReport.errorsTolerated = myWorkerState.getTocPathsErrorsTolerated();
				
					// convert to json
					String errorReportJson = new GsonBuilder().setPrettyPrinting().create().toJson(errorReport);
					
					// compress...
					String compressedPayload = new String(CompressUtil.compressAndB64EncodeASCIIChars(errorReportJson.toCharArray()));
					
					// send to control channel
					this.controlChannel.send(false, CCPayloadType.WORKER_ERROR_REPORT_DETAILS, compressedPayload);

				}
			}
			
		}
		
		// if the master tells us to shutdown
		if (payload.type == CCPayloadType.CMD_WORKER_SHUTDOWN) {
			System.exit(0); // this will trigger shutdown hook
		}
	}

	public void handlePayload(TOCPayload payload) throws Exception {
		logger.info("handlePayload() received TOCPayload: mode: "+payload.mode + " filePath:" + payload.tocInfo.getPath());

		TOCPayloadHandler handler = this.mode2TOCHandlerMap.get(payload.mode);
		
		if (handler == null) {
			throw new Exception("Cannot handle payload: " + payload.mode + " no TOCPayloadHandler configured for this MODE!");
		}
		
		handler.handlePayload(payload,this.myWorkerState);
	}
	
	private Map<MODE,TOCPayloadHandler> initTOCPayloadHandlers(Properties props) throws Exception {
		String writeClazz = props.getProperty("tocPayloadHandler.write.class");
		String validateClazz = props.getProperty("tocPayloadHandler.validate.class");
		
		Map<MODE, TOCPayloadHandler> map = new HashMap<MODE,TOCPayloadHandler>();
		map.put(MODE.WRITE, initTOCPayloadHandler(writeClazz,props));
		map.put(MODE.VALIDATE, initTOCPayloadHandler(validateClazz,props));
		
		return map;
		
	}
	
	private TOCPayloadHandler initTOCPayloadHandler(String className, Properties props) throws Exception {
		
		TOCPayloadHandler handler = (TOCPayloadHandler)Class.forName(className).newInstance();
		
		
		/**
		 * FileCopyTOCPayloadHandler
		 */
		if (handler instanceof FileCopyTOCPayloadHandler) {
			
			FileCopyTOCPayloadHandler fcHandler = (FileCopyTOCPayloadHandler)handler;
			
			fcHandler.setSourceDirectoryRootPath(props.getProperty("tocPayloadHandler.source.dir.root"));
			
			fcHandler.setTargetDirectoryRootPath(props.getProperty("tocPayloadHandler.target.dir.root"));
			
			fcHandler.setUseRsync(Boolean.valueOf(props.getProperty("tocPayloadHandler.write.use.rsync")));
			
			if (props.getProperty("tocPayloadHandler.write.rsync.tolerable.error.regex") != null) {
				fcHandler.setRsyncTolerableErrorsRegex((String)props.getProperty("tocPayloadHandler.write.rsync.tolerable.error.regex"));
			}
			
			if (fcHandler.isUseRsync()) {
				fcHandler.setRsyncOptions(props.getProperty("tocPayloadHandler.write.rsync.options"));
			}
			
			String chmod = props.getProperty("tocPayloadHandler.write.chmod");
			if (chmod != null) {
				boolean dirsOnly = Boolean.valueOf(props.getProperty("tocPayloadHandler.write.chmod.dirsOnly"));
				fcHandler.setChmod(chmod);
				fcHandler.setChmodDirsOnly(dirsOnly);
			}
			
			String chown = props.getProperty("tocPayloadHandler.write.chown");
			if (chown != null) {
				boolean dirsOnly = Boolean.valueOf(props.getProperty("tocPayloadHandler.write.chown.dirsOnly"));
				fcHandler.setChown(chown);
				fcHandler.setChownDirsOnly(dirsOnly);
			}
			
			return handler;
		}
		
		
		
		/**
		 * ValidatingTOCPayloadHandler
		 */
		if (handler instanceof ValidatingTOCPayloadHandler) {
			
			((ValidatingTOCPayloadHandler)handler)
				.setTargetDirectoryRootPath(props.getProperty("tocPayloadHandler.target.dir.root"));
			
			return handler;
		}
		

		throw new Exception("initTOCPayloadHandler() invalid tocPayloadHandler.class");

	}
	

	private String getResultsSummaryAsJSON(MODE mode) {
		
		if (mode == MODE.WRITE) {
			ResultSummary writeSummary = new ResultSummary(myWorkerState.getTotalWritesOK(), 
														   myWorkerState.getTotalWritesFailed(), 
														   myWorkerState.getTotalErrorsTolerated(),
														   myWorkerState.getTotalWritesProcessed());
	
			return gson.toJson(writeSummary);
			
			
		} else if (mode == MODE.VALIDATE) {
			
			ResultSummary validateSummary = new ResultSummary(myWorkerState.getTotalValidatesOK(), 
					   									  	  myWorkerState.getTotalValidatesFailed(), 
					   									  	  myWorkerState.getTotalErrorsTolerated(),
					   									  	  myWorkerState.getTotalValidationsProcessed());

			return gson.toJson(validateSummary);
		}
		
		throw new RuntimeException("getResultsSummaryAsJSON() called with invalid MODE: " + mode);
	}
	
	public void run() {
		
		boolean running = true;
		
		// Here we monitor our WorkerState
		// to determine where we are at
		while (running) {
			try {

				Thread.currentThread().sleep(10000);
				
				// if just in Initialized/Idle state do nothing.
				if (this.myWorkerState.getCurrentMode() == CCMode.INITIALIZED ||
					this.myWorkerState.getCurrentMode() == CCMode.IDLE ) {
					
					// if we are just in INITIALIZED state, and still are after 30s, resend it
					// as we have seen worker initializd messages not get delivered...
					long now = System.currentTimeMillis();
					if (this.myWorkerState.getCurrentMode() == CCMode.INITIALIZED && 
						this.initializedLastSentAtMS > 0 	&& 
						(now - this.initializedLastSentAtMS > 60000)) {
						
						logger.debug("Resending INITIALIZED state to master......");
						this.sendInitializedState();
					}
					
					continue;
				}
				
				/**
				 * WRITES DONE?
				 */
				if (this.myWorkerState.getCurrentMode() == CCMode.WRITE) {

					// determine TOCQueue threads who have been idle long enough to proceed to a state change assumption
					int threadsThatQualify = getIdleTOCQueueThreads();
					
					// ok all threads are created AND idle, send our summary...as we can only assume we are done
					if (this.tocQueueConsumers.size() == this.totalConsumerThreads && 
						threadsThatQualify >= this.tocQueueConsumers.size()) {
						
						// if the write monitor IS configured and states we CANNOT proceed...
						// then just exit/continue...
						if (writeMonitor != null && !writeMonitor.writesAreComplete()) {
							continue;
						}
						
						String asJson = getResultsSummaryAsJSON(MODE.WRITE);
						
						// pause
						this.pauseConsuming();
						
						// send out our summary
						this.controlChannel.send(false, CCPayloadType.WORKER_WRITES_FINISHED_SUMMARY, asJson);
						
						// state we are IDLE
						this.myWorkerState.setCurrentMode(CCMode.IDLE);
						this.controlChannel.send(false, CCPayloadType.WORKER_CURRENT_MODE, CCMode.IDLE);
						
						
					// not finished but lets send a CURRENT_SUMMARY, if necessary
					} else {
						long now = System.currentTimeMillis();;
						if ((now - this.currentSummaryLastSentAtMS) > this.sendCurrentSummariesEveryMS) {
							// send out our summary
							this.currentSummaryLastSentAtMS = now;
							String asJson = getResultsSummaryAsJSON(MODE.WRITE);
							this.controlChannel.send(false, CCPayloadType.WORKER_WRITES_CURRENT_SUMMARY, asJson);
						}
					}
				}
				
				
				
				/**
				 * VALIDATES DONE?
				 */
				if (this.myWorkerState.getCurrentMode() == CCMode.VALIDATE) {
					
					// determine TOCQueue threads who have been idle long enough to proceed to a state change assumption
					int threadsThatQualify = getIdleTOCQueueThreads();
					
					// ok all threads are created AND idle, send our summary...as we can only assume we are done
					if (this.tocQueueConsumers.size() == this.totalConsumerThreads &&
						threadsThatQualify >= this.tocQueueConsumers.size()) {
						
						String asJson = getResultsSummaryAsJSON(MODE.VALIDATE);
						
						// pause
						this.pauseConsuming();
						
						// send out our summary
						this.controlChannel.send(false, CCPayloadType.WORKER_VALIDATIONS_FINISHED_SUMMARY, asJson);

						// state we are IDLE
						this.myWorkerState.setCurrentMode(CCMode.IDLE);
						this.controlChannel.send(false, CCPayloadType.WORKER_CURRENT_MODE, CCMode.IDLE);
						
						
					// not finished but lets send a CURRENT_SUMMARY
					} else {
						long now = System.currentTimeMillis();;
						if ((now - this.currentSummaryLastSentAtMS) > this.sendCurrentSummariesEveryMS) {
							// send out our summary
							this.currentSummaryLastSentAtMS = now;
							String asJson = getResultsSummaryAsJSON(MODE.VALIDATE);
							this.controlChannel.send(false, CCPayloadType.WORKER_VALIDATIONS_CURRENT_SUMMARY, asJson);
						}
					}
				}
				
			} catch(Exception e) {
				logger.error("run() unexpected error: " + e.getMessage(),e);
			}
		}
	}

	private int getIdleTOCQueueThreads() {
		int threadsThatQualify = 0;
		for (TOCQueue tocQueue : this.tocQueueConsumers) {
			
			// not even connected/ready yet
			if (tocQueue.getLastMsgReceivedTimeMS() == -1) {
				continue;
			}
			
			long lastMsgReceivedAtMS = (System.currentTimeMillis() - tocQueue.getLastMsgReceivedTimeMS());
			if (lastMsgReceivedAtMS >= this.declareWorkerIdleAtMinLastMsgReceivedMS) {
				threadsThatQualify++;
			}
		}
		return threadsThatQualify;
	}

	public void handlePayload(TOCPayload payload,WorkerState workerState) throws Exception {
		this.handlePayload(payload);
	}
}
