package org.bitsofinfo.s3.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.bitsofinfo.ec2.Ec2Util;
import org.bitsofinfo.s3.control.CCMode;
import org.bitsofinfo.s3.control.CCPayload;
import org.bitsofinfo.s3.control.CCPayloadHandler;
import org.bitsofinfo.s3.control.CCPayloadType;
import org.bitsofinfo.s3.control.ControlChannel;
import org.bitsofinfo.s3.toc.DirectoryCrawler;
import org.bitsofinfo.s3.toc.FileInfo;
import org.bitsofinfo.s3.toc.SourceTOCGenerator;
import org.bitsofinfo.s3.toc.TOCPayload;
import org.bitsofinfo.s3.toc.TOCPayload.MODE;
import org.bitsofinfo.s3.toc.TOCQueue;
import org.bitsofinfo.s3.util.CompressUtil;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import com.google.gson.Gson;


public class Master implements CCPayloadHandler, Runnable {

	private static final Logger logger = Logger.getLogger(Master.class);

	
	private TOCQueue tocQueue = null;
	private ControlChannel controlChannel = null;
	private Properties props = null;
	
	private Set<FileInfo> toc = null;
	
	private WorkerRegistry workerRegistry = new WorkerRegistry();
	private int totalExpectedWorkers = 0;
	
	private CCMode currentMode = null;
	
	private Gson gson = new Gson();
	private Date masterStartAt = null;
	private String awsAccessKey = null;
	private String awsSecretKey = null;
	
	private boolean workersEc2Managed = false;
	private AmazonEC2Client ec2Client = null;
	private List<Instance> ec2Instances = null;
	
	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
	private SimpleDateFormat ec2TagSimpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
	
	private Thread masterMonitor = null;
	private boolean masterMonitorRunning = true;
	
	private TOCFileInfoQueueSender tocFileInfoQueueSender = null;
	private int tocDispatchThreadsTotal = 4;
	
	private Ec2Util ec2util = null;

	public Master(Properties props) {
		
		try {
			this.ec2util = new Ec2Util(); 
			
			this.props = props;

			String snsControlTopicName = props.getProperty("aws.sns.control.topic.name");
			String sqsQueueName = 		 props.getProperty("aws.sqs.queue.name");
			String userAccountPrincipalId =   props.getProperty("aws.account.principal.id");
			String userARN = 				  props.getProperty("aws.user.arn");
			this.totalExpectedWorkers = 	Integer.valueOf(props.getProperty("master.workers.total"));

			this.awsAccessKey = 		 props.getProperty("aws.access.key");
			this.awsSecretKey = 		 props.getProperty("aws.secret.key");
			
			this.tocDispatchThreadsTotal = Integer.valueOf(props.getProperty("master.tocqueue.dispatch.threads"));

			tocQueue = 		 new TOCQueue(false, awsAccessKey, awsSecretKey, sqsQueueName, null);
			controlChannel = new ControlChannel(true, awsAccessKey, awsSecretKey, snsControlTopicName, userAccountPrincipalId, userARN, this);
			
			totalExpectedWorkers = Integer.valueOf(props.getProperty("master.workers.total"));
			
			workersEc2Managed = Boolean.valueOf(props.getProperty("master.workers.ec2.managed"));

		} catch(Exception e) {
			logger.error("Master() unexpected error: " + e.getMessage(),e);
			try {
				destroy();
			} catch(Exception ignore){}
		}
	}
	
	private void spawnEC2() throws Exception {
		
		if (!workersEc2Managed) {
			return;
		}
		
		// connect to ec2
		this.ec2Client = new AmazonEC2Client(new BasicAWSCredentials(this.awsAccessKey, this.awsSecretKey));
		
		this.ec2Instances = ec2util.launchEc2Instances(ec2Client, props);
		
		// collect some info
		StringBuffer sb = new StringBuffer("EC2 instance request created, reservation details:\n");
		for (Instance instance : ec2Instances) {
			sb.append("\tid:" + instance.getInstanceId() + "\tip:" + instance.getPrivateIpAddress() + "\thostname:"+instance.getPrivateDnsName()+"\n");
		}
		logger.info(sb.toString()+"\n");
		
		Thread.currentThread().sleep(60000);
		
		// tag instances
		CreateTagsRequest tagRequest = new CreateTagsRequest();
		String dateStr = ec2TagSimpleDateFormat.format(new Date());
		
		for (Instance instance : this.ec2Instances) {
			List<String> amiIds = new ArrayList<String>();
			List<Tag> tags = new ArrayList<Tag>();
			
			amiIds.add(instance.getInstanceId());
			tags.add(new Tag("Name","s3BktLdr-wkr-"+dateStr));
			
			tagRequest.setResources(amiIds);
			tagRequest.setTags(tags);
			
			ec2Client.createTags(tagRequest);
		}

		

	}
	
	
	public void start() throws Exception {
		
		// seed start
		this.masterStartAt = new Date();
		
		// initialize workers 
		this.currentMode = CCMode.INITIALIZED;
		this.controlChannel.send(true, CCPayloadType.MASTER_CURRENT_MODE, this.currentMode);
		
		// spawn ec2 cluster if specified
		spawnEC2();
		
		// start our own monitor
		this.masterMonitor = new Thread(this);
		this.masterMonitor.start();
		
		// @see handlePayload(CCPayload) for what happens next as workers come online
		
	}

	public Set<FileInfo> generateTOC(TOCPayload.MODE payloadMode, Queue<FileInfo> tocFileInfoQueue) throws Exception {
		
		SourceTOCGenerator tocGenerator = getSourceTOCGenerator(props);

		// generate TOC 
		logger.info("getTOC("+payloadMode+") generating TOC...");
		return tocGenerator.generateTOC(tocFileInfoQueue);

	}
	
	public void destroy() throws Exception {
		
		try {
			// just in case
			this.controlChannel.send(true, CCPayloadType.CMD_WORKER_SHUTDOWN, null);
		} catch(Exception ignore){}
		
		try {
			tocFileInfoQueueSender.destroy();
		} catch(Exception ignore){}

		Thread.currentThread().sleep(10000);

		try { 
			logger.debug("Calling TOCQueue.destroy()");
			tocQueue.destroy();
		} catch(Exception ignore){}
		
		try { 
			logger.debug("Calling ControlChannel.destroy()");
			controlChannel.destroy();
		} catch(Exception ignore){}
		
		
		this.masterMonitorRunning = false;
		
		Thread.currentThread().sleep(10000);

		if (ec2Instances != null) {
			try {
				for (Instance ec2 : this.ec2Instances) {
					ec2util.terminateEc2Instance(ec2Client, ec2.getInstanceId());
				}
			
			} catch(Exception e){
				logger.error("Error terminating instances: " + e.getMessage(),e);
			}
		}


	}

	
	private SourceTOCGenerator getSourceTOCGenerator(Properties props) throws Exception {
		SourceTOCGenerator tocGenerator = (SourceTOCGenerator)Class.forName(props.getProperty("tocGenerator.class").toString()).newInstance();
		configureTocGenerator(tocGenerator,props);
		return tocGenerator;
	}
	
	private void configureTocGenerator(SourceTOCGenerator generator, Properties props) {
		if (generator instanceof DirectoryCrawler) {
			((DirectoryCrawler)generator).setRootDir(new File(props.getProperty("tocGenerator.source.dir").toString()));
		}
	}


	public void handlePayload(CCPayload payload) {
		
		// ignore messages from ourself
		if (payload.fromMaster) {
			return;
		}
		
		logger.info("handlePayload() received CCPayload: fromMaster: " + payload.fromMaster + 
				" sourceHostId:" + payload.sourceHostId + 
				" sourceHostIp:" + payload.sourceHostIP + 
				" onlyForHost:" + payload.onlyForHostIdOrIP + 
				" type:" + payload.type +
				" value:" + payload.value);
		
		// if from worker....
		if (!payload.fromMaster) {
			workerRegistry.registerWorkerPayload(payload);
		}
		
		// check for all workers in INITIALIZED mode
		if (currentMode == CCMode.INITIALIZED && 
			workerRegistry.size() == this.totalExpectedWorkers &&
			workerRegistry.allWorkersCurrentModeIs(CCMode.INITIALIZED)) {
			
			try {
				logger.info("All workers report INITIALIZED.. total: " + workerRegistry.size() + " Proceeding to build TOC....");

				// execute WRITE mode
				this.currentMode = CCMode.WRITE;
				
				// generate a queue that the "sender" will concurrently consume
				// from while the TOC is being generated
				Queue<FileInfo> tocFileInfoQueue = new ConcurrentLinkedQueue<FileInfo>();
				this.tocFileInfoQueueSender = new TOCFileInfoQueueSender(MODE.WRITE, this.tocQueue, this.tocDispatchThreadsTotal, tocFileInfoQueue);
				
				// switch the system to WRITE mode so workers can immediately start polling
				logger.info("Switching to WRITE mode....");
				controlChannel.send(true, CCPayloadType.MASTER_CURRENT_MODE, this.currentMode);
				
				// generate and get all TOC messages (write live to the queue we just created)
				tocFileInfoQueueSender.start(); // start the consumer
				this.toc = generateTOC(MODE.WRITE, tocFileInfoQueue); // begin the scan and write
				
				// while the queue is not empty, sleep....
				while (tocFileInfoQueue.size() > 0) {
					logger.debug("TOC generation complete, waiting for TOCFileInfoQueueSender" +
							" thread to complete sending to SQS.. size:" + tocFileInfoQueue.size());
					Thread.currentThread().sleep(10000);
				}
				
				// queue is empty.. stop it
				tocFileInfoQueueSender.destroy();

				logger.info("Master(currentMode:"+currentMode+") done sending " + toc.size() + " filePaths mode:"+MODE.WRITE+" to TOCQueue....");
				

			} catch(Exception e) {
				logger.error("handlePayload() error handling WRITE mode completion and" +
						" triggerring VALIDATE mode..." + e.getMessage(),e);
			}
			
		// initialized, but still waiting....
		} else if (currentMode == CCMode.INITIALIZED && workerRegistry.size() != this.totalExpectedWorkers) {
			logger.info("Total workers registered = " + workerRegistry.size() + " expected:"+this.totalExpectedWorkers);
		}
		
		
		// Check for WRITE complete
		if (currentMode == CCMode.WRITE && workerRegistry.allWorkerWritesAreComplete()) {
			try {
				// were there any errors?
				if (workerRegistry.anyWorkerWritesContainErrors()) {
	
					logger.info("One or more workers report WRITE mode completed.. but with some ERRORS. totalWritten: " + 
							workerRegistry.getTotalWritten() + " total TOC sent: " + this.toc.size() + 
							"(expected size). I am now triggering REPORT_ERRORS mode across all workers");
					
					this.currentMode = CCMode.REPORT_ERRORS;
					
					// switch the system to REPORT_ERRORS mode
					controlChannel.send(true, CCPayloadType.MASTER_CURRENT_MODE, this.currentMode);
					
					
				// no errors move onto next phase....(validate)
				} else {
					logger.info("All workers report WRITE mode completed.. totalWritten: " + 
							workerRegistry.getTotalWritten() + " total TOC sent: " + this.toc.size() + 
							"(expected size). I am now triggering VALIDATE mode across all workers");
					
					// put our cached TOC into a conccurent queue
					Queue<FileInfo> tocFileInfoQueue = new ConcurrentLinkedQueue<FileInfo>();
					tocFileInfoQueue.addAll(this.toc);
					
					// switch the system to VALIDATE mode so workers can immediately start polling
					logger.info("Switching to VALIDATE mode....");
					this.currentMode = CCMode.VALIDATE;
					controlChannel.send(true, CCPayloadType.MASTER_CURRENT_MODE, this.currentMode);
					
					// create a TOCFileInfoQueueSender to send out messages concurrently via TOC cache...
					this.tocFileInfoQueueSender = new TOCFileInfoQueueSender(MODE.VALIDATE, this.tocQueue, this.tocDispatchThreadsTotal, tocFileInfoQueue);
					this.tocFileInfoQueueSender.start();
					
					// while the queue is not empty, sleep....
					while (tocFileInfoQueue.size() > 0) {
						logger.debug("VALIDATE mode: waiting for TOCFileInfoQueueSender " +
								"thread to complete sending to SQS.. size:" + tocFileInfoQueue.size());
						Thread.currentThread().sleep(10000);
					}
					
					// queue is empty.. stop it
					tocFileInfoQueueSender.destroy();
					
					logger.info("Master(currentMode:"+currentMode+") done sending " + toc.size() + " filePaths mode:"+MODE.VALIDATE+" to TOCQueue....");
					
				}
				
			} catch(Exception e) {
				logger.error("handlePayload() error handling WRITE mode completion..." + e.getMessage(),e);
			}
			
		// WRITE partially complete? dump the workers we are waiting on....
		} else if (currentMode == CCMode.WRITE && workerRegistry.anyWorkerWritesAreComplete()) {
			StringBuffer waitingSB = new StringBuffer("\nWe are awaiting WRITE reports from the following workers:\n");
			for (String awaiting : workerRegistry.getWorkersAwaitingWriteReport()) {
				waitingSB.append(awaiting+"\n");
			}
			logger.info(waitingSB.toString()+"\n");
		}
		
		
		// Check for WRITE complete
		if (currentMode == CCMode.VALIDATE && workerRegistry.allWorkerValidatesAreComplete()) {
			
			try {
				// were there any errors?
				if (workerRegistry.anyWorkerValidationsContainErrors()) {
	
					logger.info("One or more workers report VALIDATE mode completed.. but with some ERRORS. totalValidated: " + 
							workerRegistry.getTotalValidated() + " total TOC sent: " + this.toc.size() + 
							"(expected size). I am now triggering REPORT_ERRORS mode across all workers");
					
					this.currentMode = CCMode.REPORT_ERRORS;
					
					// switch the system to REPORT_ERRORS mode
					controlChannel.send(true, CCPayloadType.MASTER_CURRENT_MODE, this.currentMode);
					
					
				// no errors, go to shutdown!
				} else {
					logger.info("All workers report VALIDATE mode completed.. totalValidated: " + 
							workerRegistry.getTotalValidated() + " total TOC sent: " + this.toc.size() + 
							" I am now issueing CMD_WORKER_SHUTDOWN");
					
					// dump runtime
					logger.info("MASTER RUNTIME:  START["+simpleDateFormat.format(masterStartAt)+"] --> END["+simpleDateFormat.format(new Date())+"]");
					
					this.controlChannel.send(true, CCPayloadType.CMD_WORKER_SHUTDOWN, null);
				}

			} catch(Exception e) {
				logger.error("handlePayload() error sending CMD_WORKER_SHUTDOWN over control channel " + e.getMessage(),e);
			}
			
		// VALIDATE partially complete? dump the workers we are waiting on....
		} else if (currentMode == CCMode.VALIDATE && workerRegistry.anyWorkerValidatesAreComplete()) {
			StringBuffer waitingSB = new StringBuffer("\nWe are awaiting VALIDATE reports from the following workers:\n");
			for (String awaiting : workerRegistry.getWorkersAwaitingValidationReport()) {
				waitingSB.append(awaiting+"\n");
			}
			logger.info(waitingSB.toString()+"\n");
		}
		
		
		// Check for REPORT_ERRORS complete
		if (currentMode == CCMode.REPORT_ERRORS && workerRegistry.allWorkerErrorReportsAreIn()) {
			
			try {
				
				logger.info("All workers report REPORT_ERRORS mode completed.. dumping details and triggering system CMD_WORKER_SHUTDOWN");
				
				StringBuffer sb = new StringBuffer();
				for (String worker : workerRegistry.getWorkerHostnames()) {
					sb.append("\n--------------------------------------------------\n");
					sb.append("WORKER: "+worker+"\n");
					sb.append("--------------------------------------------------\n");
					
					WorkerInfo winfo = workerRegistry.getWorkerInfo(worker);
					String reportPayloadValue = (String)winfo.getPayloadValue(CCPayloadType.WORKER_ERROR_REPORT_DETAILS);
					
					// decompress...
					String errorReportJson = new String(CompressUtil.decompressAndB64DecodeASCIIChars(reportPayloadValue.toCharArray()));
					
					sb.append("\n"+errorReportJson+"\n");
					
					sb.append("--------------------------------------------------\n");
					
					logger.error(sb.toString());
				}
				
				// dump runtime
				logger.info("MASTER RUNTIME:  START["+simpleDateFormat.format(masterStartAt)+"] --> END["+simpleDateFormat.format(new Date())+"]");
				
				// send out the shutdown..
				this.controlChannel.send(true, CCPayloadType.CMD_WORKER_SHUTDOWN, null);

			} catch(Exception e) {
				logger.error("handlePayload() error sending CMD_WORKER_SHUTDOWN over control channel: " + e.getMessage(),e);
			}
			
		// REPORT_ERRORS partially complete? dump the workers we are waiting on....
		} else if (currentMode == CCMode.REPORT_ERRORS && workerRegistry.anyWorkerErrorReportsAreReceived()) {
			StringBuffer waitingSB = new StringBuffer("\nWe are awaiting REPORT_ERRORS reports from the following workers:\n");
			for (String awaiting : workerRegistry.getWorkersAwaitingErrorReport()) {
				waitingSB.append(awaiting+"\n");
			}
			logger.info(waitingSB.toString()+"\n");
		}
	}
	
	public void run() {
		while(masterMonitorRunning) {
			try {
				if (this.currentMode == CCMode.INITIALIZED && workersEc2Managed) {
					
					ec2util.dumpEc2InstanceStatus(ec2Client,ec2Instances);
					
					List<String> ec2InstanceIdsNoInitializedWorker = new ArrayList<String>();
					Map<String,String> instanceId2IP = ec2util.getPrivateIPs(ec2Instances);
					for (String ec2InstanceId : instanceId2IP.keySet()) {
						String ec2IP = instanceId2IP.get(ec2InstanceId);
						if (workerRegistry.getWorkerByIP(ec2IP) == null) {
							logger.warn("EC2 node: " + ec2IP + " has yet to register its worker...");
							ec2InstanceIdsNoInitializedWorker.add(ec2InstanceId);
						}
					}
					
					// if its been more than 5 minutes since we started and we STILL
					// have ec2 instances that have yet to report their worker, kill THEM!
					// and decrement expected workers so the next INITIALIZE resend by workers
					// will get us moving forward.
					if (System.currentTimeMillis() - this.masterStartAt.getTime() > (60000*5)) {
						logger.debug("Its been more than 5 minutes since we've started and are short workers... terminating them");
						for (String instanceId2term : ec2InstanceIdsNoInitializedWorker) {
							
							// issue shutdown to them only first
							String ip = instanceId2IP.get(instanceId2term);
							this.controlChannel.send(true, CCPayloadType.CMD_WORKER_SHUTDOWN, ip, null);
							
							ec2util.terminateEc2Instance(ec2Client,instanceId2term);
							this.totalExpectedWorkers--;
							logger.debug("Total expected workers is now: " + this.totalExpectedWorkers);
						}
					}
					
				}
				
				Thread.currentThread().sleep(30000);
			} catch(Exception e) {
				logger.error("Unexpected error: " + e.getMessage(),e);
			}
		}
	}
	
}
