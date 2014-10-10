package org.bitsofinfo.s3.toc;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.bitsofinfo.s3.toc.TOCPayload.MODE;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.gson.Gson;

public class TOCQueue implements Runnable {
	
	private static final Logger logger = Logger.getLogger(TOCQueue.class);
	
	private AmazonSQSClient sqsClient = null;
	private String tocQueueUrl = null;
	
	private Thread consumerThread = null;
	
	private Gson gson = new Gson();
	
	private TOCPayloadHandler tocPayloadHandler = null;
	private String mySourceIdentifier = null;
	private boolean canDestroyQueue = false;
	private String sqsQueueName = null;
	private String myId = UUID.randomUUID().toString().replaceAll("-", "").substring(0,6);
	private boolean running = true;
	private boolean paused = false;
	
	private long lastSQSMessageReceivedMS = -1;
	private int totalMessagesProcessed = 0;
	
	public TOCQueue(boolean isConsumer, String awsAccessKey, String awsSecretKey, String sqsQueueName, TOCPayloadHandler tocPayloadHandler) throws Exception {
		super();

		mySourceIdentifier = determineHostName() + "-" + UUID.randomUUID().toString().replace("-", "").substring(0,4);
		this.sqsQueueName = sqsQueueName;
		this.tocPayloadHandler = tocPayloadHandler;
		
		if (!isConsumer) { // then I am the master...
			canDestroyQueue = true; 
			this.sqsQueueName += "-" + mySourceIdentifier;
		}
		

		sqsClient = new AmazonSQSClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));

		
		connectToQueue(isConsumer, 1000);
		
		if (isConsumer) {
			this.consumerThread = new Thread(this,"TOCQueue["+myId+"] msg consumer thread");
		}
		
		logger.info("\n-------------------------------------------\n" +
					"TOC Queue["+myId+"]: ALL SQS resources hooked up OK: "+this.tocQueueUrl+"\n" +
					"-------------------------------------------\n");
	}
	
	public Long getLastMsgReceivedTimeMS() {
		return this.lastSQSMessageReceivedMS;
	}
	
	public void start() {
		this.consumerThread.start();
	}
	
	public void pauseConsuming() {
		this.paused = true;
	}
	
	public void resumeConsuming() {
		this.lastSQSMessageReceivedMS = System.currentTimeMillis(); // set to now.
		this.paused = false;
	}
	
	/**
	 * Note here we attempt to the TOCQueue which may take some time to be shown as available
	 * @param isConsumer
	 * @param maxAttempts
	 * @throws Exception
	 */
	public void connectToQueue(boolean isConsumer, int maxAttempts) throws Exception{
		
		for (int i=0; i<maxAttempts; i++) {
			
			logger.debug("connectToQueue() attempt: " + (i+1));
			
			ListQueuesResult queuesResult = sqsClient.listQueues();
			if (queuesResult != null) {
				for (String queueUrl : queuesResult.getQueueUrls()) {
					if (queueUrl.indexOf(sqsQueueName) != -1) {
						tocQueueUrl = queueUrl;
						break;
					}
				}
			}
			
			// if consumer, retry, otherwise is master, so just exit quick to create...
			if (tocQueueUrl == null && isConsumer) {
				Thread.currentThread().sleep(1000);
				continue;
			} else {
				break; // exit;
			}
		}
		
		if (tocQueueUrl == null && !isConsumer) {
			CreateQueueResult createQueueResult = sqsClient.createQueue(sqsQueueName);
			this.tocQueueUrl = createQueueResult.getQueueUrl();
			
		} else if (tocQueueUrl == null) {
			throw new Exception("TOCQueue() isConsumer:"+ isConsumer+ " cannot start, sqsQueueName has yet to be created by master?: " + sqsQueueName);
		}
	}
	
	public void send(FileInfo fileInfo, MODE mode) throws Exception {
		TOCPayload payload = new TOCPayload();
		payload.fileInfo = fileInfo;
		payload.mode = mode;
		
		// send!
		this.sqsClient.sendMessage(this.tocQueueUrl, gson.toJson(payload));
	}
	
	public void run() {
		
		while(this.running) {
			
			if (!this.paused) {
				try {
					ReceiveMessageRequest req = new ReceiveMessageRequest();
					req.setQueueUrl(this.tocQueueUrl);
					req.setVisibilityTimeout(300); // 5 minutes it will be invisible to other consumers
					req.setMaxNumberOfMessages(1);
					
					ReceiveMessageResult msgResult = sqsClient.receiveMessage(req);
					List<Message> messages = msgResult.getMessages();
	
					for (Message msg : messages) {
						
						this.lastSQSMessageReceivedMS = System.currentTimeMillis();
						this.totalMessagesProcessed++;
						
						logger.debug("TOCQueue["+myId+"] Received SQS Message " +
								"body (json -> TOCPayload) body= " + msg.getBody());
						TOCPayload payload = null;
	
						try {
							payload = gson.fromJson(msg.getBody(), TOCPayload.class);
						
						} catch(Exception e) {
							logger.error("TOCQueue["+myId+"] ERROR: unexpected error converting SQS Message " +
									"body (json -> TOCPayload) body= " + msg.getBody()+ " error="+e.getMessage());
							
							// delete the message we just analyzed
							sqsClient.deleteMessage(tocQueueUrl, msg.getReceiptHandle());
							
							continue;
						}
						
						logger.debug("TOCQueue["+myId+"] TOCPayload received: filePath:" + payload.fileInfo.getFilePath());
	
						// handle
						this.tocPayloadHandler.handlePayload(payload);
						
						// delete the message, got here no exception
						sqsClient.deleteMessage(tocQueueUrl, msg.getReceiptHandle());
					
					}
	
				} catch(Exception e) {
					logger.error("TOCQueue["+myId+"] run() unexpected error in handling TOCPayload: " + e.getMessage(),e);
				}
			}
			
			try {
				Thread.currentThread().sleep(500);
			} catch(Exception ignore) {}
			
		}
	}

	public void stopConsuming() {
		this.running = false;
	}
	
	public void destroy() throws Exception {

		Thread.currentThread().sleep(30000);
		
		try {
			if (canDestroyQueue) {
				logger.debug("TOCQueue["+myId+"] destroy() " + this.tocQueueUrl);
				this.sqsClient.deleteQueue(this.tocQueueUrl);
			}
		} catch(Exception e) {
			logger.error("TOCQueue["+myId+"] destroy() error deleting TOCQueue: " + e.getMessage(),e);
		}
	}
	
	public AmazonSQSClient getSqsClient() {
		return sqsClient;
	}
	public void setSqsClient(AmazonSQSClient sqsClient) {
		this.sqsClient = sqsClient;
	}
	public String getTocQueueUrl() {
		return tocQueueUrl;
	}
	public void setTocQueueUrl(String tocQueueUrl) {
		this.tocQueueUrl = tocQueueUrl;
	}
	
	public void sendMessage(String messageBody) {
		this.sqsClient.sendMessage(this.tocQueueUrl, messageBody);
	}
	
	private static String determineHostName() throws Exception {

		InetAddress addr = InetAddress.getLocalHost();
		
		// Get IP Address
		byte[] ipAddr = addr.getAddress();
		// Get sourceHost
		String tmpHost = addr.getHostName();

		// we only care about the HOST portion, strip everything else
		// as some boxes report a fully qualified sourceHost such as
		// host.domainname.com

		int firstDot = tmpHost.indexOf('.');
		if (firstDot != -1) {
			tmpHost = tmpHost.substring(0,firstDot);
		}
		return tmpHost;

	}

	public int getTotalMessagesProcessed() {
		return totalMessagesProcessed;
	}

}

