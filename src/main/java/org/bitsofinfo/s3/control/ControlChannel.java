package org.bitsofinfo.s3.control;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ControlChannel implements Runnable {
	
	private static final Logger logger = Logger.getLogger(ControlChannel.class);
	
	private AmazonSNSClient snsClient = null;
	private AmazonSQSClient sqsClient = null;
	
	private String snsTopicARN = null;
	private String snsSubscriptionARN = null;
	private String sqsQueueUrl = null;
	private String sqsQueueARN = null;
	
	private Gson gson = new Gson();
	
	private CCPayloadHandler ccPayloadHandler = null;
	private String mySourceIdentifier = null;
	private String mySourceIp = null;
	private Thread consumerThread = null;
	
	private boolean canDestroyTopic = false;
	
	private boolean running = true;
	private String snsControlTopicName = null;
	
	private String uuid = UUID.randomUUID().toString().replace("-", "").substring(0,4);;
	
	public ControlChannel(boolean callerIsMaster,
						  String awsAccessKey, String awsSecretKey, String snsControlTopicName, 
						  String userAccountPrincipalId, 
						  String userARN, 
						  CCPayloadHandler ccPayloadHandler) throws Exception {
		super();
		
		try {
			this.mySourceIp = InetAddress.getLocalHost().getHostAddress();
		} catch(Exception e) {
			logger.error("Error getting local inet address: " + e.getMessage());
		}
		
		mySourceIdentifier = determineHostName() + "-" +uuid;
		this.snsControlTopicName = snsControlTopicName;
		
		if (callerIsMaster) {
			canDestroyTopic = true;
			this.snsControlTopicName += "-" + mySourceIdentifier;
		}
		
		this.ccPayloadHandler = ccPayloadHandler;
		
		sqsClient = new AmazonSQSClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
		snsClient = new AmazonSNSClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
		
		
		this.connectToTopic(callerIsMaster, 1000, userAccountPrincipalId, userARN);
		
	

	}

	
	public void connectToTopic(boolean callerIsMaster, int maxAttempts, String userAccountPrincipalId, String userARN) throws Exception {
		
		
		// try up to max attempts to connect to pre-existing topic
		for (int i=0; i<maxAttempts; i++) {
			
			logger.debug("connectToTopic() attempt: " + (i+1));
			
			ListTopicsResult listResult = snsClient.listTopics();
			List<Topic> topics = listResult.getTopics();
			
			while(topics != null) {
				
				for (Topic topic : topics) {
	
					// note we do index of match....
					if (topic.getTopicArn().indexOf(snsControlTopicName) != -1) {
						snsTopicARN = topic.getTopicArn();
						logger.info("Found existing SNS topic by name: "+snsControlTopicName + " @ " + snsTopicARN);
						break;
					}
				}
	
				String nextToken = listResult.getNextToken();
				
				if (nextToken != null && snsTopicARN == null) {
					listResult = snsClient.listTopics(nextToken);
					topics = listResult.getTopics();
					
				} else {
					break;
				}
			}
			
			// if consumer, retry, otherwise is master, so just exit quick to create...
			if (snsTopicARN == null && !callerIsMaster) {
				Thread.currentThread().sleep(1000);
				continue;
			} else {
				break; // exit;
			}
		}
		
		
		
		// if master only he can create...
		if (snsTopicARN == null && callerIsMaster) {
			this.snsControlTopicName = this.snsControlTopicName.substring(0,(snsControlTopicName.length() > 80 ? 80 : this.snsControlTopicName.length()));
			
			logger.info("Attempting to create new SNS control channel topic by name: "+this.snsControlTopicName);
			
			CreateTopicResult createTopicResult = snsClient.createTopic(this.snsControlTopicName);
			snsTopicARN = createTopicResult.getTopicArn();
			snsClient.addPermission(snsTopicARN, "Permit_SNSAdd", 
									Arrays.asList(new String[]{userARN}), 
									Arrays.asList(new String[]{"Publish","Subscribe","Receive"}));
			logger.info("Created new SNS control channel topic by name: "+this.snsControlTopicName + " @ " + snsTopicARN);
			
		} else if (snsTopicARN == null) {
			throw new Exception("Worker() cannot start, snsControlTopicName has yet to be created by master?: " + this.snsControlTopicName);
		}
		
		// http://www.jorgjanke.com/2013/01/aws-sns-topic-subscriptions-with-sqs.html
		
		// create SQS queue to get SNS notifications (max 80 len)
		String prefix =  ("s3bktLoaderCC_" + mySourceIdentifier);
		String sqsQueueName = prefix.substring(0,(prefix.length() > 80 ? 80 : prefix.length()));
		
		CreateQueueResult createQueueResult = sqsClient.createQueue(sqsQueueName);
		this.sqsQueueUrl = createQueueResult.getQueueUrl();
		this.sqsQueueARN = sqsClient.getQueueAttributes(sqsQueueUrl, Arrays.asList(new String[]{"QueueArn"})).getAttributes().get("QueueArn");

		Statement statement = new Statement(Effect.Allow)
								.withActions(SQSActions.SendMessage)
								 .withPrincipals(new Principal("*"))
								 .withConditions(ConditionFactory.newSourceArnCondition(snsTopicARN))
								 .withResources(new Resource(sqsQueueARN));
		Policy policy = new Policy("SubscriptionPermission").withStatements(statement);

		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("Policy", policy.toJson());
		SetQueueAttributesRequest request = new SetQueueAttributesRequest(sqsQueueUrl, attributes);
		sqsClient.setQueueAttributes(request);

		logger.info("Created SQS queue: " + sqsQueueARN + " @ " + sqsQueueUrl);
		
		// subscribe our SQS queue to the SNS:s3MountTest topic
		SubscribeResult subscribeResult = snsClient.subscribe(snsTopicARN,"sqs",sqsQueueARN);
		snsSubscriptionARN = subscribeResult.getSubscriptionArn();
		logger.info("Subscribed for messages from SNS control channel:" + snsTopicARN + " ----> SQS: "+sqsQueueARN);
		logger.info("Subscription ARN: " + snsSubscriptionARN);
		
		this.consumerThread = new Thread(this,"ControlChannel msg consumer thread");
		this.consumerThread.start();

		logger.info("\n-------------------------------------------\n" +
					"CONTROL CHANNEL: ALL SNS/SQS resources hooked up OK\n" +
					"-------------------------------------------\n");
	}
		

	public void send(boolean fromMaster, CCPayloadType type, Object value) throws Exception {
		this.send(fromMaster,type,null,value);
	}
	
	public void send(boolean fromMaster, CCPayloadType type, String onlyForHostOrIp, Object value) throws Exception {
		CCPayload payload = new CCPayload();
		payload.fromMaster = fromMaster;
		payload.type = type;
		payload.value = value;
		payload.onlyForHostIdOrIP = onlyForHostOrIp;
		payload.sourceHostId = this.mySourceIdentifier.trim();
		payload.sourceHostIP = this.mySourceIp;
		
		logger.debug("Sending: " + type + "="+value);
		
		// send!
		this.snsClient.publish(this.snsTopicARN, gson.toJson(payload));
	}

	public void run() {
		
		while(running) {
			
			try {
				
				ReceiveMessageRequest req = new ReceiveMessageRequest();
				req.setMaxNumberOfMessages(10);
				req.setVisibilityTimeout(300);
				req.setQueueUrl(sqsQueueUrl);
				
				ReceiveMessageResult msgResult = sqsClient.receiveMessage(req);
				List<Message> messages = msgResult.getMessages();
				
				for (Message msg : messages) {
					
					CCPayload payload = null;

					try {
						Map<String,String> body = gson.fromJson(msg.getBody(), new TypeToken<Map<String, String>>(){}.getType()); 
						payload = gson.fromJson(body.get("Message"), CCPayload.class);
					
					} catch(Exception e) {
						logger.error("ERROR: unexpected error converting SQS Message " +
								"body (json -> CCPayload) body= " + msg.getBody() + " error="+e.getMessage());
						
						// delete the message we just analyzed
						sqsClient.deleteMessage(sqsQueueUrl, msg.getReceiptHandle());
						
						continue;
					}
				
					// if NOT equal to THIS MACHINE... defer tp payload handler
					if (!payload.sourceHostId.equalsIgnoreCase(mySourceIdentifier))  {
						this.ccPayloadHandler.handlePayload(payload);
					}
					
					// delete the message we just analyzed
					sqsClient.deleteMessage(sqsQueueUrl, msg.getReceiptHandle());
				}

			} catch(Exception e) {
				logger.error("run() " + e.getMessage(),e);
			}
			
			try {
				Thread.currentThread().sleep(500);
			} catch(Exception ignore) {}
			
		}
	}


	
	public void destroy() throws Exception {
		
		this.running = false;
		
		Thread.currentThread().sleep(30000);

		if (snsClient != null) {
			try {
				logger.debug("destroy() unsubscribe " + this.snsSubscriptionARN);
				snsClient.unsubscribe(snsSubscriptionARN);
			} catch(Exception e) {
				logger.debug("destroy() error: " + e.getMessage());
			}
		}
		
		if (sqsClient != null) {
			try {
				logger.debug("destroy() " + this.sqsQueueUrl);
				sqsClient.deleteQueue(sqsQueueUrl);
			} catch(Exception e) {
				logger.debug("destroy() error: " + e.getMessage());
			}
		}
		
		if (canDestroyTopic && snsClient != null) {
			try {
				logger.debug("destroy() " + this.snsTopicARN);
				snsClient.deleteTopic(this.snsTopicARN);
			} catch(Exception e) {
				logger.debug("destroy() error: " + e.getMessage());
			}
		}

	}
	
	public AmazonSNSClient getSnsClient() {
		return snsClient;
	}

	public void setSnsClient(AmazonSNSClient snsClient) {
		this.snsClient = snsClient;
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

	public static Logger getLogger() {
		return logger;
	}

	public AmazonSQSClient getSqsClient() {
		return sqsClient;
	}

	public String getSnsTopicARN() {
		return snsTopicARN;
	}

	public String getSnsSubscriptionARN() {
		return snsSubscriptionARN;
	}

	public String getSqsQueueUrl() {
		return sqsQueueUrl;
	}

	public String getSqsQueueARN() {
		return sqsQueueARN;
	}

	public Gson getGson() {
		return gson;
	}

	public CCPayloadHandler getCcPayloadHandler() {
		return ccPayloadHandler;
	}

	public String getMySourceIdentifier() {
		return mySourceIdentifier;
	}

	public Thread getConsumerThread() {
		return consumerThread;
	}


	public String getMySourceIp() {
		return mySourceIp;
	}
	

}

