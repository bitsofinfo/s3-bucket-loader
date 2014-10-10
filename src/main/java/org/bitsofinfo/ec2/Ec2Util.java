package org.bitsofinfo.ec2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.ShutdownBehavior;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.util.Base64;

public class Ec2Util {
	
	private static final Logger logger = Logger.getLogger(Ec2Util.class);
	
	/**
	 * Returns map of instanceId:privateDnsName
	 * 
	 * @param ec2Instances
	 * @return
	 */
	public Map<String,String> getPrivateDNSNames(List<Instance> ec2Instances) {
		TreeMap<String,String> names = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
		for (Instance i : ec2Instances) {
			names.put(i.getInstanceId(),i.getPrivateDnsName().toLowerCase());
		}
		return names;
	}
	
	/**
	 * Returns map of instanceId:privateIp
	 * 
	 * @param ec2Instances
	 * @return
	 */
	public Map<String,String> getPrivateIPs(List<Instance> ec2Instances) {
		TreeMap<String,String> names = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
		for (Instance i : ec2Instances) {
			names.put(i.getInstanceId(),i.getPrivateIpAddress());
		}
		return names;
	}
	
	public void terminateEc2Instance(AmazonEC2Client ec2Client, String instanceId) throws Exception {
		try {
			TerminateInstancesRequest termReq = new TerminateInstancesRequest();
			List<String> instanceIds = new ArrayList<String>();
			instanceIds.add(instanceId);
			termReq.setInstanceIds(instanceIds);
			logger.debug("Terminating EC2 instances...." + Arrays.toString(instanceIds.toArray(new String[]{})));
			ec2Client.terminateInstances(termReq);
			
		} catch(Exception e) {
			logger.error("Unexpected error terminating: " + instanceId + " "+ e.getMessage(),e);
		}
	}

	 
	public List<Instance> launchEc2Instances(AmazonEC2Client ec2Client, Properties props) throws Exception {
		
		Integer totalExpectedWorkers = Integer.valueOf(props.getProperty("master.workers.total"));
		
		// create our run request for the total workers we expect
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		runInstancesRequest.withImageId(props.getProperty("master.workers.ec2.ami.id"))
					        .withInstanceType(props.getProperty("master.workers.ec2.instanceType"))
					        .withMinCount(totalExpectedWorkers)
					        .withMaxCount(totalExpectedWorkers)
					        .withKeyName(props.getProperty("master.workers.ec2.keyName"))
					        .withSecurityGroupIds(props.getProperty("master.workers.ec2.securityGroupId"))
					        .withInstanceInitiatedShutdownBehavior(ShutdownBehavior.valueOf(props.getProperty("master.workers.ec2.shutdownBehavior")))
					        .withSubnetId(props.getProperty("master.workers.ec2.subnetId"))
					        .withUserData(Base64.encodeAsString(readFile(props.getProperty("master.workers.ec2.userDataFile")).getBytes()));
		
		// launch
		logger.debug("Launching " + totalExpectedWorkers + " EC2 instances, " +
							"it may take few minutes for workers to come up...: \n" +
							"\tamiId:" + runInstancesRequest.getImageId() +"\n"+
							"\tsecGrpId:" + runInstancesRequest.getSecurityGroupIds().get(0) +"\n"+
							"\tsubnetId:" + runInstancesRequest.getSubnetId() +"\n"+
							"\tinstanceType:" + runInstancesRequest.getInstanceType() +"\n"+
							"\tshutdownBehavior:" + runInstancesRequest.getInstanceInitiatedShutdownBehavior() +"\n"+
							"\tkeyName:" + runInstancesRequest.getKeyName() 
							);
	

		// as the instances come up, assuming the "userData" above launches the worker we will be good
		// they will auto register w/ us the master 
		RunInstancesResult result = ec2Client.runInstances(runInstancesRequest);
		Reservation reservation = result.getReservation();
		return reservation.getInstances();
	}
	
	public void dumpEc2InstanceStatus(AmazonEC2Client ec2Client, List<Instance> ec2Instances) {
		try {
			List<String> instanceIds = new ArrayList<String>();
			
			for (Instance ec2node : ec2Instances) {
				instanceIds.add(ec2node.getInstanceId());
			}
			
			DescribeInstanceStatusRequest statusReq = new DescribeInstanceStatusRequest();
			statusReq.setInstanceIds(instanceIds);
			DescribeInstanceStatusResult result = ec2Client.describeInstanceStatus(statusReq);
			
			List<InstanceStatus> statuses = result.getInstanceStatuses();
			
			StringBuffer sb = new StringBuffer("EC2 worker instance STATUS:\n");
			for (InstanceStatus status : statuses) {
				sb.append("\tid:"+status.getInstanceId() + 
						"\taz:" + status.getAvailabilityZone() + 
						"\tstate:" + status.getInstanceState().getName() + 
						"\tstatus:" + status.getInstanceStatus().getStatus() + 
						"\tsystem_status: " + status.getSystemStatus().getStatus() + "\n"); 
			}
			
			logger.info(sb.toString()+"\n");
		} catch(Exception e) {
			logger.error("Error getting instance state: " + e.getMessage(),e);
		}
		
	}
	
	public static String readFile(String path) throws IOException {
		File file = new File(path);
		FileInputStream fis = new FileInputStream(file);
		byte[] data = new byte[(int)file.length()];
		fis.read(data);
		fis.close();
		return new String(data, "UTF-8");
	}
	
}
