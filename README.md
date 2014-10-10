s3-bucket-loader
================

This project originated out of a need to import a massive amount of files (hundreds of gigabytes) into an AWS S3 bucket, with the ultimate intent that this bucket be managed going forward via the S3 distributed filesystem; [yas3fs](https://github.com/danilop/yas3fs). Initial attempts at doing this a traditional way, (i.e. rsyncing or copying from source to destination) quickly became impractical due to the sheer amount of time that single-threaded, and even limited multi-threaded copiers would take. 

s3-bucket-loader leverages a simple master/worker paradigm to get economies of scale for copying many files from sourceA to sourceB. Even though this is coded with S3 being the ultimate destination it could be used for other targets as well. The speed at which you can import a given fileset into S3 (through yas3fs in this case) is only limited on how much money you want to spend in worker hardware. For example this has been used to import and validate in S3 over 35k files (11gb total) in roughly 16 minutes; using 40 ec2 t2.medium instances as workers. 

![Alt text](/diagram.png "Diagram")

## How it works

This is a multi-threaded Java program that can be launched in two modes `master` or `worker`. The `master` is responsible for determining a table of contents (TOC) (i.e. file paths) which are canidates for WRITE to the destination and subsequently VALIDATED. The `master` node streams these TOC events over an SQA queue which is consumed to by one or more `workers`. Each `worker` must also have access to the `source` from which the TOC was generated from. The `source` data could be the same physical set of files, a copy of them or whatever... it really does not matter, but they just need to be accessible from a local path on each `worker` (i.e. via a SAN/NAS/NFS share etc). The `worker` then copies each file via rsync to S3; It uses rsync to preserve uid/gid information which is important for the ultimate consumer; and ensured preservation if written to S3 via S3 filesystem abstractions like [yas3fs](https://github.com/danilop/yas3fs). It is also important to note that each `worker` leverages N threads to increase parrallelism and maximize the throughput to S3. The more `workers` you have the faster it goes.

## Flow overview

1. End user starts the Master which creates the SNS control-channel and SQS TOC queue

2. The Master (optionally) launches N worker nodes on EC2

3. As each worker node initializes its subscribes to the control-channel and publishes that it is INITIALIZED

4. Once the master sees all of its workers in INITIALIZED state, the master changes the state to WRITE

5. The master begins creating the TOC (consisting of filepath and size), and sends a SQS message for each file to the TOC queue.

6. Workers begin consuming TOC messages off the queue and execute rsyncs from the source -> destination. As workers are consuming they periodly send CURRENT SUMMARY updates to the master. If `failfast` is configured and any failures are detected the master can switch the cluster to ERROR_REPORT mode immediately (see below).

7. When workers are complete, they publish their WRITE SUMMARY and go into an IDLE state

8. Master receives all WRITE SUMMARYs from the workers
  * If no errors, the master transitions to the VALIDATE state, and sends the TOC to the queue again
  * If errors the master transitions to the ERROR_REPORT state, and requests error details from the workers

9. In VALIDATE state, all workers consume TOC file paths from the SQS queue and attempt to verify the file exists and its sizes matches the expected size. When complete they go into IDLE state and publish their VALIDATE SUMMARY

10. After receiving all VALIDATE SUMMARYs from the workers
  * If no errors, the master issues a shutdown command to all workers, then optionally terminates all instances
  * If errors the master transitions to the ERROR_REPORT state, and requests error details from the workers

11. In ERROR REPORT state, workers summarize and publish their errors from either state WRITE/VALIDATE, the master aggregates them and reports them to the master log file for analysis. All workers are then shutdown.

12. At any stage, issueing a control-C on the master triggers a shutdown of the entire cluster, including ec2 worker termination if configured in the properties file


## How to run

* Clone this repository

* You need a Java JDK installed preferable 1.6+

* You need [Maven](http://maven.apache.org/) installed

* Change dir to the root of the project and run 'mvn package' (this will build a runnable Jar under target/)

* Copy the [s3BucketLoader.sample.properties](https://github.com/bitsofinfo/s3-bucket-loader/blob/master/src/main/resources/s3BucketLoader.sample.properties) file under src/main/resources, make your own and customize it. 

* run the below to launch:
```
java -jar -DisMaster=true|false -Ds3BucketLoaderHome=/some/dir -DconfigFilePath=s3BucketLoader.properties s3-bucket-loader-0.0.1-SNAPSHOT.jar
```

* The sample properties should be fairly self explanitory. Its important to understand that it is up to YOU to properly configure your environment for both the master and worker(s). The `master` needs access to the gold-copy "source" files that you want to get into S3. The `workers` need access to both the "source" files and some sort of S3 target (via an S3 filesystem abstraction like yas3fs). Note that s3-bucket-loader can automatically configure your workers for you... you just need to configure a 'user-data' startup script for the EC2 instances that your `master` will launch. A example/sample one that I have used previously is provided under [ec2-init-s3BucketLoader.sample.py](src/main/resources/ec2-init-s3BucketLoader.sample.py). For example, when ec2 launches your workers, a startup script can pull all packages needed to prepare the environment from another S3 bucket, install things, configure and even pull down the latest s3-bucket-loader jar file, the worker properties file and finally launch the worker.

Enjoy. Or not. 




