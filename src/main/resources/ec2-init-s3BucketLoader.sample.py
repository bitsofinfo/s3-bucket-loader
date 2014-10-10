#!/usr/bin/python

import boto
import subprocess
import boto.s3.bucket
import boto.s3.connection
import signal
import sys
import time

##################################
# This script is A SAMPLE ONLY
# Its intended to be passed as the userData attribute
# when the master node launches ec2 worker node instances
#
# Its up to you to do whatever you want to get your
# worker nodes prepped and ready for work.
# 
# This can be read in by the MASTER node via
# the 'master.workers.ec2.userDataFile' property
# when it launches ec2 nodes.
#
# For example: Here is where you can prepare
# your launched ec2 worker node
# to install and configure
#
#  - fusepy
#  - yas3fs
#  - local dirs
#  - mount the shared NFS source dir
#  - set permissions
#  - get the latest worker.properties
#  - get the latest s3BucketLoader JAR
#  - whatever else you want
#  - start the worker!
#
# Note this sample references custom 
# fusepy/yas3fs RPMs, however you would
# would have to create your own or change
# customize this routine to install them
# via a different way.
#
##################################


access_key = 'YOUR_KEY'
secret_key = 'YOUR_KEY'

s3BktLdrBucketName='nameOf.S3Bucket.2DownloadInstall.ResourcesFrom'

s3MountRoot='/mydir/s3mount'
s3BktLdrInstallRoot='/mydir/s3BucketLoader'

s3BktLdrJar='s3-bucket-loader-0.0.1-SNAPSHOT.jar'
s3BktLdrProps='s3BucketLoader.worker.properties'

fusepyRPM='python-fusepy-version.rpm'
yas3fsRPM='python-yas3fs-version.rpm'

nfsServerPath='your.nfs.server.com:/exported/path'
nfsLocalMountRoot='/mydir/nfs'

# having local users/groups created
# is important so that when the worker
# copies files via rsync from the 'source'
# share, that the uid/gids are preserved
# by yas3fs when written to the s3 bucket
# so that ultimately whatever application
# you intend to mount the new bucket will
# be able to run as its intended uid/gid
# and access the files!
username = 'whatever'
uid = '400'
groupname = 'whatever'
gid = '400'

##################################
END
##################################

def signal_handler(signal, frame):
    print 'You pressed Ctrl+C! sleeping to let s3BucketLoader cleanup...'
    time.sleep(30000)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 's3.amazonaws.com',
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

bucket = conn.get_bucket(s3BktLdrBucketName)

# user/group setup for perms
subprocess.call(['groupadd', '-g', gid, groupname])
subprocess.call(['useradd', '-M', '-u', uid, '-g', gid, groupname])

# setup dirs
subprocess.call(['mkdir', '-p', nfsLocalMountRoot])
subprocess.call(['mkdir', '-p', s3MountRoot])
subprocess.call(['mkdir', '-p', s3BktLdrInstallRoot])

# pull down software
key = bucket.get_key(s3BktLdrJar)
key.get_contents_to_filename(s3BktLdrInstallRoot+'/'+s3BktLdrJar)

key = bucket.get_key(s3BktLdrProps)
key.get_contents_to_filename(s3BktLdrInstallRoot+'/'+s3BktLdrProps)

key = bucket.get_key(fusepyRPM)
key.get_contents_to_filename(s3BktLdrInstallRoot+'/'+fusepyRPM)

key = bucket.get_key(yas3fsRPM)
key.get_contents_to_filename(s3BktLdrInstallRoot+'/'+yas3fsRPM)

# perms
subprocess.call(['chown', '-R', ('root:'+groupname), s3BktLdrInstallRoot])
subprocess.call(['chmod', '-R', '770', s3BktLdrInstallRoot])

# update fuse yas3fs, prep, install and configure so that its runnable
# by the woker, for example, via the 'worker.initialize.cmd' property in the
# s3BucketLoader.properties file the worker would use
subprocess.call(['rpm', '-ivh', 'https://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'])
subprocess.call(['yum', '-y', '--enablerepo=epel', 'install', 'python-argparse'])
subprocess.call(['yum', '-y','--enablerepo=epel', 'localinstall', (s3BktLdrInstallRoot+'/'+fusepyRPM)])
subprocess.call(['yum', '-y','--enablerepo=epel', 'localinstall', (s3BktLdrInstallRoot+'/'+yas3fsRPM)])
subprocess.call(['mkdir', '-p', '/var/lib/yas3fs/s3BucketLoader'])
subprocess.call(['chown', '-R', 'root:yas3fs', '/var/lib/yas3fs/s3BucketLoader'])
subprocess.call(['chmod', '-R', '770', '/var/lib/yas3fs/s3BucketLoader'])
subprocess.call(['usermod', '-a', '-G', groupname, 'yas3fs'])
subprocess.call(['chkconfig', 'yas3fs', 'off', '0,1,2,3,4,5,6'])

subprocess.call(['yum', '-y', 'install', 'nfs-utils', 'nfs-utils-lib'])
subprocess.call(['mount', nfsServerPath, nfsLocalMountRoot])

text_file = open("/etc/fuse.conf", "w")
text_file.write("user_allow_other")
text_file.close()

# launch S3BucketLoader which will start up in listening mode waiting for master .....
subprocess.call(['java', '-DisMaster=false', ('-DconfigFilePath='+s3BktLdrInstallRoot+'/'+s3BktLdrProps), ('-Ds3BucketLoaderHome='+s3BktLdrInstallRoot), '-jar', (s3BktLdrInstallRoot+'/'+s3BktLdrJar)])

