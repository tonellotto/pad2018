# Working with HDFS from the command line

	hadoop fs <CMD>

### Help ([online](http://hadoop.apache.org/docs/current1/file_system_shell.html))

+ `-help [cmd]`: hopefully this is self-describing

### Inspect files

+ `-ls <path>`: list all files in `<path>`
+ `-cat <src>`: print `<src>` on `stdout`
+ `-tail [-f] <file>`: output the last part of the `<file>`
+ `-du <path>`: show `<path>` space utilization

### Create/remove files

+ `-mkdir <path>`: create a directory
+ `-mv <src> <dst>`: move (rename) files
+ `-cp <src> <dst>`: copy files
+ `-rmr <path>`: remove files and directories (recursively)

### Copy/Put files from a remote machine into the HADOOP cluster

+ `-copyFromLocal <localsrc> <dst>`: copy a local file to the HDFS
+ `-copyToLocal <src> <localdst>`: copy a file on the HDFS to the local disk

#### Examples:

	hadoop dfs -ls /
	hadoop dfs -copyFromLocal myfile remotefile

# HDFS configuration

The following properties should be set in the `hdfs-site.xml` file unless otherwise indi- cated.

* `dfs.name.dir`

	A namenode saves its data in a configurable folder(s), depending on the value of the `dfs.name.dir` property in the `hdfs-site.xml` file. This property determines where on the local filesystem the namenode should store the HDFS metadata.
	The `dfs.name.dir` specifies a comma separated list of local directories (with no spaces) in which the namenode should store a copy of the HDFS filesystem metadata.
	Administrators are strongly encouraged to specify two internal disks and a low latency, highly reliable, NFS mount.
	A complete copy of the metadata is stored in each directory. The namenode metadata is not excessively large, far less than 1TB.
	The default value of `dfs.name.dir` is `hadoop.tmp.dir/dfs/name` and, when coupled with `hadoop.tmp.dir`'s default of `/tmp/hadoop-user.name`, lands the filesystem metadata squarely in a volatile directory. This can cause problems since because `/tmp` is often managed directly by the OS, and for example it can be cleared during boot.

* `dfs.data.dir`

	A datanode saves its data in a configurable folder(s), depending on the value of the `dfs.data.dir` property in the `hdfs-site.xml` file.
	While `dfs.name.dir` specifies the location of the namenode metadata, `dfs.data.dir` is used to indicate where datanodes should store HDFS block data. Also a comma separate list, rather than mirroring data to each directory specified, the datanode round robins blocks between disks in an attempt to allocate blocks evenly across all drives. The datanode assumes each directory specifies a separate physical device in a JBOD (Just Bunch Of Disks) group (i.e.,  each disk individually addressable by the OS, and formatted and mounted as a separate mount point). Loss of a physical disk is not critical since replicas will exist on other machines in the cluster.
	The default value of the `dfs.name.dir` is `/tmp/hadoop-${user.name}/dfs/data`.

# Namenode potential issue

Sometimes the NameNode could be become suddenly corrupted, when using `/tmp` as local storage.
In such cases, the quick and dirty solution consists in cleaning up manually the `/tmp` folder and re-format the NameNode:

	hadoop@localhost$ bin/stop-all.sh
	hadoop@localhost$ rm -r /tmp/hadoop*
	hadoop@localhost$ bin/hadoop namenode -format
	hadoop@localhost$ bin/start-all.sh

When the NameNode starts up, it perfomerms some admin checks while the HDFS filesystem in in *safe mode*. Once HDFS leaves safe mode, it is again online and fully operational.
