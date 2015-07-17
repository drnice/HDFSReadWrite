
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSManipulation {

	public static void main(String[] args) throws IOException {
		HdfsRead("hdfs://192.168.245.207:8020/user/root/word_input.txt");
		HdfsWrite("hdfs://192.168.245.207:8020/user/root/word_input.txt", "hdfs://192.168.245.207:8020/user/root/word_input2.txt");
	}

public static void HdfsWrite (String fromUri, String toUri) throws IOException {
	int bytesRead ;		
	byte [] buffer = new byte[256];

	Configuration conf = new Configuration(); 
	conf.set("mapreduce.app-submission.cross-platform", "true");
	conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
	conf.addResource("/etc/hadoop/conf/core-site.xml");
	conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
	conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");   // from core-site.xml: fs.defaultFS=hdfs://sandbox.hortonworks.com:50070
	conf.set("hadoop.job.ugi", "root");
	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

	   
	FileSystem fs = FileSystem.get(URI.create(toUri), conf);
	
	FSDataOutputStream out = null ;
	FSDataInputStream in = null ;             //If source file is in HDFS		
    // if it is from Local FS.
	//InputStream in2 = new BufferedInputStream(new FileInputStream(fromUri));		     
    try
	{
		out = fs.create(new Path(toUri)) ;
		in = fs.open(new Path(fromUri));
	// we can copy bytes from input stream to output stream as shown below
	// IOUtils.copyBytes(in, out, 4096, false)
	// Read from input stream and write to output stream until EOF.
		while ((bytesRead = in.read(buffer)) > 0 )
		{
			out.write(buffer, 0, bytesRead) ;
		}
	}
	finally
	{
		in.close() ;
		out.close();
	}
}

public static void HdfsRead (String uri) throws IOException {
		
		Configuration conf = new Configuration(); 
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
		conf.addResource("/etc/hadoop/conf/core-site.xml");
		conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
		conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");   // from core-site.xml: fs.defaultFS=hdfs://sandbox.hortonworks.com:50070
		conf.set("hadoop.job.ugi", "root");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		//UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
		//ugi.doAs(new PrivilegedExceptionAction());
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null ;
		
		try
		{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally
		{
			IOUtils.closeStream(in);
		}	
	}


}
