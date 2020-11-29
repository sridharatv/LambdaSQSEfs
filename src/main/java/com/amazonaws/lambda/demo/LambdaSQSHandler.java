package com.amazonaws.lambda.demo;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.time.LocalDateTime;    


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

public class LambdaSQSHandler implements RequestHandler<SQSEvent, Void	> {

	@Override
	public Void handleRequest(SQSEvent event, Context context)
	{
		context.getLogger().log("===== Received Message count: " + event.getRecords().size());
		String FileName = "/mnt/received/file_to_write.txt";
		File fptr = new File(FileName);
		for(SQSMessage msg : event.getRecords()){
			String sqsMsg = msg.getBody() + ", Lambda time, " + LocalDateTime.now() + "\n";
			context.getLogger().log(sqsMsg);
			long flen = fptr.length();
			RandomAccessFile randomAccessFile = null;
			try {
				randomAccessFile = new RandomAccessFile(fptr, "rw");
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				// get channel
				FileChannel channel = randomAccessFile.getChannel();
				// get lock
				FileLock tryLock = channel.lock();
				//String data = "This data is written from Lambda Function";
				// Seek to end of file
				randomAccessFile.seek(flen);
				// write to file
				randomAccessFile.writeBytes(sqsMsg);
				// release lock
				tryLock.release();
				// close file
				randomAccessFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
