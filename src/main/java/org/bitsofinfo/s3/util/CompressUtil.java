package org.bitsofinfo.s3.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.amazonaws.util.Base64;

public class CompressUtil {

	public static char[] decompressAndB64DecodeUTF8Bytes(byte[] b64EncodedCompressedBytes) throws Exception {

		byte[] input = Base64.decode(b64EncodedCompressedBytes);
		
		// Compressor with highest level of compression
	    Inflater inflater = new Inflater();
	    
	    // Give the compressor the data to compress
	    inflater.setInput(input);
	    
	    ByteArrayOutputStream stream = new ByteArrayOutputStream();
	    byte[] buf = new byte[32];
	    while (!inflater.finished()) {
	        int count = inflater.inflate(buf);
	        stream.write(buf, 0, count);
	    }
	    return new String(stream.toByteArray(),"UTF-8").toCharArray();
	}
	
	public static String compressAndB64EncodeUTF8Bytes(byte[] bytes) throws Exception{
		
		byte[] input = bytes;
		
		// Compressor with highest level of compression
	    Deflater compressor = new Deflater();
	    compressor.setLevel(Deflater.BEST_COMPRESSION);
	    
	    // Give the compressor the data to compress
	    compressor.setInput(input);
	    compressor.finish();
	    
	    // Create an expandable byte array to hold the compressed data.
	    // It is not necessary that the compressed data will be smaller than
	    // the uncompressed data.
	    ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
	    
	    // Compress the data
	    byte[] buf = new byte[32];
	    while (!compressor.finished()) {
	        int count = compressor.deflate(buf);
	        bos.write(buf, 0, count);
	    }
	    try {
	        bos.close();
	    } catch (IOException e) {
	    }
	    
	    // Get the compressed data
	    byte[] compressedData = bos.toByteArray();
	    
	    return new String(Base64.encode(compressedData),"UTF-8");
	}
}
