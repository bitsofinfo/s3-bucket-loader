package org.bitsofinfo.s3.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.amazonaws.util.Base64;

public class CompressUtil {
	
	public static byte[] asciiChars2bytes(char[] asciiChars) {
		byte[] bytes = new byte[asciiChars.length];
		for (int i=0; i< asciiChars.length; i++) {
			bytes[i] = (byte)asciiChars[i];
		}
		return bytes;
	}

	public static char[] decompressAndB64DecodeASCIIChars(char[] b64EncodedCompressedString) throws Exception {

		byte[] input = Base64.decode(asciiChars2bytes(b64EncodedCompressedString));
		
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
	    return new String(stream.toByteArray(),"US-ASCII").toCharArray();
	}
	
	public static char[] compressAndB64EncodeASCIIChars(char[] str) throws Exception{
		
		byte[] input = asciiChars2bytes(str);
		
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
	    
	    return new String(Base64.encode(compressedData),"US-ASCII").toCharArray();
	}
}
