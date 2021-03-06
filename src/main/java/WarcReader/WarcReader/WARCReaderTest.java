package WarcReader.WarcReader;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;

/**
 * A raw example of how to process a WARC file using the org.archive.io package.
 * Common Crawl S3 bucket without credentials using JetS3t.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCReaderTest {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Set up a local compressed WARC file for reading 
		String fn = "F:\\Zafar\\Thesis\\collection\\ClueWeb09_English_Sample_File.warc.gz";
		FileInputStream is = new FileInputStream(fn);
		// The file name identifies the ArchiveReader and indicates if it should be decompressed
		ArchiveReader ar = WARCReaderFactory.get(fn, is, true);
		
		for(ArchiveRecord r : ar) {
			// The header file contains information such as the type of record, size, creation time, and URL
			System.out.println(r.getHeader());
			System.out.println(r.getHeader().getUrl());
			System.out.println(r.getDigestStr());
			System.out.println();
			
			// If we want to read the contents of the record, we can use the ArchiveRecord as an InputStream
			// Create a byte array that is as long as the record's stated length
			
//			byte[] rawData = new byte [] {};
//			r.read(rawData);
			byte[] rawData = IOUtils.toByteArray(r);
			// Why don't we convert it to a string and print the start of it? Let's hope it's text!
			String content = new String(rawData);
			System.out.println(content.substring(0, Math.min(5000, content.length())));
			System.out.println((content.length() > 500 ? "..." : ""));
			
			// Pretty printing to make the output more readable 
			System.out.println("=-=-=-=-=-=-=-=-=");
			//if (i++ > 4) break; 
		}
	}
}
