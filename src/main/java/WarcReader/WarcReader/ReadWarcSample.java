package WarcReader.WarcReader;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.jsoup.Jsoup;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderCompressed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WarcHTMLResponseRecord;

public class ReadWarcSample {
	
	private static FileWriter fw;
	private static WarcRecord thisWarcRecord;
	private static WarcHTMLResponseRecord htmlRecord;
	public static GZIPInputStream gzInputStream;
	public static DataInputStream inStream;
	public static ObjectMapper mapper;

	public static void main(String[] args) throws IOException {
//		String inputWarcFile=args[0];
		final File folder = new File("E://ClueWeb");
		for (final File fileEntry : folder.listFiles()) {
		System.out.println("Current Warc File "+fileEntry.getName());
		gzInputStream = new GZIPInputStream(new FileInputStream(fileEntry.getAbsolutePath()));
		fw = new FileWriter("E://ClueWebJson//"+fileEntry.getName()+".json");
		int emptyFiles = 0;
		// cast to a data input stream
		inStream = new DataInputStream(gzInputStream);
		mapper = new ObjectMapper();
		ObjectNode objectNode1 = mapper.createObjectNode();		
		while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
			// see if it's a response record
			if (thisWarcRecord.getHeaderRecordType().equals("response")) {
				// it is - create a WarcHTML record
				//				WarcReader reader = new WarcReaderCompressed();
				htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
				// get our TREC ID and target URI
				String thisTRECID=htmlRecord.getTargetTrecID();
				String thisTargetURI=htmlRecord.getTargetURI();
				String articleText = Jsoup.parse(htmlRecord.getRawRecord().getContentUTF8()).select("p").text().replaceAll("\\s+", " ");
				if(articleText.isEmpty())
				{
					//System.out.println("No Text");
					emptyFiles++;
					continue;
				}
				else
				{

					objectNode1.put("Article", articleText);
					objectNode1.put("URL", thisTargetURI);
					fw.write("{\"index\": {\"_id\":\""+thisTRECID+"\"}}" + "\n");
					fw.write(objectNode1.toString() + "\n");

				}

			}
		}
		System.out.println("Number of Empty Docs " + emptyFiles);
		fw.close();
		inStream.close();
		}
	}
}