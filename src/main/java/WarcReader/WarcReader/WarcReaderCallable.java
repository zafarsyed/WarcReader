package WarcReader.WarcReader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cmu.lemurproject.WarcHTMLResponseRecord;
import edu.cmu.lemurproject.WarcRecord;

public class WarcReaderCallable implements Callable<String> {
	
	private String fileLocation;
	private String fileName;
	private String outputLocation;
	private FileWriter fw;
	private WarcRecord thisWarcRecord;
	private WarcHTMLResponseRecord htmlRecord;
	public GZIPInputStream gzInputStream;
	public DataInputStream inStream;
	public ObjectMapper mapper;
	
	public WarcReaderCallable(String fileLocation, String fileName, String outputLocation) throws FileNotFoundException, IOException
	{
		this.fileLocation = fileLocation;
		this.fileName = fileName;
		this.outputLocation = outputLocation;
		this.gzInputStream = new GZIPInputStream(new FileInputStream(this.fileLocation+this.fileName));
		this.fw = new FileWriter(new File(this.outputLocation+this.fileName+".json"));
		this.inStream = new DataInputStream(gzInputStream);
		this.mapper = new ObjectMapper();		
	}

	public String call() throws Exception {
		
		int docCount = 0;
		int emptyFiles = 0;
		// cast to a data input stream
		
		
		ObjectNode objectNode1 = mapper.createObjectNode();		
		while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
			// see if it's a response record
			if (thisWarcRecord.getHeaderRecordType().equals("response")) {
				docCount++;
				// it is - create a WarcHTML record
				//				WarcReader reader = new WarcReaderCompressed();
				this.htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
				// get our TREC ID and target URI
				String thisTRECID=htmlRecord.getTargetTrecID();
				String thisTargetURI=htmlRecord.getTargetURI();
				//System.out.println(Jsoup.parse(htmlRecord.getRawRecord().getContentUTF8()).html());
				Elements paras = Jsoup.parse(htmlRecord.getRawRecord().getContentUTF8()).select("p");
				String articleText = "";
				for (Element element : paras) {
					element.select("a").remove();
					element.select("div").remove();
					element.select("links").remove();
					articleText = articleText +" "+ element.text();
					articleText = articleText.replace("|", "");
					articleText = articleText.replace("\u00a0", "");
				}
				if(articleText.isEmpty())
				{
					//System.out.println("No Text");
					emptyFiles++;
					continue;
				}
				else
				{
					//System.out.println(articleText);
					objectNode1.put("Article", articleText);
					objectNode1.put("URL", thisTargetURI);
					fw.write("{\"index\": {\"_id\":\""+thisTRECID+"\"}}" + "\n");
					fw.write(objectNode1.toString() + "\n");
				}

			}
		}
		fw.close();
		inStream.close();
		
		return "Number of documents read for "+this.fileName+ " is "+(docCount-emptyFiles) ;
	}

}
