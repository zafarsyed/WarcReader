package WarcReader.WarcReader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
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
	public String articleText;
	
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
		int samedocs = 0;
		// cast to a data input stream
		
		System.out.println("Processing File "+fileName);
		ObjectNode objectNode1 = mapper.createObjectNode();
		String previousText = "";
		while ((this.thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
			// see if it's a response record
			if (this.thisWarcRecord.getHeaderRecordType().equals("response")) {
				
				docCount++;
				// it is - create a WarcHTML record
				//				WarcReader reader = new WarcReaderCompressed();
				this.htmlRecord=new WarcHTMLResponseRecord(this.thisWarcRecord);
				if(this.htmlRecord.getRawRecord().getContentUTF8().contains("Content-Type: text/plain"))
				{
					emptyFiles++;
					continue;
				}
				// get our TREC ID and target URI
				Document document = Jsoup.parse(this.htmlRecord.getRawRecord().getContentUTF8());
				String thisTRECID=this.htmlRecord.getTargetTrecID();
				String thisTargetURI=this.htmlRecord.getTargetURI();
//				System.out.println(thisTRECID);
//				System.out.println(thisTargetURI);
				String docTitle = document.title();
				Elements paras = document.select("p");
				this.articleText = "";
				for (Element element : paras) {
					element.select("a").remove();
					element.select("div").remove();
					element.select("links").remove();
					String eleText = element.text().replace("\u00a0", "");
					//eleText = eleText.replaceAll("[^a-zA-Z0-9.,!{}\":;\'?/\\|()&%@#]", " ").replaceAll("\\s+", " ");
					/*if(thisTRECID.equals("clueweb12-1100wb-15-21352"))
					{
						System.out.println(eleText);
					}*/
					if(this.articleText.isEmpty() && !(eleText.isEmpty()))
					{
						this.articleText = this.articleText + eleText;
						continue;
					}
					if(!(eleText.isEmpty()))
						this.articleText = this.articleText +" "+ element.text();
				}
				this.articleText = this.articleText.replace("|", "");
				this.articleText = this.articleText.replace("\u00a0", "");
				this.articleText = this.articleText.replaceAll("[^a-zA-Z0-9.,!{}\":;'’<>?/\\()$&%@#-=_]", " ").replaceAll("\\s+", " ");
				
				if(this.articleText.isEmpty() || this.articleText.length()<100)
				{
					//System.out.println("No Text");
					emptyFiles++;
					continue;
				}
				
				if(StringUtils.equals(previousText, this.articleText))
				{
					samedocs++;
					continue;
				}
				else
				{
					//System.out.println(articleText);
					//System.out.println(this.articleText);
					if(docTitle.isEmpty())
						docTitle = "No Document Title";
					objectNode1.put("Title", docTitle);
					objectNode1.put("Article", this.articleText);
					objectNode1.put("URL", thisTargetURI);
					fw.write("{\"index\": {\"_id\":\""+thisTRECID+"\"}}" + "\n");
					fw.write(objectNode1.toString() + "\n");
					previousText = this.articleText;
				}

			}
		}
		fw.close();
		inStream.close();
		System.out.println("Finished processing file "+fileName+" Identical doc count "+samedocs);
		return "Number of documents read for "+this.fileName+ " is "+(docCount-(emptyFiles+samedocs)) ;
	}

}
