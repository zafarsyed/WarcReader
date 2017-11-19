package WarcReader.WarcReader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
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
		String inputDirectory = args[0];
		String outputdirectory = args[1];
		Set<String> results = new HashSet<String>();
		Set<WarcReaderCallable> callables = new HashSet<WarcReaderCallable>();
		final File folder = new File(inputDirectory);
		for (final File fileEntry : folder.listFiles()) {
			callables.add(new WarcReaderCallable(inputDirectory, fileEntry.getName(), outputdirectory));
		}
		
		try
		{
			ExecutorService executor = Executors.newFixedThreadPool(2);
			for(Future<String> result : executor.invokeAll(callables))
			{
				results.add(result.get());
			}
			executor.shutdown();
		}
		catch (Exception e) {

            e.printStackTrace();
        }
		for (String string : results) {
			System.out.println(string);
		}
	}
}