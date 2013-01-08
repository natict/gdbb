import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class GraphLoader {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.err.println("usage: GraphLoader <nodes.csv> <edges.csv> <target_dir>");
			System.exit(1);
		}
		
		long startTime = System.currentTimeMillis();
		batchInsert(args[0], args[1], args[2]);
		System.err.printf("graph generation elapsed: %.3f\n", (double)(System.currentTimeMillis()-startTime)/1000);
	}

	private static void batchInsert(String nodesFile, String edgesFile,
			String targetDir) {
		
		BufferedReader inNodes = null;
		BufferedReader inEdges = null;
		Map<Long, Long> idToNeo4jId = new HashMap<Long, Long>();
		
		File targetDirFile = new File(targetDir);
		if (! targetDirFile.exists()) {
			targetDirFile.mkdirs();
		} else {
			System.err.println("error: target directory already exists");
			System.exit(1);
		}
		BatchInserter inserter = BatchInserters.inserter(targetDir);
				
		try {
			// read nodes csv line-by-line and generate HashMap
			inNodes = new BufferedReader(new FileReader(nodesFile));
			String line, name;
			Long id, nid;
			Map<String, Object> properties = new HashMap<String, Object>();
			while ((line=inNodes.readLine()) != null) {
				if (line.split(",").length == 2) {
					id = Long.parseLong(line.split(",")[0]);
					name = line.split(",")[1];
					properties.put( "name", name );
					properties.put( "nid", id );
					nid = inserter.createNode( properties );
					idToNeo4jId.put(id, nid);
				}
			}
			
			inEdges = new BufferedReader(new FileReader(edgesFile));
			RelationshipType coauth = DynamicRelationshipType.withName( "COAUTH" );
			Long id1, id2, nid1, nid2;
			while ((line=inEdges.readLine()) != null) {
				if (line.split(",").length == 2) {
					id1 = Long.parseLong(line.split(",")[0]);
					id2 = Long.parseLong(line.split(",")[1]);
					nid1 = idToNeo4jId.get(id1);
					nid2 = idToNeo4jId.get(id2);
					inserter.createRelationship(nid1, nid2, coauth, null);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (inNodes != null) inNodes.close();
				if (inEdges != null) inEdges.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			inserter.shutdown();
		}
	}

}
