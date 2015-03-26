package edu.umd.ujjwalgoel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank. 
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private ArrayListOfIntsWritable sources;
	private ArrayListOfFloatsWritable pageranks;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNode() {}

	public ArrayListOfFloatsWritable getPageRanks() {
		return pageranks;
	}

	public void setPageRanks(ArrayListOfFloatsWritable p) {
		this.pageranks = p;
	}

	public ArrayListOfIntsWritable getSources() {
                return sources;
        }

        public void setSources(ArrayListOfIntsWritable s) {
                this.sources = s;
        }

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public void parseObject(String s){
		String n = s.split("\\t")[1];
		String[] nodeStr = n.replaceAll("[{]", "").replaceAll("[}]", "").split("  ");
		boolean valid = true;
		nodeid = Integer.parseInt(nodeStr[0]);
		String[] srcs = nodeStr[1].replaceAll("\\[", "").replaceAll("\\]", "").split(",");
		int[] sourceList = new int[srcs.length];
		for(int i = 0; i < srcs.length; i++){
			String value = srcs[i].trim();
			if(value.isEmpty()){
				valid = false;
				break;
			}
			sourceList[i] = Integer.parseInt(value);
		}
		if(valid)
			sources = new ArrayListOfIntsWritable(sourceList);
		else
			sources = new ArrayListOfIntsWritable();
		
		valid = true;
		String[] prs = nodeStr[2].replaceAll("\\[", "").replaceAll("\\]", "").split(",");
               	float[] prList = new float[prs.length];
                for(int i = 0; i < prs.length; i++){
			String value = prs[i].trim();
                        if(value.isEmpty()){
				valid = false;
                                break;
                        }
			else if(value.equals("-Infinity")){
			    prList[i] = Float.NEGATIVE_INFINITY;
			}
			else {
                            prList[i] = Float.parseFloat(value);
			}
                }
		if(valid)
                	pageranks = new ArrayListOfFloatsWritable(prList);
		else
			pageranks = new ArrayListOfFloatsWritable();

		valid = true;
		String[] adj = nodeStr[3].replaceAll("\\[", "").replaceAll("\\]", "").split(",");
                int[] adjList = new int[adj.length];
                for(int i = 0; i < adj.length; i++){
			String value = adj[i].trim();
                        if(value.isEmpty()){
				valid = false;
                                break;
                        }
                        adjList[i] = Integer.parseInt(value);
                }
		if(valid)
                	adjacenyList = new ArrayListOfIntsWritable(adjList);
		else
			adjacenyList = new ArrayListOfIntsWritable();
		type = Type.valueOf(nodeStr[4].trim());
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();

		if (type.equals(Type.Mass)) {
			pageranks = new ArrayListOfFloatsWritable();
			pageranks.readFields(in);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageranks = new ArrayListOfFloatsWritable();
			pageranks.readFields(in);
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
		boolean exceptionFound = false;
		ArrayListOfIntsWritable newSources = new ArrayListOfIntsWritable();
                try {
		    newSources.readFields(in);
		}
		catch(IOException ex) {
		    exceptionFound = true;
		}
		if(!exceptionFound){
		    sources = newSources;
		}
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);

		if (type.equals(Type.Mass)) {
			pageranks.write(out);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageranks.write(out);
		}
		
		adjacenyList.write(out);
		
		if(sources != null){
                     sources.write(out);
                }

	}

	@Override
	public String toString() {
		return String.format("{%d  %s  %s  %s  %s}",
				nodeid, (sources == null ? "[]" : sources.toString()) , (pageranks == null ? "[]" : pageranks.toString()), (adjacenyList == null ? "[]" : adjacenyList.toString(10)), type);
	}


  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
