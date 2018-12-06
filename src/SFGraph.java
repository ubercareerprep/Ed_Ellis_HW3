import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.*;

class Edge {
  int edgeId;
  int startNodeId;
  int endNodeId;
  float l2Distance;
}

class Node {
  int nodeId;
  float latitude;
  float longitude;
}

class Vertex implements Comparable{
  int name;
  float distance;
  ArrayList<Edge> adj;
  Vertex prev;
  boolean known;

  public Vertex(int n) {
    name = n;
    distance = Float.POSITIVE_INFINITY;
    adj = new ArrayList<>();
    prev = null;
    known = false;
  }

  public void add(Edge e) {
    adj.add(e);
  }

  public int compareTo(Object o) {
    Vertex rhs = (Vertex) o;
    if (this.distance > rhs.distance) {
      return 1;
    }
    if (this.distance < rhs.distance) {
      return -1;
    }
    return 0;
  }
}


public class SFGraph {
  public static void dijkstra(int start, int end, List<Edge> edges, List<Node> nodes) {
    int n = nodes.size();
    Vertex[] vertices = new Vertex[n];
    // store distance for vertices and end of every edge to easily query later
    HashMap<String, Float> distances = new HashMap<>();

    for (int i=0; i<n; i++) {
      vertices[i] = new Vertex(i);
    }
    for (Edge e : edges) {
      vertices[e.startNodeId].add(e);
      vertices[e.endNodeId].add(e);
      String ed = e.startNodeId+" "+e.endNodeId;
      String rev = e.endNodeId+" "+e.startNodeId;
      distances.put(ed,e.l2Distance);
      distances.put(rev,e.l2Distance);
    }

    PriorityQueue<Vertex> q = new PriorityQueue<>();
    vertices[start].distance = 0;
    q.add(vertices[start]);

    while (!q.isEmpty()) {
      Vertex min = q.peek();
      q.remove();
      min.known = true;
      //System.out.println(min.name+" I'm the current minimum vertex");
      for (Edge e : min.adj) {
        Vertex v;
        //if the vtx is the same as the start, the endNodeId is the adj vtx
        //you're looking at and vice versa
        if (e.startNodeId == min.name) {
          v = vertices[e.endNodeId];
        } else {
          v = vertices[e.startNodeId];
        }
        //System.out.println(v.name+" current vertex being looked at");
        if (!v.known) {
          float cvw = e.l2Distance;
          //System.out.println(v.distance+" current vertex distance before for "+v.name);
          if (min.distance + cvw < v.distance) {
            v.distance = min.distance + cvw;
            v.prev = min;
            //System.out.println(min.name+" is the previous vertex to "+v.name);
            q.add(v);
          }
          //System.out.println(v.distance+" current vertex distance after for "+v.name);
        }
      }
    }
    Vertex b = vertices[end];
    String path = "";
    int ct = 0;
    String curr = "";
    float distance = 0;
    while (b != null) {
      curr = Integer.toString(b.name);
      if (ct == 0)
        path += curr;
      else
        path = curr + ", " + path;
      //if prev isn't null find the distance btw vertices
      if (b.prev != null) {
        String key = b.name+" "+b.prev.name;
        distance += distances.get(key);
      }
      b = b.prev;
      ct++;
    }
    if (!curr.equals(Integer.toString(start)))
      System.out.println("There is no path between "+start+" and "+end);
    else
      System.out.println(path);
      System.out.println("Distance: "+distance);
  }

  public static void fillDBEdges() {
    Database db = new Database();

    //load all edges and nodes from cs.utah.spatial into DB
    //reading from downloaded text files of these
    try {
      File edFile = new File("cal.cedge.txt");
      BufferedReader br = new BufferedReader(new FileReader(edFile));
      String line;
      while((line = br.readLine()) != null){
        String[] data = line.split(" ");
        int EdgeId = Integer.parseInt(data[0]);
        int StartNodeId = Integer.parseInt(data[1]);
        int EndNodeId = Integer.parseInt(data[2]);
        float L2Distance = Float.parseFloat(data[3]);
        db.insertEdge(EdgeId, StartNodeId, EndNodeId, L2Distance);
      }
      db.selectAllEdges();
    }
    catch(IOException e){
      System.out.println("Try again to read in your file");
    }
  }

  public static void fillDBNodes() {
    Database db = new Database();

    //load all edges and nodes from cs.utah.spatial into DB
    //reading from downloaded text files of these
    try {
      File edFile = new File("cal.cnode.txt");
      BufferedReader br = new BufferedReader(new FileReader(edFile));
      String line;
      while((line = br.readLine()) != null){
        String[] data = line.split(" ");
        int NodeID = Integer.parseInt(data[0]);
        float Latitude = Float.parseFloat(data[1]);
        float Longitude = Float.parseFloat(data[2]);
        /*Inserting for each record takes a long time
         *since this database has 20K+ edges
         *As a result, database isn't completely full
         *Display the nodes and edges by creating a
         *Database object and calling db.selectAllNodes()
         *and selectAllEdges()
         *This database currently has nodes 0-2287 and edges 0-273
         */
        db.insertNode(NodeID, Latitude, Longitude);
      }
    }
    catch(IOException e){
      System.out.println("Try again to read in your file");
    }
  }

  public static ArrayList<Edge> loadEdges(){
    String sql = "SELECT EdgeId, StartNodeId, EndNodeId, L2Distance FROM edges";
    String url = "jdbc:sqlite:C:/Users/Edward/Desktop/HGP_Repositories" +
            "/Ed_Ellis_HW3/src/RoadNetwork.db";
    ArrayList<Edge> edges = new ArrayList<Edge>();
    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt  = conn.createStatement();
         ResultSet rs    = stmt.executeQuery(sql)){

      // loop through the result set
      while (rs.next()) {
        Edge e = new Edge();
        e.edgeId = rs.getInt("EdgeId");
        e.startNodeId = rs.getInt("StartNodeId");
        e.endNodeId = rs.getInt("EndNodeId");
        e.l2Distance = rs.getFloat("L2Distance");
        edges.add(e);
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return edges;
  }

  public static ArrayList<Node> loadNodes(){
    String sql = "SELECT NodeID, Latitude, Longitude FROM nodes";
    String url = "jdbc:sqlite:C:/Users/Edward/Desktop/HGP_Repositories" +
            "/Ed_Ellis_HW3/src/RoadNetwork.db";
    ArrayList<Node> nodes = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt  = conn.createStatement();
         ResultSet rs    = stmt.executeQuery(sql)){

      // loop through the result set
      while (rs.next()) {
        Node n = new Node();
        n.nodeId = rs.getInt("NodeID");
        n.latitude = rs.getFloat("Latitude");
        n.longitude = rs.getFloat("Longitude");
        nodes.add(n);
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return nodes;
  }

  public static void loadDB() {
    ArrayList<Edge> eds = loadEdges();
    ArrayList<Node> nds = loadNodes();
    dijkstra(10,81,eds,nds);
  }

  public static void main(String[] args) {
    loadDB();
  }
}
