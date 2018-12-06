import java.sql.*;

public class Database {
  public static void createNodeTable() {
    String url = "jdbc:sqlite:C:/Users/Edward/Desktop/HGP_Repositories" +
            "/Ed_Ellis_HW3/src/RoadNetwork.db";

    // SQL statement for creating a new table
    String sql = "CREATE TABLE IF NOT EXISTS nodes (\n"
            + "	NodeID integer PRIMARY KEY,\n"
            + "	Latitude float,\n"
            + "	Longitude float\n"
            + ");";

    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      // create a new table
      stmt.execute(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  public static void createEdgeTable() {
    String url = "jdbc:sqlite:C:/Users/Edward/Desktop/HGP_Repositories" +
            "/Ed_Ellis_HW3/src/RoadNetwork.db";

    // SQL statement for creating a new table
    String sql = "CREATE TABLE IF NOT EXISTS edges (\n"
            + "	EdgeId integer PRIMARY KEY,\n"
            + "	StartNodeId integer,\n"
            + "	EndNodeId integer,\n"
            + " L2Distance float\n"
            + ");";

    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      // create a new table
      stmt.execute(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * Connect to the test.db database
   *
   * @return the Connection object
   */
  private Connection connect() {
    // SQLite connection string
    String url = "jdbc:sqlite:C:/Users/Edward/Desktop/HGP_Repositories" +
            "/Ed_Ellis_HW3/src/RoadNetwork.db";
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return conn;
  }

  public void insertNode(int NodeID, float lat, float lon) {
    String sql = "INSERT INTO nodes(NodeID,Latitude,Longitude) VALUES(?,?,?)";

    try (Connection conn = this.connect();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setInt(1, NodeID);
      pstmt.setFloat(2, lat);
      pstmt.setFloat(3, lon);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void insertEdge(int EdgeId, int StartNodeId, int EndNodeId, float L2Distance) {
    String sql = "INSERT INTO edges(EdgeId,StartNodeId,EndNodeId,L2Distance) " +
            "VALUES(?,?,?,?)";

    try (Connection conn = this.connect();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setInt(1, EdgeId);
      pstmt.setInt(2, StartNodeId);
      pstmt.setInt(3, EndNodeId);
      pstmt.setFloat(4, L2Distance);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * select all rows in the warehouses table
   */
  public void selectAllNodes(){
    String sql = "SELECT NodeID, Latitude, Longitude FROM nodes";

    try (Connection conn = this.connect();
         Statement stmt  = conn.createStatement();
         ResultSet rs    = stmt.executeQuery(sql)){

      // loop through the result set
      while (rs.next()) {
        System.out.println(rs.getInt("NodeID") +  "\t" +
                rs.getFloat("Latitude") + "\t" +
                rs.getFloat("Longitude"));
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void selectAllEdges(){
    String sql = "SELECT EdgeId, StartNodeId, EndNodeId, L2Distance FROM edges";

    try (Connection conn = this.connect();
         Statement stmt  = conn.createStatement();
         ResultSet rs    = stmt.executeQuery(sql)){

      // loop through the result set
      while (rs.next()) {
        System.out.println(rs.getInt("EdgeId") +  "\t" +
                rs.getInt("StartNodeId") + "\t" +
                rs.getInt("EndNodeId") + "\t" +
                rs.getFloat("L2Distance"));
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void deleteAllEdges() {
    String sql = "DELETE FROM edges WHERE EdgeId > -1";

    try (Connection conn = this.connect();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      // execute the delete statement
      pstmt.executeUpdate();

    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void deleteAllNodes() {
    String sql = "DELETE FROM nodes WHERE NodeID > -1";

    try (Connection conn = this.connect();
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      // execute the delete statement
      pstmt.executeUpdate();

    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
}
