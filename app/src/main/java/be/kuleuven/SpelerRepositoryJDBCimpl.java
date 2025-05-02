package be.kuleuven;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

public class SpelerRepositoryJDBCimpl implements SpelerRepository {
  private Connection connection;

  // Constructor
  SpelerRepositoryJDBCimpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      // // WITHOUT prepared statement
      // Statement s = (Statement) connection.createStatement();
      // int goedBezig = student.isGoedBezig() ? 1 : 0;
      // s.executeUpdate("INSERT INTO student (studnr, naam, voornaam, goedbezig) VALUES (" + student.getStudnr() + ", '" + student.getNaam() + "', '" + student.getVoornaam() + "', " + goedBezig + ");");
      // s.close();

      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (?, ?, ?, ?);");
      prepared.setInt(1, speler.getTennisvlaanderenid()); // First questionmark
      prepared.setString(2, speler.getNaam()); // Second questionmark
      prepared.setInt(3, speler.getPunten()); // Third questionmark // Fourth questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler found_speler = null;
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler WHERE tennisvlaanderenID = '" + tennisvlaanderenId + "'";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int tennisvlaanderenIDfromDB = result.getInt("tennisvlaanderenID");
        String naam = result.getString("naam");
        int punten = result.getInt("punten");

        found_speler = new Speler(tennisvlaanderenIDfromDB, naam, punten);
      }
      if (found_speler == null) {
        throw new InvalidSpelerException(tennisvlaanderenId + "");
      }
      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return found_speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    ArrayList<Speler> resultList = new ArrayList<Speler>();
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler;";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int punten = result.getInt("punten");
        String naam = result.getString("naam");
        int tennisvlaanderenId = result.getInt("tennisvlaanderenId");

        resultList.add(new Speler(tennisvlaanderenId, naam, punten));
      }
      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return resultList;
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    getSpelerByTennisvlaanderenId(speler.getTennisvlaanderenid());
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("UPDATE speler SET naam = ?, punten = ?, tennisvlaanderenid = ?;");
      prepared.setInt(1, speler.getTennisvlaanderenid()); // First questionmark
      prepared.setString(2, speler.getNaam()); // Second questionmark
      prepared.setInt(3, speler.getPunten()); // Third questionmark // Fourth questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    getSpelerByTennisvlaanderenId(tennisvlaanderenid);
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("DELETE FROM speler WHERE tennisvlaanderenID = ?");
      prepared.setInt(1, tennisvlaanderenid); // First questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    String hoogsteRanking = null;
    try {
        // Prepare the SQL query to get the highest ranking
        PreparedStatement prepared = (PreparedStatement) connection
            .prepareStatement("SELECT MAX(ranking) AS hoogsteRanking FROM speler_ranking WHERE tennisvlaanderenID = ?");
        prepared.setInt(1, tennisvlaanderenid);

        // Execute the query
        ResultSet result = prepared.executeQuery();

        // Retrieve the highest ranking from the result set
        if (result.next()) {
            hoogsteRanking = result.getString("hoogsteRanking");
        }

        // Close resources
        result.close();
        prepared.close();
        connection.commit();

        // If no ranking is found, throw an exception
        if (hoogsteRanking == null) {
            throw new InvalidSpelerException("No ranking found for speler with ID: " + tennisvlaanderenid);
        }
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
    return hoogsteRanking;
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    try {
        // Prepare the SQL query to add a speler to a tornooi
        PreparedStatement prepared = (PreparedStatement) connection
            .prepareStatement("INSERT INTO tornooi_speler (tornooiId, tennisvlaanderenId) VALUES (?, ?)");
        prepared.setInt(1, tornooiId); // First question mark
        prepared.setInt(2, tennisvlaanderenId); // Second question mark

        // Execute the update
        prepared.executeUpdate();

        // Close resources
        prepared.close();
        connection.commit();
    } catch (Exception e) {
        throw new RuntimeException("Failed to add speler to tornooi: " + e.getMessage(), e);
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    try {
        // Prepare the SQL query to remove a speler from a tornooi
        PreparedStatement prepared = (PreparedStatement) connection
            .prepareStatement("DELETE FROM tornooi_speler WHERE tornooiId = ? AND tennisvlaanderenId = ?");
        prepared.setInt(1, tornooiId); // First question mark
        prepared.setInt(2, tennisvlaanderenId); // Second question mark

        // Execute the update
        prepared.executeUpdate();

        // Close resources
        prepared.close();
        connection.commit();
    } catch (Exception e) {
        throw new RuntimeException("Failed to remove speler from tornooi: " + e.getMessage(), e);
    }
  }
}
