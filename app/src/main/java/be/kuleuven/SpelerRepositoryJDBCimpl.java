package be.kuleuven;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class SpelerRepositoryJDBCimpl implements SpelerRepository {
  private Connection connection;

  // Constructor
  SpelerRepositoryJDBCimpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (?, ?, ?);");
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
          .prepareStatement("UPDATE speler SET naam = ?, punten = ? WHERE tennisvlaanderenid = ?;");
      prepared.setInt(3, speler.getTennisvlaanderenid()); 
      prepared.setString(1, speler.getNaam()); 
      prepared.setInt(2, speler.getPunten()); 
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
    String besteTornooi = null; // Declare outside the try block
    String besteFase = null;    // Declare outside the try block

    try {
        String query = "SELECT w.finale, w.winnaar, t.clubnaam " +
                       "FROM wedstrijd w JOIN tornooi t ON w.tornooi = t.id " +
                       "WHERE w.speler1 = ? OR w.speler2 = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, tennisvlaanderenid);
        stmt.setInt(2, tennisvlaanderenid);

        ResultSet rs = stmt.executeQuery();

        int hoogsteScore = -1;

        while (rs.next()) {
            int finale = rs.getInt("finale");
            int winnaar = rs.getInt("winnaar");
            String clubnaam = rs.getString("clubnaam");

            int score = switch (finale) {
                case 1 -> (winnaar == tennisvlaanderenid) ? 3 : 2;
                case 2 -> 1;
                default -> 0;
            };

            if (score > hoogsteScore) {
                hoogsteScore = score;
                besteTornooi = clubnaam;
                besteFase = switch (score) {
                    case 3 -> "winst";
                    case 2 -> "finale";
                    case 1 -> "halve finale";
                    default -> null;
                };
            }
        }

        if (besteTornooi == null || besteFase == null) {
            throw new InvalidSpelerException(String.valueOf(tennisvlaanderenid));
        }

        rs.close();
        stmt.close();

    } catch (SQLException e) {
        throw new InvalidSpelerException(String.valueOf(tennisvlaanderenid));
    }

    String resultaat = "Hoogst geplaatst in het tornooi van " + besteTornooi + " met plaats in de " + besteFase;
    return resultaat;
}

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    //DONE: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeert zodat de testen slagen.

    try {
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (?, ?)");
        stmt.setInt(1, tennisvlaanderenId);
        stmt.setInt(2, tornooiId);
        stmt.executeUpdate();
        stmt.close();
        connection.commit();
      } catch (Exception e) {
        throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));    
     }
}

@Override
public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    //DONE: verwijder de "throw new UnsupportedOperationException" en schrijf de code die de gewenste methode op de juiste manier implementeert zodat de testen slagen.

    try {
        PreparedStatement stmt = connection.prepareStatement(
            "DELETE FROM speler_speelt_tornooi WHERE speler = ? AND tornooi = ?");
        stmt.setInt(1, tennisvlaanderenId);
        stmt.setInt(2, tornooiId);
        stmt.executeUpdate();
        stmt.close();
        connection.commit();
      } catch (Exception e) {
        throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));    
    }
  }
}
