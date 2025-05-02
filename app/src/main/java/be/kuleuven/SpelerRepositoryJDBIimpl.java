package be.kuleuven;


import java.util.List;
import org.jdbi.v3.core.Jdbi;
import java.util.Map;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private final Jdbi jdbi;

  // Constructor
  public SpelerRepositoryJDBIimpl(String connectionString, String user, String pwd) {
    this.jdbi = Jdbi.create(connectionString, user, pwd);
  }


  public Jdbi getJdbi() {
    return jdbi;
  }
  @Override
  public void addSpelerToDb(Speler speler) {
    jdbi.withHandle(handle -> {

      return handle.createUpdate("INSERT INTO speler (tennisvlaanderenid, naam, punten) VALUES (:tennisvlaanderenid, :naam, :punten)")
      .bindBean(speler)
      .execute();
    });
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = (Speler) jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenid = :nummer")
          .bind("nummer", tennisvlaanderenId)
          .mapToBean(Speler.class)
          .findOne()
          .orElseThrow(() -> new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenId + ""));
    });
    return speler;
  }


  @Override
  public List<Speler> getAllSpelers() {
    return jdbi.withHandle(handle -> {
        return handle.createQuery("SELECT * FROM speler")
            .mapToBean(Speler.class)
            .list();

      });
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle.createUpdate("UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenid = :tennisvlaanderenid")
        .bindBean(speler)
        .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException(speler.getTennisvlaanderenid() + "");
    }
  }
    

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenID) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle.createUpdate("DELETE FROM speler WHERE tennisvlaanderenid = :id")
        .bind("id", tennisvlaanderenID)
        .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException(tennisvlaanderenID + "");
    }
  }


  @Override
public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    return jdbi.withHandle(handle -> {
        String query = "SELECT w.finale, w.winnaar, t.clubnaam " +
                       "FROM wedstrijd w JOIN tornooi t ON w.tornooi = t.id " +
                       "WHERE w.speler1 = :tennisvlaanderenid OR w.speler2 = :tennisvlaanderenid";

        List<Map<String, Object>> results = handle.createQuery(query)
            .bind("tennisvlaanderenid", tennisvlaanderenid)
            .mapToMap()
            .list();

        String besteTornooi = null;
        String besteFase = null;
        int hoogsteScore = -1;

        for (Map<String, Object> row : results) {
            int finale = (int) row.get("finale");
            int winnaar = (int) row.get("winnaar");
            String clubnaam = (String) row.get("clubnaam");

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

        return "Hoogst geplaatst in het tornooi van " + besteTornooi + " met plaats in de " + besteFase;
    });
}

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle.createUpdate("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (:speler, :tornooi)")
        .bind("speler", tennisvlaanderenId)
        .bind("tornooi", tornooiId)
        .execute();
    });
    if (affectedRows == 0) {
      throw new RuntimeException();
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle.createUpdate("DELETE FROM speler_speelt_tornooi WHERE speler = :speler AND tornooi = :tornooi")
        .bind("speler", tennisvlaanderenId)
        .bind("tornooi", tornooiId)
        .execute();
    });
    if (affectedRows == 0) {
      throw new RuntimeException();
    }
  }
}

