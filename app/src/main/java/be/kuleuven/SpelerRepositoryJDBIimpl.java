package be.kuleuven;

import java.util.List;
import org.jdbi.v3.core.Jdbi;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private final Jdbi jdbi;

  // Constructor
  SpelerRepositoryJDBIimpl(String connectionString, String user, String pwd) {
    this.jdbi = Jdbi.create(connectionString, user, pwd);
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (:id, :naam, :punten)")
          .bind("id", speler.getTennisvlaanderenid())
          .bind("naam", speler.getNaam())
          .bind("punten", speler.getPunten())
          .execute();
    });
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    return jdbi.withHandle(handle ->
        handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenID = :id")
            .bind("id", tennisvlaanderenId)
            .map((rs, ctx) -> new Speler(
                rs.getInt("tennisvlaanderenID"),
                rs.getString("naam"),
                rs.getInt("punten")
            ))
            .findOne()
            .orElseThrow(() -> new InvalidSpelerException("No speler found with ID: " + tennisvlaanderenId))
    );
  }

  @Override
  public List<Speler> getAllSpelers() {
    return jdbi.withHandle(handle ->
        handle.createQuery("SELECT * FROM speler")
            .map((rs, ctx) -> new Speler(
                rs.getInt("tennisvlaanderenID"),
                rs.getString("naam"),
                rs.getInt("punten")
            ))
            .list()
    );
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenID = :id")
          .bind("id", speler.getTennisvlaanderenid())
          .bind("naam", speler.getNaam())
          .bind("punten", speler.getPunten())
          .execute();
    });
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("DELETE FROM speler WHERE tennisvlaanderenID = :id")
          .bind("id", tennisvlaanderenid)
          .execute();
    });
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    return jdbi.withHandle(handle ->
        handle.createQuery("SELECT MAX(ranking) AS hoogsteRanking FROM speler_ranking WHERE tennisvlaanderenID = :id")
            .bind("id", tennisvlaanderenid)
            .mapTo(String.class)
            .findOne()
            .orElseThrow(() -> new InvalidSpelerException("No ranking found for speler with ID: " + tennisvlaanderenid))
    );
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("INSERT INTO tornooi_speler (tornooiId, tennisvlaanderenId) VALUES (:tornooiId, :spelerId)")
          .bind("tornooiId", tornooiId)
          .bind("spelerId", tennisvlaanderenId)
          .execute();
    });
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("DELETE FROM tornooi_speler WHERE tornooiId = :tornooiId AND tennisvlaanderenId = :spelerId")
          .bind("tornooiId", tornooiId)
          .bind("spelerId", tennisvlaanderenId)
          .execute();
    });
  }
}
