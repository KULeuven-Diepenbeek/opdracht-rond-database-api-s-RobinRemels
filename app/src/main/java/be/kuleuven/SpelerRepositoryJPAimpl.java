package be.kuleuven;

import java.util.Comparator;
import java.util.List;


import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public class SpelerRepositoryJPAimpl implements SpelerRepository {
  private final EntityManager em;
  public static final String PERSISTANCE_UNIT_NAME = "be.kuleuven.spelerhibernateTest";

  // Constructor
  SpelerRepositoryJPAimpl(EntityManager entityManager) {
    this.em = entityManager;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    em.getTransaction().begin();
    if (em.find(Speler.class, speler.getTennisvlaanderenid()) != null) {
      em.getTransaction().rollback();
      throw new RuntimeException(" A PRIMARY KEY constraint failed");
    }
    em.persist(speler);
    em.getTransaction().commit();
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = em.find(Speler.class, tennisvlaanderenId);
    if (speler == null) {
      throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
    }
    return speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    CriteriaBuilder cb = em.getCriteriaBuilder();
    CriteriaQuery<Speler> cq = cb.createQuery(Speler.class);
    Root<Speler> root = cq.from(Speler.class);
    cq.select(root);
    return em.createQuery(cq).getResultList();
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    em.getTransaction().begin();
    if (em.find(Speler.class, speler.getTennisvlaanderenid()) == null) {
      throw new InvalidSpelerException(String.valueOf(speler.getTennisvlaanderenid()));
    }
    em.merge(speler);
    em.getTransaction().commit();
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenId) {
    em.getTransaction().begin();
    Speler speler = em.find(Speler.class, tennisvlaanderenId);
    if (speler == null) {
      throw new InvalidSpelerException(String.valueOf(tennisvlaanderenId));
    }
    em.remove(speler);
    em.getTransaction().commit();
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenId) {
    Speler speler = getSpelerByTennisvlaanderenId(tennisvlaanderenId);
    String finaleString = null;
    String clubnaam = null;
    List<Wedstrijd> wedstrijden = speler.getWedstrijden();
    Wedstrijd wedstrijd = (Wedstrijd) wedstrijden.stream()
        .filter(w -> w.getWinnaarId() == speler.getTennisvlaanderenid() && w.getFinale() == 1)
        .findAny()
        .orElse(wedstrijden.stream().min(Comparator.comparingInt(Wedstrijd::getFinale)).orElse(null));
    
    if (wedstrijd != null) {
      switch (wedstrijd.getFinale()) {
        case 1:
          if (wedstrijd.getWinnaarId() == speler.getTennisvlaanderenid()) {
            finaleString = "winst";
          } else {
            finaleString = "finale";
          }
          break;
          
        case 2:
          finaleString = "halve finale";
          break;
        case 4:
          finaleString = "kwart finale";
          break;
        default:
          finaleString ="groepsfase";
          break;
      }
      int tornooiId = wedstrijd.getTornooiId();
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);
      if (tornooi != null) {
        clubnaam = tornooi.getClubnaam();
      } else {
        throw new InvalidTornooiException("Tornooi not found for ID: " + tornooiId);
      }
    }
    return "Hoogst geplaatst in het tornooi van " + clubnaam + " met plaats in de " + finaleString;
  }
  

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();
    try {
      tx.begin();

      Speler speler = em.find(Speler.class, tennisvlaanderenId);
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);

      speler.getTornooien().add(tornooi);
      em.merge(speler);

      tx.commit();
    } catch (Exception e) {
      if (tx.isActive())
        tx.rollback();
      throw e;
    } finally {
      em.close();
    }
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    EntityTransaction tx = em.getTransaction();

    try {
      tx.begin();

      Speler speler = em.find(Speler.class, tennisvlaanderenId);
      Tornooi tornooi = em.find(Tornooi.class, tornooiId);

      if (speler == null || tornooi == null) {
        throw new IllegalArgumentException("Speler or Tornooi not found");
      }

      speler.getTornooien().remove(tornooi);
      em.merge(speler);

      tx.commit();
    } catch (Exception e) {
      if (tx.isActive())
        tx.rollback();
      throw e;
    } finally {
      em.close();
    }
  }
}