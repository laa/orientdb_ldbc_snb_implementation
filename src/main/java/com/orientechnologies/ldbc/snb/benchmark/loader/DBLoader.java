package com.orientechnologies.ldbc.snb.benchmark.loader;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonEMailLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonSpeaksLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.utils.DateUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DBLoader {
  public static final  String POST_IMAGE_FILE          = "imageFile";
  private static final String DEFAULT_ENGINE_DIRECTORY = "./build/databases";
  private static final String DEFAULT_DB_NAME          = "snb";
  public static final  String PERSON_CLASS             = "Person";
  public static final  String PERSON_ID                = "id";
  public static final  String PERSON_FIRST_NAME        = "firstName";
  public static final  String PERSON_LAST_NAME         = "lastName";
  public static final  String PERSON_GENDER            = "gender";
  public static final  String PERSON_BIRTH_DATE        = "birthDate";
  public static final  String PERSON_EMAIL             = "email";
  public static final  String PERSON_SPEAKS         = "speaks";
  public static final  String PERSON_BROWSER_USED   = "browserUsed";
  public static final  String PERSON_LOCATION_IP    = "locationIP";
  public static final  String PERSON_CREATION_DATE  = "creationDate";
  public static final  String FORUM_CLASS           = "Forum";
  public static final  String FORUM_ID              = "id";
  public static final  String FORUM_TITLE           = "title";
  public static final  String FORUM_CREATION_DATE   = "creationDate";
  private static final String MESSAGE_CLASS         = "Message";
  public static final  String MESSAGE_ID            = "id";
  public static final  String MESSAGE_BROWSER_USED  = "browserUsed";
  public static final  String MESSAGE_CREATION_DATE = "creationDate";
  public static final  String MESSAGE_LOCATION_IP   = "locationIP";
  public static final  String MESSAGE_CONTENT       = "content";
  public static final  String MESSAGE_LENGTH        = "length";
  public static final  String POST_CLASS            = "Post";
  public static final  String POST_LANGUAGE            = "language";
  public static final  String COMMENT_CLASS            = "Comment";
  public static final  String TAG_CLASS_CLASS    = "TagClass";
  public static final  String TAG_CLASS_ID       = "id";
  public static final  String TAG_CLASS_NAME     = "name";
  public static final  String TAG_CLASS_URL      = "url";
  public static final  String TAG_CLASS          = "Tag";
  public static final  String TAG_ID             = "id";
  public static final  String TAG_NAME           = "name";
  public static final  String TAG_URL            = "url";
  private static final String PLACE_CLASS        = "Place";
  public static final  String PLACE_ID           = "id";
  public static final  String PLACE_NAME         = "name";
  public static final  String PLACE_URL          = "url";
  public static final  String CITY_CLASS         = "City";
  public static final  String COUNTRY_CLASS      = "Country";
  public static final  String CONTINENT_CLASS    = "Continent";
  private static final String ORGANISATION_CLASS = "Organisation";
  public static final  String ORGANISATION_ID    = "id";
  public static final  String ORGANISATION_NAME  = "name";
  public static final  String ORGANISATION_URL   = "url";
  public static final  String UNIVERSITY_CLASS   = "University";
  public static final  String COMPANY_CLASS      = "Company";

  private static final String DEFAULT_DATA_DIR = "./data/SF1/social_network/";

  public static void main(String[] args) throws Exception {
    final long start = System.nanoTime();
    System.out.printf("%tc : Start loading of data from directory %s\n", System.currentTimeMillis(), DEFAULT_DATA_DIR);

    OFileUtils.deleteRecursively(new File(DEFAULT_ENGINE_DIRECTORY));

    try (OrientDB orientDB = new OrientDB("plocal:" + DEFAULT_ENGINE_DIRECTORY, OrientDBConfig.defaultConfig())) {
      orientDB.create(DEFAULT_DB_NAME, ODatabaseType.PLOCAL);
      System.out.printf("%tc : Database %s was created \n", System.currentTimeMillis(), DEFAULT_DATA_DIR);

      generateSchema(orientDB);

      final Path dataDir = Paths.get(DEFAULT_DATA_DIR);
      try (ODatabasePool pool = new ODatabasePool(orientDB, DEFAULT_DB_NAME, "admin", "admin")) {
        final ExecutorService cachedExecutor = Executors.newCachedThreadPool();

        final PersonLoader personLoader = new PersonLoader(dataDir, "person_\\d+_\\d+\\.csv");
        personLoader.loadData(pool, cachedExecutor);

        final PersonEMailLoader personEMailLoader = new PersonEMailLoader(dataDir, "person_email_emailaddress_\\d+_\\d+\\.csv");
        personEMailLoader.loadData(pool, cachedExecutor);

        final PersonSpeaksLoader personSpeaksLoader = new PersonSpeaksLoader(dataDir, "person_speaks_language_\\d+_\\d+\\.csv");
        personSpeaksLoader.loadData(pool, cachedExecutor);

        cachedExecutor.shutdown();
      }
    }

    final long end = System.nanoTime();
    final int[] passed = DateUtils.convertIntervalInHoursMinSec(end - start);

    System.out
        .printf("%tc : Loading of data is completed in %d h. %d. m. %d s.\n", System.currentTimeMillis(), passed[0], passed[1],
            passed[2]);
  }

  private static void generateSchema(OrientDB orientDB) {
    System.out.printf("%tc : Start schema generation\n", System.currentTimeMillis());

    try (ODatabaseSession session = orientDB.open(DEFAULT_DB_NAME, "admin", "admin")) {
      createVertexTypes(session);
      createEdgeTypes(session);
    }

    System.out.printf("%tc : Schema generation is completed\n", System.currentTimeMillis());
  }

  private static void createEdgeTypes(ODatabaseSession session) {
    session.createEdgeClass("containerOf");
    session.createEdgeClass("hasCreator");
    session.createEdgeClass("hasInterest");

    OClass hasMemberClass = session.createEdgeClass("hasMember");
    hasMemberClass.createProperty("joinDate", OType.DATETIME);

    session.createEdgeClass("hasModerator");
    session.createEdgeClass("messageHasTag");
    session.createEdgeClass("forumHasTag");
    session.createEdgeClass("hasType");
    session.createEdgeClass("companyIsLocatedId");
    session.createEdgeClass("messageIsLocatedId");
    session.createEdgeClass("personIsLocatedId");
    session.createEdgeClass("universityIsLocatedId");
    session.createEdgeClass("cityIsPartOf");
    session.createEdgeClass("countryIsPartOf");

    final OClass knowsClass = session.createEdgeClass("knows");
    knowsClass.createProperty("creationDate", OType.DATETIME);

    final OClass likes = session.createEdgeClass("likes");
    likes.createProperty("creationDate", OType.DATETIME);

    session.createEdgeClass("replyOf");

    final OClass studyAtClass = session.createEdgeClass("studyAt");
    studyAtClass.createProperty("classYear", OType.INTEGER);

    final OClass workAtClass = session.createEdgeClass("workAt");
    workAtClass.createProperty("workFrom", OType.INTEGER);
  }

  private static void createVertexTypes(ODatabaseSession session) {
    final OClass personClass = session.createVertexClass(PERSON_CLASS);

    personClass.createProperty(PERSON_ID, OType.LONG);
    personClass.createProperty(PERSON_FIRST_NAME, OType.STRING);
    personClass.createProperty(PERSON_LAST_NAME, OType.STRING);
    personClass.createProperty(PERSON_GENDER, OType.STRING);
    personClass.createProperty(PERSON_BIRTH_DATE, OType.DATE);
    personClass.createProperty(PERSON_EMAIL, OType.EMBEDDEDLIST);
    personClass.createProperty(PERSON_SPEAKS, OType.EMBEDDEDLIST);
    personClass.createProperty(PERSON_BROWSER_USED, OType.STRING);
    personClass.createProperty(PERSON_LOCATION_IP, OType.STRING);
    personClass.createProperty(PERSON_CREATION_DATE, OType.DATETIME);

    personClass.createIndex("person_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PERSON_ID);

    final OClass forumClass = session.createVertexClass(FORUM_CLASS);

    forumClass.createProperty(FORUM_ID, OType.LONG);
    forumClass.createProperty(FORUM_TITLE, OType.STRING);
    forumClass.createProperty(FORUM_CREATION_DATE, OType.DATETIME);

    forumClass.createIndex("forum_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, FORUM_ID);

    final OClass messageClass = session.createVertexClass(MESSAGE_CLASS);

    messageClass.createProperty(MESSAGE_ID, OType.LONG);
    messageClass.createProperty(MESSAGE_BROWSER_USED, OType.STRING);
    messageClass.createProperty(MESSAGE_CREATION_DATE, OType.DATETIME);
    messageClass.createProperty(MESSAGE_LOCATION_IP, OType.STRING);
    messageClass.createProperty(MESSAGE_CONTENT, OType.STRING);
    messageClass.createProperty(MESSAGE_LENGTH, OType.INTEGER);

    messageClass.createIndex("message_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, MESSAGE_ID);

    final OClass postClass = session.createClass(POST_CLASS, MESSAGE_CLASS);
    postClass.createProperty(POST_LANGUAGE, OType.STRING);
    postClass.createProperty(POST_IMAGE_FILE, OType.STRING);

    session.createClass(COMMENT_CLASS, MESSAGE_CLASS);

    final OClass tagClassClass = session.createVertexClass(TAG_CLASS_CLASS);

    tagClassClass.createProperty(TAG_CLASS_ID, OType.LONG);
    tagClassClass.createProperty(TAG_CLASS_NAME, OType.STRING);
    tagClassClass.createProperty(TAG_CLASS_URL, OType.STRING);

    tagClassClass.createIndex("tagClass_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, TAG_CLASS_ID);

    final OClass tagClass = session.createVertexClass(TAG_CLASS);

    tagClass.createProperty(TAG_ID, OType.LONG);
    tagClass.createProperty(TAG_NAME, OType.STRING);
    tagClass.createProperty(TAG_URL, OType.STRING);

    tagClass.createIndex("tag_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, TAG_ID);

    final OClass placeClass = session.createVertexClass(PLACE_CLASS);

    placeClass.createProperty(PLACE_ID, OType.LONG);
    placeClass.createProperty(PLACE_NAME, OType.STRING);
    placeClass.createProperty(PLACE_URL, OType.STRING);

    placeClass.createIndex("place_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PLACE_ID);

    session.createClass(CITY_CLASS, PLACE_CLASS);

    session.createClass(COUNTRY_CLASS, PLACE_CLASS);

    session.createClass(CONTINENT_CLASS, PLACE_CLASS);

    final OClass organisationClass = session.createVertexClass(ORGANISATION_CLASS);

    organisationClass.createProperty(ORGANISATION_ID, OType.LONG);
    organisationClass.createProperty(ORGANISATION_NAME, OType.STRING);
    organisationClass.createProperty(ORGANISATION_URL, OType.STRING);

    organisationClass.createIndex("organisation_id", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, ORGANISATION_ID);

    session.createClass(UNIVERSITY_CLASS, ORGANISATION_CLASS);

    session.createClass(COMPANY_CLASS, ORGANISATION_CLASS);
  }
}
