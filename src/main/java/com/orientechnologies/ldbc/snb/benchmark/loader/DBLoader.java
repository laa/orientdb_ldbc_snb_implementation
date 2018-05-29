package com.orientechnologies.ldbc.snb.benchmark.loader;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentHasCreatorLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentHasTagLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentIsLocatedInLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentReplyOfCommentLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.CommentReplyOfPostLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.ContainerOfLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.ForumHasTagLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.ForumLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.HasInterestLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.HasMemberLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.HasModeratorLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.HasTypeLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.KnowsLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.OrganisationIsLocatedInLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.OrganisationLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonEMailLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonIsLocatedInLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonLikesCommentLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonLikesPostLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PersonSpeaksLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PlaceIsPartOfLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PlaceLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PostHasCreatorLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PostHasTagLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PostIsLocatedInLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.PostLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.StudyAtLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.TagClassLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.TagLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.loaders.WorkAtLoader;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.Knows;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.Likes;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.PlaceIsPartOf;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageIsLocatedIn;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.OrganisationIsLocatedIn;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.ContainerOf;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.ForumHasTag;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasCreator;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasInterest;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasMember;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasModerator;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.HasType;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.MessageHasTag;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.PersonIsLocatedIn;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.ReplyOf;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.StudyAt;
import com.orientechnologies.ldbc.snb.benchmark.loader.schema.WorkAt;
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
  public static final  String PERSON_SPEAKS            = "speaks";
  public static final  String PERSON_BROWSER_USED      = "browserUsed";
  public static final  String PERSON_LOCATION_IP       = "locationIP";
  public static final  String PERSON_CREATION_DATE     = "creationDate";
  public static final  String FORUM_CLASS              = "Forum";
  public static final  String FORUM_ID                 = "id";
  public static final  String FORUM_TITLE              = "title";
  public static final  String FORUM_CREATION_DATE      = "creationDate";
  private static final String MESSAGE_CLASS            = "Message";
  public static final  String MESSAGE_ID               = "id";
  public static final  String MESSAGE_BROWSER_USED     = "browserUsed";
  public static final  String MESSAGE_CREATION_DATE    = "creationDate";
  public static final  String MESSAGE_LOCATION_IP      = "locationIP";
  public static final  String MESSAGE_CONTENT          = "content";
  public static final  String MESSAGE_LENGTH           = "length";
  public static final  String POST_CLASS               = "Post";
  public static final  String POST_LANGUAGE            = "language";
  public static final  String COMMENT_CLASS            = "Comment";
  public static final  String TAG_CLASS_CLASS          = "TagClass";
  public static final  String TAG_CLASS_ID             = "id";
  public static final  String TAG_CLASS_NAME           = "name";
  public static final  String TAG_CLASS_URL            = "url";
  public static final  String TAG_CLASS                = "Tag";
  public static final  String TAG_ID                   = "id";
  public static final  String TAG_NAME                 = "name";
  public static final  String TAG_URL                  = "url";
  public static final  String PLACE_CLASS              = "Place";
  public static final  String PLACE_ID                 = "id";
  public static final  String PLACE_NAME               = "name";
  public static final  String PLACE_URL                = "url";
  public static final  String CITY_CLASS               = "City";
  public static final  String COUNTRY_CLASS            = "Country";
  public static final  String CONTINENT_CLASS          = "Continent";
  public static final  String ORGANISATION_CLASS       = "Organisation";
  public static final  String ORGANISATION_ID          = "id";
  public static final  String ORGANISATION_NAME        = "name";
  public static final  String ORGANISATION_URL         = "url";
  public static final  String UNIVERSITY_CLASS         = "University";
  public static final  String COMPANY_CLASS            = "Company";

  private static final String DEFAULT_DATA_DIR = "./data/SF1/social_network/";

  public static void main(String[] args) throws Exception {
    final long start = System.nanoTime();
    System.out.printf("%tc : Start loading of data from directory %s\n", System.currentTimeMillis(), DEFAULT_DATA_DIR);

    OFileUtils.deleteRecursively(new File(DEFAULT_ENGINE_DIRECTORY));
    final long entriesLoadInterval;
    final long relationsLoadInterval;
    final long schemaGenerationInterval;

    try (OrientDB orientDB = new OrientDB("plocal:" + DEFAULT_ENGINE_DIRECTORY, OrientDBConfig.defaultConfig())) {
      orientDB.create(DEFAULT_DB_NAME, ODatabaseType.PLOCAL);
      System.out.printf("%tc : Database %s was created \n", System.currentTimeMillis(), DEFAULT_DATA_DIR);

      schemaGenerationInterval = generateSchema(orientDB);

      final Path dataDir = Paths.get(DEFAULT_DATA_DIR);
      try (ODatabasePool pool = new ODatabasePool(orientDB, DEFAULT_DB_NAME, "admin", "admin")) {
        final ExecutorService cachedExecutor = Executors.newCachedThreadPool();

        entriesLoadInterval = loadEntries(dataDir, pool, cachedExecutor);

        relationsLoadInterval = loadRelations(dataDir, pool, cachedExecutor);

        cachedExecutor.shutdown();
      }
    }

    final long end = System.nanoTime();
    final int[] passed = DateUtils.convertIntervalInHoursMinSec(end - start);

    System.out
        .printf("%tc : Loading of data is completed in %d h. %d. m. %d s.\n", System.currentTimeMillis(), passed[0], passed[1],
            passed[2]);
    System.out.printf("%tc : Schema is loaded in %d s.\n", System.currentTimeMillis(),
        schemaGenerationInterval / DateUtils.NANOS_IN_SECONDS);
    System.out.printf("%tc : Entries are loaded in  %d s. \n", System.currentTimeMillis(),
        entriesLoadInterval / DateUtils.NANOS_IN_SECONDS);
    System.out.printf("%tc : Relations are loaded in %d s. \n", System.currentTimeMillis(),
        relationsLoadInterval / DateUtils.NANOS_IN_SECONDS);
  }

  private static long loadRelations(Path dataDir, ODatabasePool pool, ExecutorService cachedExecutor)
      throws java.io.IOException, java.util.concurrent.ExecutionException, InterruptedException {
    System.out.printf("%tc: Loading of database relations is started \n", System.currentTimeMillis());

    final long start = System.nanoTime();

    final ContainerOfLoader containerOfLoader = new ContainerOfLoader(dataDir, "forum_containerOf_post_\\d+_\\d+\\.csv");
    containerOfLoader.loadData(pool, cachedExecutor);

    final CommentHasCreatorLoader commentHasCreatorLoader = new CommentHasCreatorLoader(dataDir,
        "comment_hasCreator_person_\\d+_\\d+\\.csv");
    commentHasCreatorLoader.loadData(pool, cachedExecutor);

    final PostHasCreatorLoader postHasCreatorLoader = new PostHasCreatorLoader(dataDir, "post_hasCreator_person_\\d+_\\d+\\.csv");
    postHasCreatorLoader.loadData(pool, cachedExecutor);

    final HasInterestLoader hasInterestLoader = new HasInterestLoader(dataDir, "person_hasInterest_tag_\\d+_\\d+\\.csv");
    hasInterestLoader.loadData(pool, cachedExecutor);

    final HasMemberLoader hasMemberLoader = new HasMemberLoader(dataDir, "forum_hasMember_person_\\d+_\\d+\\.csv");
    hasMemberLoader.loadData(pool, cachedExecutor);

    final OrganisationIsLocatedInLoader organisationIsLocatedInLoader = new OrganisationIsLocatedInLoader(dataDir,
        "organisation_isLocatedIn_place_\\d+_\\d+\\.csv");
    organisationIsLocatedInLoader.loadData(pool, cachedExecutor);

    final HasModeratorLoader hasModeratorLoader = new HasModeratorLoader(dataDir, "forum_hasModerator_person_\\d+_\\d+\\.csv");
    hasModeratorLoader.loadData(pool, cachedExecutor);

    final CommentHasTagLoader commentHasTagLoader = new CommentHasTagLoader(dataDir, "comment_hasTag_tag_\\d+_\\d+\\.csv");
    commentHasTagLoader.loadData(pool, cachedExecutor);

    final PostHasTagLoader postHasTagLoader = new PostHasTagLoader(dataDir, "post_hasTag_tag_\\d+_\\d+\\.csv");
    postHasTagLoader.loadData(pool, cachedExecutor);

    final ForumHasTagLoader forumHasTagLoader = new ForumHasTagLoader(dataDir, "forum_hasTag_tag_\\d+_\\d+\\.csv");
    forumHasTagLoader.loadData(pool, cachedExecutor);

    final HasTypeLoader hasTypeLoader = new HasTypeLoader(dataDir, "tag_hasType_tagclass_\\d+_\\d+\\.csv");
    hasTypeLoader.loadData(pool, cachedExecutor);

    final CommentIsLocatedInLoader commentIsLocatedInLoader = new CommentIsLocatedInLoader(dataDir,
        "comment_isLocatedIn_place_\\d+_\\d+\\.csv");
    commentIsLocatedInLoader.loadData(pool, cachedExecutor);

    final PostIsLocatedInLoader postIsLocatedInLoader = new PostIsLocatedInLoader(dataDir,
        "post_isLocatedIn_place_\\d+_\\d+\\.csv");
    postIsLocatedInLoader.loadData(pool, cachedExecutor);

    final PersonIsLocatedInLoader personIsLocatedInLoader = new PersonIsLocatedInLoader(dataDir,
        "person_isLocatedIn_place_\\d+_\\d+\\.csv");
    personIsLocatedInLoader.loadData(pool, cachedExecutor);

    final PlaceIsPartOfLoader placeIsPartOfLoader = new PlaceIsPartOfLoader(dataDir, "place_isPartOf_place_\\d+_\\d+\\.csv");
    placeIsPartOfLoader.loadData(pool, cachedExecutor);

    final KnowsLoader knowsLoader = new KnowsLoader(dataDir, "person_knows_person_\\d+_\\d+\\.csv");
    knowsLoader.loadData(pool, cachedExecutor);

    final PersonLikesCommentLoader personLikesCommentLoader = new PersonLikesCommentLoader(dataDir,
        "person_likes_comment_\\d+_\\d+\\.csv");
    personLikesCommentLoader.loadData(pool, cachedExecutor);

    final PersonLikesPostLoader personLikesPostLoader = new PersonLikesPostLoader(dataDir, "person_likes_post_\\d+_\\d+\\.csv");
    personLikesPostLoader.loadData(pool, cachedExecutor);

    final CommentReplyOfCommentLoader commentReplyOfCommentLoader = new CommentReplyOfCommentLoader(dataDir,
        "comment_replyOf_comment_\\d+_\\d+\\.csv");
    commentReplyOfCommentLoader.loadData(pool, cachedExecutor);

    final CommentReplyOfPostLoader commentReplyOfPostLoader = new CommentReplyOfPostLoader(dataDir,
        "comment_replyOf_post_\\d+_\\d+\\.csv");
    commentReplyOfPostLoader.loadData(pool, cachedExecutor);

    final StudyAtLoader studyAtLoader = new StudyAtLoader(dataDir, "person_studyAt_organisation_\\d+_\\d+\\.csv");
    studyAtLoader.loadData(pool, cachedExecutor);

    final WorkAtLoader workAtLoader = new WorkAtLoader(dataDir, "person_workAt_organisation_\\d+_\\d+\\.csv");
    workAtLoader.loadData(pool, cachedExecutor);

    final long end = System.nanoTime();

    final long interval = end - start;

    final int[] passed = DateUtils.convertIntervalInHoursMinSec(end - start);

    System.out.printf("%tc: Loading of database relations is completed in %d h. %d. m. %d s. (%d nanoseconds) \n",
        System.currentTimeMillis(), passed[0], passed[1], passed[2], interval);

    return interval;
  }

  private static long loadEntries(Path dataDir, ODatabasePool pool, ExecutorService cachedExecutor)
      throws java.io.IOException, java.util.concurrent.ExecutionException, InterruptedException {
    System.out.printf("%tc: Loading of database entries is started \n", System.currentTimeMillis());

    final long start = System.nanoTime();

    final PersonLoader personLoader = new PersonLoader(dataDir, "person_\\d+_\\d+\\.csv");
    personLoader.loadData(pool, cachedExecutor);

    final PersonEMailLoader personEMailLoader = new PersonEMailLoader(dataDir, "person_email_emailaddress_\\d+_\\d+\\.csv");
    personEMailLoader.loadData(pool, cachedExecutor);

    final PersonSpeaksLoader personSpeaksLoader = new PersonSpeaksLoader(dataDir, "person_speaks_language_\\d+_\\d+\\.csv");
    personSpeaksLoader.loadData(pool, cachedExecutor);

    final ForumLoader forumLoader = new ForumLoader(dataDir, "forum_\\d+_\\d+\\.csv");
    forumLoader.loadData(pool, cachedExecutor);

    final PostLoader postLoader = new PostLoader(dataDir, "post_\\d+_\\d+\\.csv");
    postLoader.loadData(pool, cachedExecutor);

    final CommentLoader commentLoader = new CommentLoader(dataDir, "comment_\\d+_\\d+\\.csv");
    commentLoader.loadData(pool, cachedExecutor);

    final TagClassLoader tagClassLoader = new TagClassLoader(dataDir, "tagclass_\\d+_\\d+\\.csv");
    tagClassLoader.loadData(pool, cachedExecutor);

    final TagLoader tagLoader = new TagLoader(dataDir, "tag_\\d+_\\d+\\.csv");
    tagLoader.loadData(pool, cachedExecutor);

    final PlaceLoader placeLoader = new PlaceLoader(dataDir, "place_\\d+_\\d+\\.csv");
    placeLoader.loadData(pool, cachedExecutor);

    final OrganisationLoader organisationLoader = new OrganisationLoader(dataDir, "organisation_\\d+_\\d+\\.csv");
    organisationLoader.loadData(pool, cachedExecutor);

    final long end = System.nanoTime();

    final long interval = end - start;

    final int[] passed = DateUtils.convertIntervalInHoursMinSec(end - start);

    System.out.printf("%tc: Loading of database entries is completed in %d h. %d. m. %d s. (%d nanoseconds) \n",
        System.currentTimeMillis(), passed[0], passed[1], passed[2], interval);

    return interval;
  }

  private static long generateSchema(OrientDB orientDB) {
    System.out.printf("%tc : Start schema generation\n", System.currentTimeMillis());

    final long start;
    final long end;
    try (ODatabaseSession session = orientDB.open(DEFAULT_DB_NAME, "admin", "admin")) {
      start = System.nanoTime();

      createVertexTypes(session);
      createEdgeTypes(session);

      end = System.nanoTime();
    }

    System.out.printf("%tc : Schema generation is completed\n", System.currentTimeMillis());

    return end - start;
  }

  private static void createEdgeTypes(ODatabaseSession session) {
    session.createEdgeClass(ContainerOf.NAME);
    session.createEdgeClass(HasCreator.NAME);
    session.createEdgeClass(HasInterest.NAME);

    OClass hasMemberClass = session.createEdgeClass(HasMember.NAME);
    hasMemberClass.createProperty(HasMember.JOIN_DATE, HasMember.JOIN_DATE_TYPE);

    session.createEdgeClass(HasModerator.NAME);
    session.createEdgeClass(MessageHasTag.NAME);
    session.createEdgeClass(ForumHasTag.NAME);
    session.createEdgeClass(HasType.NAME);
    session.createEdgeClass(OrganisationIsLocatedIn.NAME);
    session.createEdgeClass(MessageIsLocatedIn.NAME);
    session.createEdgeClass(PersonIsLocatedIn.NAME);
    session.createEdgeClass(PlaceIsPartOf.NAME);

    final OClass knowsClass = session.createEdgeClass(Knows.NAME);
    knowsClass.createProperty(Knows.CREATION_DATE, Knows.CREATION_DATE_TYPE);

    final OClass likes = session.createEdgeClass(Likes.NAME);
    likes.createProperty(Likes.CREATION_DATE, Likes.CREATION_DATE_TYPE);

    session.createEdgeClass(ReplyOf.NAME);

    final OClass studyAtClass = session.createEdgeClass(StudyAt.NAME);
    studyAtClass.createProperty(StudyAt.CLASS_YEAR, StudyAt.CLASS_YEAR_TYPE);

    final OClass workAtClass = session.createEdgeClass(WorkAt.NAME);
    workAtClass.createProperty(WorkAt.WORK_FROM, WorkAt.WORK_FROM_TYPE);
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
