package com.youtrackdb.ldbc.ytdb.loader;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import static com.youtrackdb.ldbc.ytdb.loader.LdbcSchema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.youtrackdb.ldbc.ytdb.loader.EntityRecords.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;

public class YtdbLoader {

    private static final Logger log = LoggerFactory.getLogger(YtdbLoader.class);

    private static final int BATCH_SIZE = 50000;

    private final YTDBGraphTraversalSource traversal;

    public YtdbLoader(YTDBGraphTraversalSource traversal) {
        this.traversal = traversal;
    }

    public void loadAll(Path datasetRoot) throws Exception {
        log.info("Starting LDBC SNB data load from: {}", datasetRoot);

        Path staticDir = datasetRoot.resolve("static");
        Path dynamicDir = datasetRoot.resolve("dynamic");

        validateDirectories(staticDir, dynamicDir);

        long startTime = System.currentTimeMillis();

        // Phase 1: Load static entities
        log.info("Loading static entities...");
        loadEntities(staticDir, "place_0_0.csv", PLACE, Place::parse, this::insertPlace);
        loadEntities(staticDir, "organisation_0_0.csv", ORGANISATION, Organisation::parse, this::insertOrganisation);
        loadEntities(staticDir, "tagclass_0_0.csv", TAG_CLASS, TagClass::parse, this::insertTagClass);
        loadEntities(staticDir, "tag_0_0.csv", TAG, Tag::parse, this::insertTag);

        // Phase 2: Load static relationships
        log.info("Loading static relationships...");
        loadSimpleEdge(staticDir, "place_isPartOf_place_0_0.csv", IS_PART_OF, PLACE, PLACE);
        loadSimpleEdge(staticDir, "organisation_isLocatedIn_place_0_0.csv", IS_LOCATED_IN, ORGANISATION, PLACE);
        loadSimpleEdge(staticDir, "tagclass_isSubclassOf_tagclass_0_0.csv", IS_SUBCLASS_OF, TAG_CLASS, TAG_CLASS);
        loadSimpleEdge(staticDir, "tag_hasType_tagclass_0_0.csv", HAS_TYPE, TAG, TAG_CLASS);

        // Phase 3: Load dynamic entities
        log.info("Loading dynamic entities...");
        loadEntities(dynamicDir, "person_0_0.csv", PERSON, Person::parse, this::insertPerson);
        loadEntities(dynamicDir, "forum_0_0.csv", FORUM, Forum::parse, this::insertForum);
        loadEntities(dynamicDir, "post_0_0.csv", POST, Post::parse, this::insertPost);
        loadEntities(dynamicDir, "comment_0_0.csv", COMMENT, Comment::parse, this::insertComment);

        // Phase 4: Load dynamic relationships
        log.info("Loading dynamic relationships...");
        loadPersonRelationships(dynamicDir);
        loadForumRelationships(dynamicDir);
        loadContentRelationships(dynamicDir);

        long duration = System.currentTimeMillis() - startTime;
        log.info("Data loading completed in {}ms ({} seconds)", duration, duration / 1000.0);
    }

    public Map<String, Long> counts() {
        var counts = new HashMap<String, Long>();
        try {
            List<String> vertexLabels = List.of(
                    PERSON,
                    PLACE,
                    ORGANISATION,
                    TAG_CLASS,
                    TAG,
                    FORUM,
                    POST,
                    COMMENT
            );

            for (String label : vertexLabels) {
                counts.put(label, traversal.V().hasLabel(label).count().next());
            }

            List<String> edgeLabels = List.of(
                    KNOWS,
                    HAS_CREATOR,
                    IS_LOCATED_IN,
                    HAS_INTEREST,
                    HAS_MEMBER,
                    LIKES,
                    HAS_TAG,
                    REPLY_OF,
                    STUDY_AT,
                    WORK_AT,
                    HAS_MODERATOR,
                    CONTAINER_OF,
                    IS_PART_OF,
                    HAS_TYPE
            );

            for (String edgeLabel : edgeLabels) {
                counts.put(edgeLabel, traversal.E().hasLabel(edgeLabel).count().next());
            }
        } catch (Exception e) {
            log.error("Error computing counts", e);
        }
        return counts;
    }

    // ==================== ENTITY LOADERS ====================

    private <T> void loadEntities(Path dir, String filename, String entityLabel,
                                  java.util.function.Function<String[], T> parser,
                                  java.util.function.Consumer<List<T>> inserter) {
        try {
            Path csvFile = dir.resolve(filename);
            if (!Files.exists(csvFile)) {
                log.warn("File not found: {}", csvFile);
                return;
            }

            var processor = new CsvProcessor<T>(BATCH_SIZE);
            long count = processor.process(csvFile,
                    parser::apply,
                    inserter::accept
            );

            log.info("Loaded {} {} entities", count, entityLabel);
        } catch (Exception e) {
            log.error("Failed to load {}: {}", entityLabel, e.getMessage(), e);
            throw new RuntimeException("Failed to load " + entityLabel, e);
        }
    }

    private void insertPlace(List<Place> batch) {
        traversal.executeInTx(g -> {
            for (Place place : batch) {
                g.addV(PLACE)
                        .property(ID, place.id())
                        .property(NAME, place.name())
                        .property(URL, place.url())
                        .property(TYPE, place.type())
                        .iterate();
            }
        });
    }

    private void insertOrganisation(List<Organisation> batch) {
        traversal.executeInTx(g -> {
            for (Organisation org : batch) {
                g.addV(ORGANISATION)
                        .property(ID, org.id())
                        .property(TYPE, org.type())
                        .property(NAME, org.name())
                        .property(URL, org.url())
                        .iterate();
            }
        });
    }

    private void insertTagClass(List<TagClass> batch) {
        traversal.executeInTx(g -> {
            for (TagClass tagClass : batch) {
                g.addV(TAG_CLASS)
                        .property(ID, tagClass.id())
                        .property(NAME, tagClass.name())
                        .property(URL, tagClass.url())
                        .iterate();
            }
        });
    }

    private void insertTag(List<Tag> batch) {
        traversal.executeInTx(g -> {
            for (Tag tag : batch) {
                g.addV(TAG)
                        .property(ID, tag.id())
                        .property(NAME, tag.name())
                        .property(URL, tag.url())
                        .iterate();
            }
        });
    }

    private void insertPerson(List<Person> batch) {
        traversal.executeInTx(g -> {
            for (Person person : batch) {
                g.addV(PERSON)
                        .property(ID, person.id())
                        .property(FIRST_NAME, person.firstName())
                        .property(LAST_NAME, person.lastName())
                        .property(GENDER, person.gender())
                        .property(BIRTHDAY, person.birthday())
                        .property(CREATION_DATE, person.creationDate())
                        .property(LOCATION_IP, person.locationIP())
                        .property(BROWSER_USED, person.browserUsed())
                        .property(LANGUAGES, person.languages())
                        .property(EMAILS, person.emails())
                        .iterate();
            }
        });
    }

    private void insertForum(List<Forum> batch) {
        traversal.executeInTx(g -> {
            for (Forum forum : batch) {
                g.addV(FORUM)
                        .property(ID, forum.id())
                        .property(TITLE, forum.title())
                        .property(CREATION_DATE, forum.creationDate())
                        .iterate();
            }
        });
    }

    private void insertPost(List<Post> batch) {
        traversal.executeInTx(g -> {
            for (Post post : batch) {
                var traversal = g.addV(POST)
                        .property(ID, post.id())
                        .property(CREATION_DATE, post.creationDate())
                        .property(LOCATION_IP, post.locationIP())
                        .property(BROWSER_USED, post.browserUsed())
                        .property(LANGUAGE, post.language())
                        .property(LENGTH, post.length());

                if (post.imageFile() != null) {
                    traversal.property(IMAGE_FILE, post.imageFile());
                }
                if (post.content() != null) {
                    traversal.property(CONTENT, post.content());
                }

                traversal.iterate();
            }
        });
    }

    private void insertComment(List<Comment> batch) {
        traversal.executeInTx(g -> {
            for (Comment comment : batch) {
                g.addV(COMMENT)
                        .property(ID, comment.id())
                        .property(CREATION_DATE, comment.creationDate())
                        .property(LOCATION_IP, comment.locationIP())
                        .property(BROWSER_USED, comment.browserUsed())
                        .property(CONTENT, comment.content())
                        .property(LENGTH, comment.length())
                        .iterate();
            }
        });
    }

    // ==================== RELATIONSHIP LOADERS ====================

    private void loadPersonRelationships(Path dynamicDir) throws Exception {
        loadKnowsEdge(dynamicDir.resolve("person_knows_person_0_0.csv"));
        loadSimpleEdge(dynamicDir, "person_isLocatedIn_place_0_0.csv", IS_LOCATED_IN, PERSON, PLACE);
        loadSimpleEdge(dynamicDir, "person_hasInterest_tag_0_0.csv", HAS_INTEREST, PERSON, TAG);
        loadStudyAtEdge(dynamicDir.resolve("person_studyAt_organisation_0_0.csv"));
        loadWorkAtEdge(dynamicDir.resolve("person_workAt_organisation_0_0.csv"));
        loadLikesEdge(dynamicDir.resolve("person_likes_post_0_0.csv"), POST);
        loadLikesEdge(dynamicDir.resolve("person_likes_comment_0_0.csv"), COMMENT);
    }

    private void loadForumRelationships(Path dynamicDir) throws Exception {
        loadSimpleEdge(dynamicDir, "forum_hasModerator_person_0_0.csv", HAS_MODERATOR, FORUM, PERSON);
        loadSimpleEdge(dynamicDir, "forum_containerOf_post_0_0.csv", CONTAINER_OF, FORUM, POST);
        loadSimpleEdge(dynamicDir, "forum_hasTag_tag_0_0.csv", HAS_TAG, FORUM, TAG);
        loadHasMemberEdge(dynamicDir.resolve("forum_hasMember_person_0_0.csv"));
    }

    private void loadContentRelationships(Path dynamicDir) throws Exception {
        loadSimpleEdge(dynamicDir, "post_hasCreator_person_0_0.csv", HAS_CREATOR, POST, PERSON);
        loadSimpleEdge(dynamicDir, "post_isLocatedIn_place_0_0.csv", IS_LOCATED_IN, POST, PLACE);
        loadSimpleEdge(dynamicDir, "post_hasTag_tag_0_0.csv", HAS_TAG, POST, TAG);
        loadSimpleEdge(dynamicDir, "comment_hasCreator_person_0_0.csv", HAS_CREATOR, COMMENT, PERSON);
        loadSimpleEdge(dynamicDir, "comment_isLocatedIn_place_0_0.csv", IS_LOCATED_IN, COMMENT, PLACE);
        loadSimpleEdge(dynamicDir, "comment_replyOf_post_0_0.csv", REPLY_OF, COMMENT, POST);
        loadSimpleEdge(dynamicDir, "comment_replyOf_comment_0_0.csv", REPLY_OF, COMMENT, COMMENT);
        loadSimpleEdge(dynamicDir, "comment_hasTag_tag_0_0.csv", HAS_TAG, COMMENT, TAG);
    }

    private void loadSimpleEdge(Path dir, String filename, String edgeLabel,
                                String fromLabel, String toLabel) {
        try {
            Path csvFile = dir.resolve(filename);
            if (!Files.exists(csvFile)) {
                log.warn("File not found: {}", csvFile);
                return;
            }

            var processor = new CsvProcessor<SimpleEdge>(BATCH_SIZE);

            long count = processor.process(csvFile,
                    SimpleEdge::parse,
                    batch -> insertSimpleEdges(batch, edgeLabel, fromLabel, toLabel)
            );

            log.info("Loaded {} {} edges", count, edgeLabel);
        } catch (Exception e) {
            log.error("Failed to load {} edges: {}", edgeLabel, e.getMessage(), e);
            throw new RuntimeException("Failed to load " + edgeLabel + " edges", e);
        }
    }

    private void insertSimpleEdges(List<SimpleEdge> batch, String edgeLabel,
                                   String fromLabel, String toLabel) {
        traversal.executeInTx(g -> {
            for (SimpleEdge edge : batch) {
                g.V().has(fromLabel, ID, edge.fromId())
                        .addE(edgeLabel)
                        .to(V().has(toLabel, ID, edge.toId()))
                        .iterate();
            }
        });
    }

    private void loadKnowsEdge(Path csvFile) throws Exception {
        if (!Files.exists(csvFile)) {
            log.warn("File not found: {}", csvFile);
            return;
        }

        var processor = new CsvProcessor<KnowsEdge>(BATCH_SIZE);
        long count = processor.process(csvFile,
                KnowsEdge::parse,
                this::insertKnowsEdges
        );

        log.info("Loaded {} KNOWS edges", count);
    }

    private void insertKnowsEdges(List<KnowsEdge> batch) {
        traversal.executeInTx(g -> {
            for (KnowsEdge edge : batch) {
                // Bidirectional relationship
                g.V().has(PERSON, ID, edge.person1Id())
                        .addE(KNOWS)
                        .to(V().has(PERSON, ID, edge.person2Id()))
                        .property(CREATION_DATE, edge.creationDate()).iterate();
                g.V().has(PERSON, ID, edge.person2Id())
                        .addE(KNOWS)
                        .to(V().has(PERSON, ID, edge.person1Id()))
                        .property(CREATION_DATE, edge.creationDate()).iterate();
            }
        });
    }

    private void loadStudyAtEdge(Path csvFile) throws Exception {
        if (!Files.exists(csvFile)) {
            log.warn("File not found: {}", csvFile);
            return;
        }

        var processor = new CsvProcessor<StudyAtEdge>(BATCH_SIZE);

        long count = processor.process(csvFile,
                StudyAtEdge::parse,
                this::insertStudyAtEdges
        );

        log.info("Loaded {} STUDY_AT edges", count);
    }

    private void insertStudyAtEdges(List<StudyAtEdge> batch) {
        traversal.executeInTx(g -> {
            for (StudyAtEdge edge : batch) {
                g.V().has(PERSON, ID, edge.personId())
                        .addE(STUDY_AT)
                        .to(V().has(ORGANISATION, ID, edge.organisationId()))
                        .property(CLASS_YEAR, edge.classYear()).iterate();
            }
        });
    }

    private void loadWorkAtEdge(Path csvFile) throws Exception {
        if (!Files.exists(csvFile)) {
            log.warn("File not found: {}", csvFile);
            return;
        }

        var processor = new CsvProcessor<WorkAtEdge>(BATCH_SIZE);

        long count = processor.process(csvFile,
                WorkAtEdge::parse,
                this::insertWorkAtEdges
        );

        log.info("Loaded {} WORK_AT edges", count);
    }

    private void insertWorkAtEdges(List<WorkAtEdge> batch) {
        traversal.executeInTx(g -> {
            for (WorkAtEdge edge : batch) {
                g.V().has(PERSON, ID, edge.personId())
                        .addE(WORK_AT)
                        .to(V().has(ORGANISATION, ID, edge.organisationId()))
                        .property(WORK_FROM, edge.workFrom()).iterate();
            }
        });
    }

    private void loadHasMemberEdge(Path csvFile) throws Exception {
        if (!Files.exists(csvFile)) {
            log.warn("File not found: {}", csvFile);
            return;
        }

        var processor = new CsvProcessor<HasMemberEdge>(BATCH_SIZE);

        long count = processor.process(csvFile,
                HasMemberEdge::parse,
                this::insertHasMemberEdges
        );

        log.info("Loaded {} HAS_MEMBER edges", count);
    }

    private void insertHasMemberEdges(List<HasMemberEdge> batch) {
        traversal.executeInTx(g -> {
            for (HasMemberEdge edge : batch) {
                g.V().has(FORUM, ID, edge.forumId())
                        .addE(HAS_MEMBER)
                        .to(V().has(PERSON, ID, edge.personId()))
                        .property(JOIN_DATE, edge.joinDate()).iterate();
            }
        });
    }

    private void loadLikesEdge(Path csvFile, String contentLabel) {
        try {
            if (!Files.exists(csvFile)) {
                log.warn("File not found: {}", csvFile);
                return;
            }

            var processor = new CsvProcessor<LikesEdge>(BATCH_SIZE);

            long count = processor.process(csvFile,
                    LikesEdge::parse,
                    batch -> insertLikesEdges(batch, contentLabel)
            );

            log.info("Loaded {} LIKES {} edges", count, contentLabel);
        } catch (Exception e) {
            log.error("Failed to load LIKES {} edges: {}", contentLabel, e.getMessage(), e);
            throw new RuntimeException("Failed to load LIKES edges", e);
        }
    }

    private void insertLikesEdges(List<LikesEdge> batch, String contentLabel) {
        traversal.executeInTx(g -> {
            for (LikesEdge edge : batch) {
                g.V().has(PERSON, ID, edge.personId())
                        .addE(LIKES)
                        .to(V().has(contentLabel, ID, edge.contentId()))
                        .property(CREATION_DATE, edge.creationDate()).iterate();
            }
        });
    }

    // ==================== UTILITIES ====================

    private void validateDirectories(Path staticDir, Path dynamicDir) {
        if (!Files.exists(staticDir) || !Files.exists(dynamicDir)) {
            throw new IllegalArgumentException(
                    "Dataset directory must contain static/ and dynamic/ subdirectories");
        }
    }
}
