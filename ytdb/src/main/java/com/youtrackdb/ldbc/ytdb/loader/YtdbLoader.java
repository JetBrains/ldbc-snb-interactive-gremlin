package com.youtrackdb.ldbc.ytdb.loader;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.youtrackdb.ldbc.common.LdbcSchema;
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
        loadEntities(staticDir, "place_0_0.csv", LdbcSchema.PLACE, Place::parse, this::insertPlace);
        loadEntities(staticDir, "organisation_0_0.csv", LdbcSchema.ORGANISATION, Organisation::parse, this::insertOrganisation);
        loadEntities(staticDir, "tagclass_0_0.csv", LdbcSchema.TAG_CLASS, TagClass::parse, this::insertTagClass);
        loadEntities(staticDir, "tag_0_0.csv", LdbcSchema.TAG, Tag::parse, this::insertTag);

        // Phase 2: Load static relationships
        log.info("Loading static relationships...");
        loadSimpleEdge(staticDir, "place_isPartOf_place_0_0.csv", LdbcSchema.IS_PART_OF, LdbcSchema.PLACE, LdbcSchema.PLACE);
        loadSimpleEdge(staticDir, "organisation_isLocatedIn_place_0_0.csv", LdbcSchema.IS_LOCATED_IN, LdbcSchema.ORGANISATION, LdbcSchema.PLACE);
        loadSimpleEdge(staticDir, "tagclass_isSubclassOf_tagclass_0_0.csv", LdbcSchema.IS_SUBCLASS_OF, LdbcSchema.TAG_CLASS, LdbcSchema.TAG_CLASS);
        loadSimpleEdge(staticDir, "tag_hasType_tagclass_0_0.csv", LdbcSchema.HAS_TYPE, LdbcSchema.TAG, LdbcSchema.TAG_CLASS);

        // Phase 3: Load dynamic entities
        log.info("Loading dynamic entities...");
        loadEntities(dynamicDir, "person_0_0.csv", LdbcSchema.PERSON, Person::parse, this::insertPerson);
        loadEntities(dynamicDir, "forum_0_0.csv", LdbcSchema.FORUM, Forum::parse, this::insertForum);
        loadEntities(dynamicDir, "post_0_0.csv", LdbcSchema.POST, Post::parse, this::insertPost);
        loadEntities(dynamicDir, "comment_0_0.csv", LdbcSchema.COMMENT, Comment::parse, this::insertComment);

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
                    LdbcSchema.PERSON,
                    LdbcSchema.PLACE,
                    LdbcSchema.ORGANISATION,
                    LdbcSchema.TAG_CLASS,
                    LdbcSchema.TAG,
                    LdbcSchema.FORUM,
                    LdbcSchema.POST,
                    LdbcSchema.COMMENT
            );

            for (String label : vertexLabels) {
                counts.put(label, traversal.V().hasLabel(label).count().next());
            }

            List<String> edgeLabels = List.of(
                    LdbcSchema.KNOWS,
                    LdbcSchema.HAS_CREATOR,
                    LdbcSchema.IS_LOCATED_IN,
                    LdbcSchema.HAS_INTEREST,
                    LdbcSchema.HAS_MEMBER,
                    LdbcSchema.LIKES,
                    LdbcSchema.HAS_TAG,
                    LdbcSchema.REPLY_OF,
                    LdbcSchema.STUDY_AT,
                    LdbcSchema.WORK_AT,
                    LdbcSchema.HAS_MODERATOR,
                    LdbcSchema.CONTAINER_OF,
                    LdbcSchema.IS_PART_OF,
                    LdbcSchema.HAS_TYPE
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
                g.addV(LdbcSchema.PLACE)
                        .property(LdbcSchema.ID, place.id())
                        .property(LdbcSchema.NAME, place.name())
                        .property(LdbcSchema.URL, place.url())
                        .property(LdbcSchema.TYPE, place.type())
                        .iterate();
            }
        });
    }

    private void insertOrganisation(List<Organisation> batch) {
        traversal.executeInTx(g -> {
            for (Organisation org : batch) {
                g.addV(LdbcSchema.ORGANISATION)
                        .property(LdbcSchema.ID, org.id())
                        .property(LdbcSchema.TYPE, org.type())
                        .property(LdbcSchema.NAME, org.name())
                        .property(LdbcSchema.URL, org.url())
                        .iterate();
            }
        });
    }

    private void insertTagClass(List<TagClass> batch) {
        traversal.executeInTx(g -> {
            for (TagClass tagClass : batch) {
                g.addV(LdbcSchema.TAG_CLASS)
                        .property(LdbcSchema.ID, tagClass.id())
                        .property(LdbcSchema.NAME, tagClass.name())
                        .property(LdbcSchema.URL, tagClass.url())
                        .iterate();
            }
        });
    }

    private void insertTag(List<Tag> batch) {
        traversal.executeInTx(g -> {
            for (Tag tag : batch) {
                g.addV(LdbcSchema.TAG)
                        .property(LdbcSchema.ID, tag.id())
                        .property(LdbcSchema.NAME, tag.name())
                        .property(LdbcSchema.URL, tag.url())
                        .iterate();
            }
        });
    }

    private void insertPerson(List<Person> batch) {
        traversal.executeInTx(g -> {
            for (Person person : batch) {
                g.addV(LdbcSchema.PERSON)
                        .property(LdbcSchema.ID, person.id())
                        .property(LdbcSchema.FIRST_NAME, person.firstName())
                        .property(LdbcSchema.LAST_NAME, person.lastName())
                        .property(LdbcSchema.GENDER, person.gender())
                        .property(LdbcSchema.BIRTHDAY, person.birthday())
                        .property(LdbcSchema.CREATION_DATE, person.creationDate())
                        .property(LdbcSchema.LOCATION_IP, person.locationIP())
                        .property(LdbcSchema.BROWSER_USED, person.browserUsed())
                        .property(LdbcSchema.LANGUAGES, person.languages())
                        .property(LdbcSchema.EMAILS, person.emails())
                        .iterate();
            }
        });
    }

    private void insertForum(List<Forum> batch) {
        traversal.executeInTx(g -> {
            for (Forum forum : batch) {
                g.addV(LdbcSchema.FORUM)
                        .property(LdbcSchema.ID, forum.id())
                        .property(LdbcSchema.TITLE, forum.title())
                        .property(LdbcSchema.CREATION_DATE, forum.creationDate())
                        .iterate();
            }
        });
    }

    private void insertPost(List<Post> batch) {
        traversal.executeInTx(g -> {
            for (Post post : batch) {
                var traversal = g.addV(LdbcSchema.POST)
                        .property(LdbcSchema.ID, post.id())
                        .property(LdbcSchema.CREATION_DATE, post.creationDate())
                        .property(LdbcSchema.LOCATION_IP, post.locationIP())
                        .property(LdbcSchema.BROWSER_USED, post.browserUsed())
                        .property(LdbcSchema.LANGUAGE, post.language())
                        .property(LdbcSchema.LENGTH, post.length());

                if (post.imageFile() != null) {
                    traversal.property(LdbcSchema.IMAGE_FILE, post.imageFile());
                }
                if (post.content() != null) {
                    traversal.property(LdbcSchema.CONTENT, post.content());
                }

                traversal.iterate();
            }
        });
    }

    private void insertComment(List<Comment> batch) {
        traversal.executeInTx(g -> {
            for (Comment comment : batch) {
                g.addV(LdbcSchema.COMMENT)
                        .property(LdbcSchema.ID, comment.id())
                        .property(LdbcSchema.CREATION_DATE, comment.creationDate())
                        .property(LdbcSchema.LOCATION_IP, comment.locationIP())
                        .property(LdbcSchema.BROWSER_USED, comment.browserUsed())
                        .property(LdbcSchema.CONTENT, comment.content())
                        .property(LdbcSchema.LENGTH, comment.length())
                        .iterate();
            }
        });
    }

    // ==================== RELATIONSHIP LOADERS ====================

    private void loadPersonRelationships(Path dynamicDir) throws Exception {
        loadKnowsEdge(dynamicDir.resolve("person_knows_person_0_0.csv"));
        loadSimpleEdge(dynamicDir, "person_isLocatedIn_place_0_0.csv", LdbcSchema.IS_LOCATED_IN, LdbcSchema.PERSON, LdbcSchema.PLACE);
        loadSimpleEdge(dynamicDir, "person_hasInterest_tag_0_0.csv", LdbcSchema.HAS_INTEREST, LdbcSchema.PERSON, LdbcSchema.TAG);
        loadStudyAtEdge(dynamicDir.resolve("person_studyAt_organisation_0_0.csv"));
        loadWorkAtEdge(dynamicDir.resolve("person_workAt_organisation_0_0.csv"));
        loadLikesEdge(dynamicDir.resolve("person_likes_post_0_0.csv"), LdbcSchema.POST);
        loadLikesEdge(dynamicDir.resolve("person_likes_comment_0_0.csv"), LdbcSchema.COMMENT);
    }

    private void loadForumRelationships(Path dynamicDir) throws Exception {
        loadSimpleEdge(dynamicDir, "forum_hasModerator_person_0_0.csv", LdbcSchema.HAS_MODERATOR, LdbcSchema.FORUM, LdbcSchema.PERSON);
        loadSimpleEdge(dynamicDir, "forum_containerOf_post_0_0.csv", LdbcSchema.CONTAINER_OF, LdbcSchema.FORUM, LdbcSchema.POST);
        loadSimpleEdge(dynamicDir, "forum_hasTag_tag_0_0.csv", LdbcSchema.HAS_TAG, LdbcSchema.FORUM, LdbcSchema.TAG);
        loadHasMemberEdge(dynamicDir.resolve("forum_hasMember_person_0_0.csv"));
    }

    private void loadContentRelationships(Path dynamicDir) throws Exception {
        loadSimpleEdge(dynamicDir, "post_hasCreator_person_0_0.csv", LdbcSchema.HAS_CREATOR, LdbcSchema.POST, LdbcSchema.PERSON);
        loadSimpleEdge(dynamicDir, "post_isLocatedIn_place_0_0.csv", LdbcSchema.IS_LOCATED_IN, LdbcSchema.POST, LdbcSchema.PLACE);
        loadSimpleEdge(dynamicDir, "post_hasTag_tag_0_0.csv", LdbcSchema.HAS_TAG, LdbcSchema.POST, LdbcSchema.TAG);
        loadSimpleEdge(dynamicDir, "comment_hasCreator_person_0_0.csv", LdbcSchema.HAS_CREATOR, LdbcSchema.COMMENT, LdbcSchema.PERSON);
        loadSimpleEdge(dynamicDir, "comment_isLocatedIn_place_0_0.csv", LdbcSchema.IS_LOCATED_IN, LdbcSchema.COMMENT, LdbcSchema.PLACE);
        loadSimpleEdge(dynamicDir, "comment_replyOf_post_0_0.csv", LdbcSchema.REPLY_OF, LdbcSchema.COMMENT, LdbcSchema.POST);
        loadSimpleEdge(dynamicDir, "comment_replyOf_comment_0_0.csv", LdbcSchema.REPLY_OF, LdbcSchema.COMMENT, LdbcSchema.COMMENT);
        loadSimpleEdge(dynamicDir, "comment_hasTag_tag_0_0.csv", LdbcSchema.HAS_TAG, LdbcSchema.COMMENT, LdbcSchema.TAG);
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
                g.V().has(fromLabel, LdbcSchema.ID, edge.fromId())
                        .addE(edgeLabel)
                        .to(V().has(toLabel, LdbcSchema.ID, edge.toId()))
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
                g.V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.person1Id())
                        .addE(LdbcSchema.KNOWS)
                        .to(V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.person2Id()))
                        .property(LdbcSchema.CREATION_DATE, edge.creationDate()).iterate();
                g.V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.person2Id())
                        .addE(LdbcSchema.KNOWS)
                        .to(V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.person1Id()))
                        .property(LdbcSchema.CREATION_DATE, edge.creationDate()).iterate();
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
                g.V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.personId())
                        .addE(LdbcSchema.STUDY_AT)
                        .to(V().has(LdbcSchema.ORGANISATION, LdbcSchema.ID, edge.organisationId()))
                        .property(LdbcSchema.CLASS_YEAR, edge.classYear()).iterate();
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
                g.V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.personId())
                        .addE(LdbcSchema.WORK_AT)
                        .to(V().has(LdbcSchema.ORGANISATION, LdbcSchema.ID, edge.organisationId()))
                        .property(LdbcSchema.WORK_FROM, edge.workFrom()).iterate();
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
                g.V().has(LdbcSchema.FORUM, LdbcSchema.ID, edge.forumId())
                        .addE(LdbcSchema.HAS_MEMBER)
                        .to(V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.personId()))
                        .property(LdbcSchema.JOIN_DATE, edge.joinDate()).iterate();
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
                g.V().has(LdbcSchema.PERSON, LdbcSchema.ID, edge.personId())
                        .addE(LdbcSchema.LIKES)
                        .to(V().has(contentLabel, LdbcSchema.ID, edge.contentId()))
                        .property(LdbcSchema.CREATION_DATE, edge.creationDate()).iterate();
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
